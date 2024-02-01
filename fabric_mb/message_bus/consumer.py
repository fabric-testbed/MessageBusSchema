#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)

"""
Defines AvroConsumer API class which exposes interface for various consumer functions
"""
import importlib
import logging
import re
import traceback
from typing import List

from confluent_kafka.avro import AvroConsumer, SerializerError
from confluent_kafka.cimpl import KafkaError, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField

from fabric_mb.message_bus.abc_mb_api import ABCMbApi
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
import time
from confluent_kafka import TopicPartition


class AvroConsumerApi(ABCMbApi):
    """
    This class implements the Interface for Kafka consumer carrying Avro messages.
    It is expected that the users would extend this class and override handle_message function.
    """
    def __init__(self, *, consumer_conf: dict, schema_registry_conf: dict, value_schema_location: str,
                 topics: List[str], batch_size: int = 5, logger: logging.Logger = None, sync: bool = False):
        super(AvroConsumerApi, self).__init__(logger=logger)

        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        self.value_schema = self.load_schema(schema_file=value_schema_location)

        self.string_deserializer = StringDeserializer('utf_8')

        self.value_deserializer = AvroDeserializer(schema_str=self.value_schema,
                                                   schema_registry_client=self.schema_registry_client,
                                                   from_dict=self.from_dict)

        self.consumer = Consumer(consumer_conf)
        self.running = True
        self.topics = topics
        self.batch_size = batch_size
        self.sync = sync

    def shutdown(self):
        """
        Shutdown the consumer
        :return:
        """
        self.logger.debug("Trigger shutdown")
        self.running = False

    @staticmethod
    def _create_instance(*, module_name: str, class_name: str):
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        return class_()

    @staticmethod
    def _create_instance_with_params(*, module_name: str, class_name: str):
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        return class_

    def handle_message(self, message: AbcMessageAvro):
        """
        Handle incoming message; must be overridden by the derived class
        :param message: incoming message
        """
        print(message)

    def consume(self, timeout: float = 0.25):
        """
            Consume records unless shutdown triggered. Using synchronous commit after a message batch.
        """
        self.consumer.subscribe(self.topics)

        msg_count = 0
        while self.running:
            try:
                msg = self.consumer.poll(timeout)

                # There were no messages on the queue, continue polling
                if msg is None:
                    continue

                if msg.error():
                    self.logger.error(f"KAFKA: Consumer error: {msg.error()}")
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        self.logger.error(f"KAFKA: {msg.topic()} {msg.partition} reached end at offset [{msg.offset()}]")
                    elif msg.error():
                        self.logger.error(f"KAFKA: Consumer error: {msg.error()}")
                        continue

                deserialized_message = self.value_deserializer(msg.value(),
                                                              SerializationContext(msg.topic(), MessageField.VALUE))
                # Retrieve current offset and high-water mark
                partition = msg.partition()
                topic = msg.topic()
                current_offset = msg.offset()
                low_mark, highwater_mark = self.consumer.get_watermark_offsets(TopicPartition(topic=topic,
                                                                                              partition=partition))

                # Calculate lag
                lag = highwater_mark - current_offset

                self.logger.info(
                    f"Partition {partition}: Current Offset={current_offset}, Highwater Mark={highwater_mark}, "
                    f"Lag={lag}, Current Message: {deserialized_message.get_message_name()}")
                begin = time.time()
                self.handle_message(message=deserialized_message)
                self.logger.info(f"KAFKA PROCESS TIME: {time.time() - begin:.0f}")

                if self.sync:
                    msg_count += 1
                    if msg_count % self.batch_size == 0:
                        self.consumer.commit(asynchronous=False)

            except SerializerError as e:
                # Report malformed record, discard results, continue polling
                self.logger.error(f"KAFKA: Message deserialization failed {e}")
                self.logger.error(traceback.format_exc())
                continue
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"KAFKA: consumer error: {e}")
                self.logger.error(traceback.format_exc())
                continue

        self.logger.debug("KAFKA: Shutting down consumer..")
        self.consumer.close()

    @staticmethod
    def from_dict(value: dict, ctx: SerializationContext) -> AbcMessageAvro:
        logging.getLogger().debug(f"KAFKA: Message received for topic {ctx.topic}")
        logging.getLogger().debug(f"KAFKA: Value = {value}")
        class_name = value.get('name', None) + 'Avro'
        logging.getLogger().debug("KAFKA: class_name = {}".format(class_name))
        module_name = f"fabric_mb.message_bus.messages.{re.sub(r'(?<!^)(?=[A-Z])', '_', class_name).lower()}"
        logging.getLogger().debug(f"KAFKA: module_name = {module_name}")

        message = AvroConsumerApi._create_instance(module_name=module_name, class_name=class_name)
        message.from_dict(value)
        return message
