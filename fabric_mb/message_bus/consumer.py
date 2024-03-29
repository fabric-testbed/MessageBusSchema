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
import time
import traceback
from typing import List

from confluent_kafka.avro import AvroConsumer, SerializerError
from confluent_kafka.cimpl import KafkaError, TopicPartition

from fabric_mb.message_bus.abc_mb_api import ABCMbApi
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro


class AvroConsumerApi(ABCMbApi):
    """
    This class implements the Interface for Kafka consumer carrying Avro messages.
    It is expected that the users would extend this class and override handle_message function.
    """
    def __init__(self, *, consumer_conf: dict, key_schema_location, value_schema_location: str,
                 topics: List[str], batch_size: int = 5, logger: logging.Logger = None,
                 poll_timeout: int = 250):
        super(AvroConsumerApi, self).__init__(logger=logger)

        self.key_schema = self.load_schema(schema_file=key_schema_location)
        self.value_schema = self.load_schema(schema_file=value_schema_location)

        consumer_conf['on_commit'] = self.commit_completed
        self.consumer = AvroConsumer(consumer_conf, reader_key_schema=self.key_schema,
                                     reader_value_schema=self.value_schema)
        self.running = True
        self.topics = topics
        self.batch_size = batch_size
        self.poll_timeout = float(poll_timeout/1000)
        self.enable_auto_commit = consumer_conf.get("enable.auto.commit", True)

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

    def process_message(self, topic: str, key: dict, value: dict):
        """
        Process the incoming message. Must be overridden in the derived class
        :param topic: topic name
        :param key: incoming message key
        :param value: incoming message value
        :return:
        """
        self.logger.debug("KAFKA: Message received for topic " + topic)
        self.logger.debug("KAFKA: Key = {}".format(key))
        self.logger.debug("KAFKA: Value = {}".format(value))
        class_name = value.get('name', None) + 'Avro'
        self.logger.debug("KAFKA: class_name = {}".format(class_name))
        module_name = f"fabric_mb.message_bus.messages.{re.sub(r'(?<!^)(?=[A-Z])', '_', class_name).lower()}"
        self.logger.debug(f"KAFKA: module_name = {module_name}")

        message = self._create_instance(module_name=module_name, class_name=class_name)
        message.from_dict(value)

        self.handle_message(message=message)

    def handle_message(self, message: AbcMessageAvro):
        """
        Handle incoming message; must be overridden by the derived class
        :param message: incoming message
        """
        print(message)

    def consume(self):
        """
            Consume records unless shutdown triggered. Using synchronous commit after a message batch.
        """
        self.consumer.subscribe(self.topics)

        msg_count = 0
        offsets = []
        while self.running:
            try:
                msg = self.consumer.poll(timeout=self.poll_timeout)

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

                # Retrieve current offset and high-water mark
                partition = msg.partition()
                topic = msg.topic()
                current_offset = msg.offset()
                offsets.append(TopicPartition(topic=topic, partition=partition, offset=current_offset + 1))
                low_mark, highwater_mark = self.consumer.get_watermark_offsets(TopicPartition(topic=topic,
                                                                                              partition=partition))

                # Calculate lag
                lag = highwater_mark - current_offset

                self.logger.info(
                    f"Partition {partition}: Current Offset={current_offset}, Highwater Mark={highwater_mark}, "
                    f"Lag={lag}, Current Message: {msg.value().get('name')}")
                begin = time.time()
                self.process_message(msg.topic(), msg.key(), msg.value())
                self.logger.info(f"KAFKA PROCESS TIME: {time.time() - begin:.0f}")

                if not self.enable_auto_commit:
                    msg_count += 1
                    if msg_count % self.batch_size == 0:
                        self.consumer.commit(offsets=offsets)
                        offsets.clear()

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

        self.logger.info("KAFKA: Shutting down consumer..")
        # Commit all the messages processed if any pending
        if len(offsets):
            self.consumer.commit(offsets=offsets, asynchronous=False)
        self.consumer.close()
        self.logger.info("KAFKA: Consumer Shutting down complete..")

    def commit_completed(self, err, partitions):
        if err:
            self.logger.error(f"KAFKA: commit failure: {err}")
        else:
            self.logger.debug(f"KAFKA: Committed partition offsets: {partitions}")