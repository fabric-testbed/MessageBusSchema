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
import re

from confluent_kafka.avro import AvroConsumer, SerializerError
from confluent_kafka.cimpl import KafkaError

from fabric_mb.message_bus.base import Base
from fabric_mb.message_bus.messages.message import IMessageAvro


class AvroConsumerApi(Base):
    """
    This class implements the Interface for Kafka consumer carrying Avro messages.
    It is expected that the users would extend this class and override handle_message function.
    """
    def __init__(self, conf, key_schema, record_schema, topics, batch_size=5, logger=None):
        super().__init__(logger)
        self.consumer = AvroConsumer(conf, reader_key_schema=key_schema, reader_value_schema=record_schema)
        self.running = True
        self.topics = topics
        self.batch_size = batch_size

    def shutdown(self):
        """
        Shutdown the consumer
        :return:
        """
        self.log_debug("Trigger shutdown")
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
        self.log_debug("Message received for topic " + topic)
        self.log_debug("Key = {}".format(key))
        self.log_debug("Value = {}".format(value))
        class_name = value.get('name', None) + 'Avro'
        self.log_debug("class_name = {}".format(class_name))
        module_name = 'fabric_mb.message_bus.messages.' + re.sub(r'(?<!^)(?=[A-Z])', '_', class_name).lower()
        self.log_debug("module_name = {}".format(module_name))

        message = self._create_instance(module_name=module_name, class_name=class_name)
        message.from_dict(value)

        self.handle_message(message=message)

    def handle_message(self, message: IMessageAvro):
        """
        Handle incoming message; must be overridden by the derived class
        :param message: incoming message
        """
        print(message)

    def consume_auto(self):
        """
            Consume records unless shutdown triggered. Uses Kafka's auto commit.
        """
        self.consumer.subscribe(self.topics)

        while self.running:
            try:
                msg = self.consumer.poll(1)

                # There were no messages on the queue, continue polling
                if msg is None:
                    continue

                if msg.error():
                    self.log_error("Consumer error: {}".format(msg.error()))
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        self.log_error('%% %s [%d] reached end at offset %d\n' % (msg.topic(),
                                                                                  msg.partition(), msg.offset()))
                    elif msg.error():
                        self.log_error("Consumer error: {}".format(msg.error()))
                        continue

                self.process_message(msg.topic(), msg.key(), msg.value())
            except SerializerError as e:
                # Report malformed record, discard results, continue polling
                self.log_error("Message deserialization failed {}".format(e))
                continue
            except KeyboardInterrupt:
                break

        self.log_debug("Shutting down consumer..")
        self.consumer.close()

    def consume_sync(self):
        """
            Consume records unless shutdown triggered. Using synchronous commit after a message batch.
        """
        self.consumer.subscribe(self.topics)

        msg_count = 0
        while self.running:
            try:
                msg = self.consumer.poll(1)

                # There were no messages on the queue, continue polling
                if msg is None:
                    continue

                if msg.error():
                    self.log_error("Consumer error: {}".format(msg.error()))
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        self.log_error('%% %s [%d] reached end at offset %d\n' % (msg.topic(),
                                                                                  msg.partition(), msg.offset()))
                    elif msg.error():
                        self.log_error("Consumer error: {}".format(msg.error()))
                        continue

                self.process_message(msg.topic(), msg.key(), msg.value())
                msg_count += 1
                if msg_count % self.batch_size == 0:
                    self.consumer.commit(asynchronous=False)

            except SerializerError as e:
                # Report malformed record, discard results, continue polling
                self.log_error("Message deserialization failed {}".format(e))
                continue
            except KeyboardInterrupt:
                break

        self.log_debug("Shutting down consumer..")
        self.consumer.close()
