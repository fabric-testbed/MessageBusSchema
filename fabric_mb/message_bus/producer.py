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
import traceback
import threading
import logging
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro

from fabric_mb.message_bus.abc_mb_api import ABCMbApi
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient


class AvroProducerApi(ABCMbApi):
    """
    This class implements the Interface for Kafka producer carrying Avro messages.
    It is expected that the users would extend this class and override on_delivery function.
    """
    def __init__(self, *, producer_conf: dict, schema_registry_conf: dict,
                 value_schema_location: str, logger: logging.Logger = None, retries: int = 3):
        """
        Initialize the Producer API
        :param producer_conf: configuration e.g:
                    {'bootstrap.servers': localhost:9092,
                    'schema.registry.url': http://localhost:8083}
        :param key_schema_str: AVRO schema string for the key
        :param value_schema_str: AVRO schema string for the value
        """
        super(AvroProducerApi, self).__init__()
        self.lock = threading.Lock()

        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        self.value_schema = self.load_schema(schema_file=value_schema_location)

        self.string_serializer = StringSerializer('utf_8')

        self.value_serializer = AvroSerializer(schema_str=self.value_schema,
                                               schema_registry_client=self.schema_registry_client,
                                               to_dict=self.to_dict)

        # Construct Producer with AvroSerializer instances
        self.producer = Producer(producer_conf)
        self.retry_attempts = retries

        self.logger = logger if logger else logging.getLogger(__name__)

    def delivery_report(self, err, msg, obj: AbcMessageAvro):
        """
        Handle delivery reports served from producer.poll.
        This callback takes an extra argument, obj.
        This allows the original contents to be included for debugging purposes.
        """
        if err is not None:
            self.logger.error(f"KAFKA: Message Delivery Failure! Error [{err}] MsgId: [{obj.id}] "
                              f"Msg Name: [{obj.name}]")
            obj.set_kafka_error(kafka_error=err)
        else:
            self.logger.debug(f"KAFKA: Message Delivery Successful! MsgId: [{obj.id}] Msg Name: [{obj.name}] "
                              f"Topic: [{msg.topic()}] Partition [{msg.partition()}] Offset [{msg.offset()}]")

    def produce(self, topic, record: AbcMessageAvro) -> bool:
        """
        Produce records for a specific topic
        :param topic: topic to which messages are written to
        :param record: record/message to be written
        :return:
        """
        try:
            self.logger.debug(f"KAFKA: Record type={type(record)}")
            self.logger.debug(f"KAFKA: Producing key {record.get_id()} to topic {topic}.")
            self.logger.debug(f"KAFKA: Producing record {record.to_dict()} to topic {topic}.")

            value_bytes = self.value_serializer(record, SerializationContext(topic, MessageField.VALUE))

            self.lock.acquire()
            # Pass the message asynchronously
            self.producer.produce(topic=topic, key=self.string_serializer(record.get_id()), value=value_bytes,
                                  callback=lambda err, msg, obj=record: self.delivery_report(err, msg, obj))
            return True
        except ValueError as ex:
            self.logger.error("KAFKA: Avro serialization error, discarding record...")
            self.logger.error(f"KAFKA: Exception occurred {ex}")
            self.logger.error(traceback.format_exc())
        except Exception as ex:
            self.logger.error(f"KAFKA: Exception occurred {ex}")
            self.logger.error(traceback.format_exc())
        finally:
            if self.lock.locked():
                self.lock.release()
        return False

    def poll(self, timeout: float = 0.0):
        """
        Poll for delivery callbacks
        :param timeout: timeout
        :return:
        """
        try:
            self.lock.acquire()
            return self.producer.poll(timeout=timeout)
        finally:
            self.lock.release()

    def flush(self, timeout: float = None):
        """
        Flush all pending writes
        :param timeout: timeout
        :return:
        """
        try:
            self.lock.acquire()
            self.producer.flush()
        finally:
            self.lock.release()

    @staticmethod
    def to_dict(record: AbcMessageAvro, ctx: SerializationContext):
        return record.to_dict()
