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
Defines AvroProducer API class which exposes interface for various producer functions
"""
import logging
import threading
import traceback

from confluent_kafka.avro import AvroProducer

from fabric_mb.message_bus.abc_mb_api import ABCMbApi
from fabric_mb.message_bus.admin import AdminApi
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.query_avro import QueryAvro
from fabric_mb.message_bus.messages.query_result_avro import QueryResultAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro


class AvroProducerApi(ABCMbApi):
    """
        This class implements the Interface for Kafka producer carrying Avro messages.
        It is expected that the users would extend this class and override on_delivery function.
    """
    def __init__(self, *, producer_conf: dict, key_schema_location, value_schema_location: str,
                 logger: logging.Logger = None, retries: int = 3):
        """
            Initialize the Producer API
            :param producer_conf: configuration e.g:
                        {'bootstrap.servers': localhost:9092,
                        'schema.registry.url': http://localhost:8083}
            :param key_schema_location: AVRO schema location for the key
            :param value_schema_location: AVRO schema location for the value
        """
        super(AvroProducerApi, self).__init__(logger=logger)
        self.lock = threading.Lock()
        self.key_schema = self.load_schema(schema_file=key_schema_location)
        self.value_schema = self.load_schema(schema_file=value_schema_location)
        self.producer = AvroProducer(producer_conf, default_key_schema=self.key_schema,
                                     default_value_schema=self.value_schema)
        self.retry_attempts = retries

    def load_schema(self, schema_file: str):
        try:
            from confluent_kafka import avro
            file = open(schema_file, "r")
            schema_bytes = file.read()
            file.close()
            return avro.loads(schema_bytes)
        except Exception as e:
            self.logger.error(f"Exception occurred while loading the schema: {schema_file}: {e}")
            self.logger.error(traceback.format_exc())

    def set_logger(self, logger):
        """
        Set logger
        :param logger: logger
        """
        self.logger = logger

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

            self.lock.acquire()
            # Pass the message asynchronously
            self.producer.produce(topic=topic, key=record.get_id(), value=record.to_dict(),
                                  callback=lambda err, msg, obj=record: self.delivery_report(err, msg, obj))
            return True
        except ValueError as ex:
            self.logger.error("KAFKA: Invalid input, discarding record...")
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


if __name__ == '__main__':
    # Create Admin API object
    conf = {'metadata.broker.list': 'localhost:19092',
            'security.protocol': 'SSL',
            'ssl.ca.location': '../../secrets/snakeoil-ca-1.crt',
            'ssl.key.location': '../../secrets/kafkacat.client.key',
            'ssl.key.password': 'confluent',
            'ssl.certificate.location': '../../secrets/kafkacat-ca1-signed.pem',
            'request.timeout.ms': 120}

    api = AdminApi(conf=conf)
    topics = ['topic1']

    # create topics
    api.create_topics(topics)

    test_key_schema = "schema/key.avsc"
    test_val_schema = "schema/message.avsc"

    producer_conf = conf.copy()
    producer_conf['schema.registry.url'] = "http://localhost:8081"

    # create a producer
    producer = AvroProducerApi(producer_conf=producer_conf, key_schema_location=test_key_schema,
                               value_schema_location=test_val_schema)

    # produce message to topics
    id_token = 'id_token'

    auth = AuthAvro()
    auth.guid = "testguid"
    auth.name = "testactor"
    auth.oidc_sub_claim = "test-oidc"

    query = QueryAvro()
    query.message_id = "msg1"
    query.callback_topic = "topic"
    query.properties = {"abc": "def"}
    query.auth = auth
    query.id_token = id_token
    producer.produce("topic1", query)

    query_result = QueryResultAvro()
    query_result.message_id = "msg2"
    query_result.request_id = "req2"
    query_result.properties = {"abc": "def"}
    query_result.auth = auth
    producer.produce("topic1", query_result)

    # delete topics
    api.delete_topics(topics)
