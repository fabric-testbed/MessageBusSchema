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
import traceback

from confluent_kafka.avro import AvroProducer

from fabric_mb.message_bus.admin import AdminApi
from fabric_mb.message_bus.base import Base
from fabric_mb.message_bus.messages.query_avro import QueryAvro
from fabric_mb.message_bus.messages.query_result_avro import QueryResultAvro
from fabric_mb.message_bus.messages.message import IMessageAvro


class AvroProducerApi(Base):
    """
        This class implements the Interface for Kafka producer carrying Avro messages.
        It is expected that the users would extend this class and override on_delivery function.
    """
    def __init__(self, conf, key_schema, record_schema, logger=None):
        """
            Initialize the Producer API
            :param conf: configuration e.g:
                        {'bootstrap.servers': localhost:9092,
                        'schema.registry.url': http://localhost:8083}
            :param key_schema: loaded AVRO schema for the key
            :param record_schema: loaded AVRO schema for the value
        """
        super().__init__(logger)
        self.producer = AvroProducer(conf, default_key_schema=key_schema, default_value_schema=record_schema)

    def set_logger(self, logger):
        """
        Set logger
        :param logger: logger
        """
        self.logger = logger

    def delivery_report(self, err, msg, obj):
        """
            Handle delivery reports served from producer.poll.
            This callback takes an extra argument, obj.
            This allows the original contents to be included for debugging purposes.
        """
        if err is not None:
            self.log_error('Message {} delivery failed for user {} with error {}'.format(
                obj.id, obj.name, err))
        else:
            self.log_debug('Message {} successfully produced to {} [{}] at offset {}'.format(
                obj.id, msg.topic(), msg.partition(), msg.offset()))

    def produce_async(self, topic, record: IMessageAvro) -> bool:
        """
            Produce records for a specific topic
            :param topic: topic to which messages are written to
            :param record: record/message to be written
            :return:
        """
        self.log_debug("Producing records to topic {}.".format(topic))
        try:
            # The message passed to the delivery callback will already be serialized.
            # To aid in debugging we provide the original object to the delivery callback.
            self.producer.produce(topic=topic, key=record.get_id(), value=record.to_dict(),
                                  callback=lambda err, msg, obj=record: self.delivery_report(err, msg, obj))
            # Serve on_delivery callbacks from previous asynchronous produce()
            self.producer.poll(0)
            return True
        except ValueError as ex:
            traceback.print_exc()
            self.log_error("Invalid input, discarding record...{}".format(ex))
        return False

    def produce_sync(self, topic, record: IMessageAvro) -> bool:
        """
            Produce records for a specific topic
            :param topic: topic to which messages are written to
            :param record: record/message to be written
            :return:
        """
        self.log_debug("Record type={}".format(type(record)))
        self.log_debug("Producing key {} to topic {}.".format(record.get_id(), topic))
        self.log_debug("Producing record {} to topic {}.".format(record.to_dict(), topic))

        try:
            # Pass the message synchronously
            self.producer.produce(topic=topic, key=record.get_id(), value=record.to_dict())
            self.producer.flush()
            return True
        except ValueError as ex:
            traceback.print_exc()
            self.log_error("Invalid input, discarding record...")
            self.log_error(str(ex))
        return False


if __name__ == '__main__':
    from confluent_kafka import avro

    # Create Admin API object
    api = AdminApi("localhost:9092")
    topics = ['topic1']

    # create topics
    api.create_topics(topics)

    test_conf = {'bootstrap.servers': "localhost:9092",
                 'schema.registry.url': "http://localhost:8081"}

    # load AVRO schema
    test_key_schema = avro.loads(open('schema/key.avsc', "r").read())
    test_val_schema = avro.loads(open('schema/message.avsc', "r").read())

    # create a producer
    producer = AvroProducerApi(test_conf, test_key_schema, test_val_schema)

    # produce message to topics
    message = QueryAvro()
    message.message_id = "msg1"
    message.properties = {"abc": "def"}
    producer.produce_sync("topic1", message)

    message = QueryResultAvro()
    message.message_id = "msg2"
    message.properties = {"abc": "def"}
    producer.produce_sync("topic1", message)

    # delete topics
    api.delete_topics(topics)
