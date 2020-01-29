#!/usr/bin/env python3
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from confluent_kafka.avro import AvroProducer

from message_bus.message import IMessage


class AvroProducerApi:
    """
        This class implements the Interface for Kafka producer carrying Avro messages.
        It is expected that the users would extend this class and override on_delivery function.
    """
    def __init__(self, conf, record_schema):
        """
            Initialize the Producer API
            :param conf: configuration e.g:
                        {'bootstrap.servers': localhost:9092,
                        'schema.registry.url': http://localhost:8083}
            :param record_schema: loaded AVRO schema
        """
        self.producer = AvroProducer(conf, default_value_schema=record_schema)

    def delivery_report(self, err, msg, obj):
        """
            Handle delivery reports served from producer.poll.
            This callback takes an extra argument, obj.
            This allows the original contents to be included for debugging purposes.
        """
        if err is not None:
            print('Message {} delivery failed for user {} with error {}'.format(
                obj.id, obj.name, err))
        else:
            print('Message {} successfully produced to {} [{}] at offset {}'.format(
                obj.id, msg.topic(), msg.partition(), msg.offset()))

    def produce_async(self, topic, record: IMessage):
        """
            Produce records for a specific topic
            :param topic: topic to which messages are written to
            :param record: record/message to be written
            :return:
        """
        print("Producing records to topic {}.".format(topic))
        try:
            # The message passed to the delivery callback will already be serialized.
            # To aid in debugging we provide the original object to the delivery callback.
            self.producer.produce(topic=topic, value=record.to_dict(),
                             callback=lambda err, msg, obj=record: self.delivery_report(err, msg, obj))
            # Serve on_delivery callbacks from previous asynchronous produce()
            self.producer.poll(0)
        except ValueError:
            print("Invalid input, discarding record...")

    def produce_sync(self, topic, record: IMessage):
        """
            Produce records for a specific topic
            :param topic: topic to which messages are written to
            :param record: record/message to be written
            :return:
        """
        print("Producing records to topic {}.".format(topic))
        try:
            # Pass the message synchronously
            self.producer.produce(topic=topic, value=record.to_dict())
            self.producer.flush()
        except ValueError:
            print("Invalid input, discarding record...")


if __name__ == '__main__':
    from message_bus.admin import AdminApi
    from confluent_kafka import avro

    # Create Admin API object
    api = AdminApi("localhost:9092")
    topics = ['topic1']

    # create topics
    api.create_topics(topics)

    conf = {'bootstrap.servers': "localhost:9092",
            'schema.registry.url': "http://localhost:8081"}

    # load AVRO schema
    schema = avro.loads(open('schema/message.avsc', "r").read())

    # create a producer
    producer = AvroProducerApi(conf, schema)

    # produce message to topics
    message = IMessage("slice1")
    producer.produce_sync("topic1", message)
    message = IMessage("slice2")
    producer.produce_sync("topic1", message)

    # delete topics
    api.delete_topics(topics)