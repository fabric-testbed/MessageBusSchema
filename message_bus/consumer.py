#!/usr/bin/env python3
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from confluent_kafka.avro import AvroConsumer, SerializerError
from confluent_kafka.cimpl import KafkaError

from message_bus.message import IMessage


class AvroConsumerApi:
    def __init__(self, conf, record_schema, topics, batchSize = 5):
        self.consumer = AvroConsumer(conf, reader_value_schema=record_schema)
        self.running = True
        self.topics = topics
        self.batchSize = batchSize

    def shutdown(self):
        """
        Shutdown the consumer
        :return:
        """
        print("Trigger shutdown")
        self.running = False

    def process_message(self, topic: str, message: IMessage):
        """
        Process the incoming message. Must be overridden in the derived class
        :param message: incoming message
        :return:
        """
        print("Message received for topic " + topic)
        message.print()

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
                    print("Consumer error: {}".format(msg.error()))
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        print("Consumer error: {}".format(msg.error()))
                        continue

                self.process_message(msg.topic(), IMessage(msg.value()))
            except SerializerError as e:
                # Report malformed record, discard results, continue polling
                print("Message deserialization failed {}".format(e))
                continue
            except KeyboardInterrupt:
                break

        print("Shutting down consumer..")
        self.consumer.close()

    def consume_sync(self):
        """
            Consume records unless shutdown triggered. Using synchronous commit after a message batch.
        """
        self.consumer.subscribe(topics)

        msg_count = 0
        while self.running:
            try:
                msg = self.consumer.poll(1)

                # There were no messages on the queue, continue polling
                if msg is None:
                    continue

                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        print("Consumer error: {}".format(msg.error()))
                        continue

                self.process_message(msg.topic(), IMessage(msg.value()))
                msg_count += 1
                if msg_count % self.batch_size == 0:
                    self.consumer.commit(asynchronous=False)

            except SerializerError as e:
                # Report malformed record, discard results, continue polling
                print("Message deserialization failed {}".format(e))
                continue
            except KeyboardInterrupt:
                break

        print("Shutting down consumer..")
        self.consumer.close()


if __name__ == '__main__':
    from message_bus.admin import AdminApi
    from message_bus.producer import AvroProducerApi
    from confluent_kafka import avro
    from threading import Thread
    import time

    # Create Admin API object
    api = AdminApi("localhost:9092")
    topics = ['topic1', 'topic2']

    # create topics
    api.create_topics(topics)

    conf = {'bootstrap.servers': "localhost:9092",
            'schema.registry.url': "http://localhost:8081"}

    # load AVRO schema
    schema = avro.loads(open('schema/message.avsc', "r").read())

    # create a producer
    producer = AvroProducerApi(conf, schema)

    # push messages to topics
    for x in range(0,5):
        producer.produce_sync("topic1", IMessage("slice" + str(x)))

    for x in range(11,15):
        producer.produce_sync("topic2", IMessage("slice" + str(x)))

    # Fallback to earliest to ensure all messages are consumed
    conf['group.id'] = "example_avro"
    conf['auto.offset.reset'] = "earliest"

    # create a consumer
    consumer = AvroConsumerApi(conf, schema, topics)

    # start a thread to consume messages
    consume_thread = Thread(target = consumer.consume_auto, daemon=True)
    #consume_thread = Thread(target = consumer.consume_sync, daemon=True)

    consume_thread.start()
    time.sleep(10)

    # trigger shutdown
    consumer.shutdown()

    # delete topics
    api.delete_topics(topics)
