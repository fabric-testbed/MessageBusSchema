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
Defines Admin API class which exposes interface for various admin client functions
"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

from fabric_mb.message_bus.abc_mb_api import ABCMbApi


class AdminApi(ABCMbApi):
    """
    Implements interface for Admin APIs
    """
    def __init__(self, *, conf: dict, logger: logging.Logger = None):
        super().__init__(logger=logger)
        self.admin_client = AdminClient(conf)

    def create_topics(self, topics, num_partitions: int = 3, replication_factor: int = 1):
        """
            Create a list of topics
            :param topics: list of topics to be created
            :param num_partitions: number of partitions for the topic
            :param replication_factor: replication factor
            :return:
        """

        new_topics = [NewTopic(topic, num_partitions=num_partitions,
                               replication_factor=replication_factor) for topic in topics]

        # Call create_topics to asynchronously create topics, a dict
        # of <topic,future> is returned.
        fs = self.admin_client.create_topics(new_topics)

        # Wait for operation to finish.
        # Timeouts are preferably controlled by passing request_timeout=15.0
        # to the create_topics() call.
        # All futures will finish at the same time.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                self.logger.debug(f"Topic {topic} created")
            except Exception as e:
                self.logger.error(f"Failed to create topic {topic}: {e}")

    def delete_topics(self, topics):
        """
            delete list of topics
            :param topics: list of topics
            :return:
        """

        # Call delete_topics to asynchronously delete topics, a future is returned.
        # By default this operation on the broker returns immediately while
        # topics are deleted in the background. But here we give it some time (30s)
        # to propagate in the cluster before returning.
        #
        # Returns a dict of <topic,future>.
        fs = self.admin_client.delete_topics(topics, operation_timeout=30)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                self.logger.debug(f"Topic {topic} deleted".format(topic))
            except Exception as e:
                self.logger.error(f"Failed to delete topic {topic}: {e}")

    def create_partitions(self, topic_partitions):
        """
            create partitions for list of topics
            :param topic_partitions: list of tuples (topic, number of partitions)
            :return:
        """

        new_parts = [NewPartitions(topic, int(new_total_count)) for
                     topic, new_total_count in topic_partitions]

        # Try switching validate_only to True to only validate the operation
        # on the broker but not actually perform it.
        fs = self.admin_client.create_partitions(new_parts, validate_only=False)

        # Wait for operation to finish.
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                self.logger.debug(f"Additional partitions created for topic {topic}")
            except Exception as e:
                self.logger.error(f"Failed to add partitions to topic {topic}: {e}")

    def list_topics(self, timeout: int = 10) -> list:
        """
            list topics and cluster metadata
            :param type: list topics or brokers or all; allowed values (all|topics|brokers)
            :param timeout: timeout in ms
            :return:
        """
        result = []
        md = self.admin_client.list_topics(timeout=timeout)

        for t in iter(md.topics.values()):
            result.append(str(t))

        return result


if __name__ == '__main__':

    # create admin client
    conf = {'metadata.broker.list': 'localhost:19092',
            'security.protocol': 'SSL',
            'ssl.ca.location': '../../secrets/snakeoil-ca-1.crt',
            'ssl.key.location': '../../secrets/kafkacat.client.key',
            'ssl.key.password': 'confluent',
            'ssl.certificate.location': '../../secrets/kafkacat-ca1-signed.pem'}
    api = AdminApi(conf=conf)
    test_topics = ['topic1', "topic2"]

    # create topics
    api.create_topics(test_topics)
    test_topic_partitions = [('topic1', 4), ("topic2", 4)]

    # create partitions
    api.create_partitions(test_topic_partitions)
    time.sleep(5)

    # delete partitions
    api.delete_topics(test_topics)
