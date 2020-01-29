#!/usr/bin/env python3
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################
import time

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka import KafkaException


class AdminApi:
    def __init__(self, broker_address: str):
        self.admin_client = AdminClient({'bootstrap.servers': broker_address})

    def create_topics(self, topics, num_partitions: int = 3, replication_factor: int = 1):
        """
            Create a list of topics
            :param topics: list of topics to be created
            :param num_partitions: number of partitions for the topic
            :param replication_factor: replication factor
            :return:
        """

        new_topics = [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor) for topic in topics]

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
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))

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
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))

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
                print("Additional partitions created for topic {}".format(topic))
            except Exception as e:
                print("Failed to add partitions to topic {}: {}".format(topic, e))

    def _print_config(self, config, depth):
        """
            Prints the config
            :param config: config to be printed
            :param depth: number of spaces to be printed before config
            :return:
        """
        print('%40s = %-50s  [%s,is:read-only=%r,default=%r,sensitive=%r,synonym=%r,synonyms=%s]' %
              ((' ' * depth) + config.name, config.value, ConfigSource(config.source),
               config.is_read_only, config.is_default,
               config.is_sensitive, config.is_synonym,
               ["%s:%s" % (x.name, ConfigSource(x.source))
                for x in iter(config.synonyms.values())]))

    def describe_configs(self, resources):
        """
            describe configs
            :param resources: list of tuples (resource type, resource name)
            :return:
        """

        resources = [ConfigResource(restype, resname) for
                     restype, resname in resources]

        fs = self.admin_client.describe_configs(resources)

        # Wait for operation to finish.
        for res, f in fs.items():
            try:
                configs = f.result()
                for config in iter(configs.values()):
                    self._print_config(config, 1)

            except KafkaException as e:
                print("Failed to describe {}: {}".format(res, e))
            except Exception:
                raise

    def alter_configs(self, resource_list):
        """
            Alter configs atomically, replacing non-specified
            configuration properties with their default values.
            :param resource_list: list of tuples (resource type, resource name, list of config params <config=val,config2=val2>)
            :return:
        """

        resources = []
        for restype, resname, configs in resource_list:
            resource = ConfigResource(restype, resname)
            resources.append(resource)
            for k, v in [conf.split('=') for conf in configs.split(',')]:
                resource.set_config(k, v)

        fs = self.admin_client.alter_configs(resources)

        # Wait for operation to finish.
        for res, f in fs.items():
            try:
                f.result()  # empty, but raises exception on failure
                print("{} configuration successfully altered".format(res))
            except Exception:
                raise

    def list(self, type=None):
        """
            list topics and cluster metadata
            :param type: list topics or brokers or all; allowed values (all|topics|brokers)
            :return:
        """

        if type is None:
            what = "all"
        else:
            what = type

        md = self.admin_client.list_topics(timeout=10)

        print("Cluster {} metadata (response from broker {}):".format(md.cluster_id, md.orig_broker_name))

        if what in ("all", "brokers"):
            print(" {} brokers:".format(len(md.brokers)))
            for b in iter(md.brokers.values()):
                if b.id == md.controller_id:
                    print("  {}  (controller)".format(b))
                else:
                    print("  {}".format(b))

        if what not in ("all", "topics"):
            return

        print(" {} topics:".format(len(md.topics)))
        for t in iter(md.topics.values()):
            if t.error is not None:
                errstr = ": {}".format(t.error)
            else:
                errstr = ""

            print("  \"{}\" with {} partition(s){}".format(t, len(t.partitions), errstr))

            for p in iter(t.partitions.values()):
                if p.error is not None:
                    errstr = ": {}".format(p.error)
                else:
                    errstr = ""

                print("    partition {} leader: {}, replicas: {}, isrs: {}".format(
                    p.id, p.leader, p.replicas, p.isrs, errstr))


if __name__ == '__main__':

    # create admin client
    api = AdminApi("localhost:9092")
    topics = ['topic1', "topic2"]

    # create topics
    api.create_topics(topics)
    topic_partitions = [('topic1', 4), ("topic2", 4)]

    # create partitions
    api.create_partitions(topic_partitions)
    time.sleep(5)

    # delete partitions
    api.delete_topics(topics)