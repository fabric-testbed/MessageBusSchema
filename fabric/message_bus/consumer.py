#!/usr/bin/env python3
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################


from confluent_kafka.avro import AvroConsumer, SerializerError
from confluent_kafka.cimpl import KafkaError

from fabric.message_bus.admin import AdminApi
from fabric.message_bus.base import Base
from fabric.message_bus.messages.AuthAvro import AuthAvro
from fabric.message_bus.messages.ClaimAvro import ClaimAvro
from fabric.message_bus.messages.ClaimResourcesAvro import ClaimResourcesAvro
from fabric.message_bus.messages.ClaimResourcesResponseAvro import ClaimResourcesResponseAvro
from fabric.message_bus.messages.CloseAvro import CloseAvro
from fabric.message_bus.messages.ExtendLeaseAvro import ExtendLeaseAvro
from fabric.message_bus.messages.ExtendTicketAvro import ExtendTicketAvro
from fabric.message_bus.messages.FailedRPCAvro import FailedRPCAvro
from fabric.message_bus.messages.GetReservationsRequest import GetReservationsRequestAvro
from fabric.message_bus.messages.GetReservationsResponse import GetReservationsResponseAvro
from fabric.message_bus.messages.GetSlicesRequestAvro import GetSlicesRequestAvro
from fabric.message_bus.messages.GetSlicesResponseAvro import GetSlicesResponseAvro
from fabric.message_bus.messages.ModifyLeaseAvro import ModifyLeaseAvro
from fabric.message_bus.messages.QueryAvro import QueryAvro
from fabric.message_bus.messages.QueryResultAvro import QueryResultAvro
from fabric.message_bus.messages.RedeemAvro import RedeemAvro
from fabric.message_bus.messages.RelinquishAvro import RelinquishAvro
from fabric.message_bus.messages.ReservationAvro import ReservationAvro
from fabric.message_bus.messages.ReservationMng import ReservationMng
from fabric.message_bus.messages.ResourceDataAvro import ResourceDataAvro
from fabric.message_bus.messages.ResourceSetAvro import ResourceSetAvro
from fabric.message_bus.messages.ResultAvro import ResultAvro
from fabric.message_bus.messages.SliceAvro import SliceAvro
from fabric.message_bus.messages.TermAvro import TermAvro
from fabric.message_bus.messages.TicketAvro import TicketAvro
from fabric.message_bus.messages.UpdateDataAvro import UpdateDataAvro
from fabric.message_bus.messages.UpdateLeaseAvro import UpdateLeaseAvro
from fabric.message_bus.messages.UpdateTicketAvro import UpdateTicketAvro
from fabric.message_bus.messages.message import IMessageAvro
from fabric.message_bus.producer import AvroProducerApi


class AvroConsumerApi(Base):
    def __init__(self, conf, key_schema, record_schema, topics, batchSize = 5, logger=None):
        super().__init__(logger)
        self.consumer = AvroConsumer(conf, reader_key_schema=key_schema, reader_value_schema=record_schema)
        self.running = True
        self.topics = topics
        self.batch_size = batchSize

    def shutdown(self):
        """
        Shutdown the consumer
        :return:
        """
        self.log_debug("Trigger shutdown")
        self.running = False

    def process_message(self, topic: str, key: dict, value: dict):
        """
        Process the incoming message. Must be overridden in the derived class
        :param key: incoming message key
        :param value: incoming message value
        :return:
        """
        self.log_debug("Message received for topic " + topic)
        self.log_debug("Key = {}".format(key))
        self.log_debug("Value = {}".format(value))
        if value['name'] == IMessageAvro.Query:
            message = QueryAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.QueryResult:
            message = QueryResultAvro()
            message.from_dict(value)
            self.log_debug("QueryResult: {}".format(message))
        elif value['name'] == IMessageAvro.FailedRPC:
            message = FailedRPCAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.Redeem:
            message = RedeemAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.Ticket:
            message = TicketAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.Claim:
            message = ClaimAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ExtendTicket:
            message = ExtendTicketAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.Relinquish:
            message = RelinquishAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ExtendLease:
            message = ExtendLeaseAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ModifyLease:
            message = ModifyLeaseAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.Close:
            message = CloseAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.UpdateTicket:
            message = UpdateTicketAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.UpdateLease:
            message = UpdateLeaseAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ClaimResources:
            message = ClaimResourcesAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ClaimResourcesResponse:
            message = ClaimResourcesResponseAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetSlicesRequest:
            message = GetSlicesRequestAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetSlicesResponse:
            message = GetSlicesResponseAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetReservationsRequest:
            message = GetReservationsRequestAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetReservationsResponse:
            message = GetReservationsResponseAvro()
            message.from_dict(value)
        else:
            self.log_error("Unsupported message: {}".format(value))
            return

        self.handle_message(message)

    def handle_message(self, message: IMessageAvro):
        print(message)
        return

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
                        self.log_error('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
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
        self.consumer.subscribe(topics)

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
                        self.log_error('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
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


if __name__ == '__main__':

    from confluent_kafka import avro
    from threading import Thread
    import time

    # Create Admin API object
    api = AdminApi("localhost:9092")

    for a in api.list_topics():
        print("Topic {}".format(a))


    tt=["fabric-broker-topic", "fabric-vm-am-topic"]
    api.delete_topics(tt)

    topics = ['topic1', 'topic2']

    # create topics
    api.delete_topics(topics)
    api.create_topics(topics, num_partitions=1, replication_factor=1)

    conf = {'bootstrap.servers': "localhost:9092",
            'schema.registry.url': "http://localhost:8081"}

    # load AVRO schema
    file = open('schema/key.avsc', "r")
    key_bytes = file.read()
    file.close()
    key_schema = avro.loads(key_bytes)
    file = open('schema/message.avsc', "r")
    val_bytes = file.read()
    file.close()
    val_schema = avro.loads(val_bytes)


    # create a producer
    producer = AvroProducerApi(conf, key_schema, val_schema)

    # push messages to topics

    # udd
    udd = UpdateDataAvro()
    udd.message = "message"
    udd.failed = False

    auth = AuthAvro()
    auth.guid = "testguid"
    auth.name = "testactor"
    
    # Query
    query = QueryAvro()
    query.message_id = "msg1"
    query.callback_topic = "topic"
    query.properties = {"abc": "def"}
    query.auth = auth
    print(query.to_dict())
    producer.produce_sync("topic1", query)

    # QueryResult
    query_result = QueryResultAvro()
    query_result.message_id = "msg2"
    query_result.request_id = "req2"
    query_result.properties = {"abc": "def"}
    query_result.auth = auth
    print(query_result.to_dict())
    producer.produce_sync("topic2", query_result)

    # FailedRPC
    failed_rpc = FailedRPCAvro()
    failed_rpc.message_id = "msg3"
    failed_rpc.request_id = "req3"
    failed_rpc.reservation_id = "rsv_abc"
    failed_rpc.request_type = 1
    failed_rpc.error_details = "test error message"
    failed_rpc.auth = auth
    print(failed_rpc.to_dict())
    producer.produce_sync("topic2", failed_rpc)

    claim_req = ClaimResourcesAvro()
    claim_req.guid = "dummy-guid"
    claim_req.auth = auth
    claim_req.broker_id = "brokerid"
    claim_req.reservation_id = "rsv_id"
    claim_req.message_id = "test_claim_1"
    claim_req.callback_topic = "test"
    claim_req.slice_id = "null"

    print(claim_req.to_dict())
    producer.produce_sync("topic2", claim_req)
    
    reservation = ReservationAvro()
    reservation.reservation_id = "res123"
    reservation.sequence = 1
    reservation.slice = SliceAvro()
    reservation.slice.guid = "slice-12"
    reservation.slice.slice_name = "test_slice"
    reservation.term = TermAvro()
    reservation.term.start_time = 1593854111999
    reservation.term.end_time = 1593854111999
    reservation.term.new_start_time = 1593854111999
    reservation.resource_set = ResourceSetAvro()
    reservation.resource_set.units = 0
    reservation.resource_set.type = "type1"
    reservation.resource_set.resource_data = ResourceDataAvro()
    reservation.resource_set.resource_data.request_properties = {'type': 'site.vlan', 'label': 'Net AM', 'attributescount': '1', 'attribute.0.key': 'resource.class.invfortype', 'resource.class.invfortype.type': '6', 'resource.class.invfortype.value': 'actor.core.policy.SimplerUnitsInventory.SimplerUnitsInventory', 'pool.name': 'Net AM'}

    claim = ClaimAvro()
    claim.auth = auth
    claim.message_id = "msg4"
    claim.callback_topic = "test"
    claim.reservation = reservation
    print(claim.to_dict())
    producer.produce_sync("topic2", claim)

    # Redeem
    redeem = RedeemAvro()
    redeem.message_id = "msg4"
    redeem.callback_topic = "test"
    redeem.reservation = reservation
    print(redeem.to_dict())
    producer.produce_sync("topic2", redeem)


    update_ticket = UpdateTicketAvro()
    update_ticket.auth = auth
    update_ticket.message_id = "msg11"
    update_ticket.callback_topic = "test"
    update_ticket.reservation = reservation
    update_ticket.update_data = UpdateDataAvro()
    update_ticket.update_data.failed = False

    reservation.resource_set.concrete = b'\x80\x04\x95\xb9\x02\x00\x00\x00\x00\x00\x00\x8c\x16actor.core.core.Ticket\x94\x8c\x06Ticket\x94\x93\x94)\x81\x94}\x94(\x8c\tauthority\x94\x8c,actor.core.proxies.kafka.KafkaAuthorityProxy\x94\x8c\x13KafkaAuthorityProxy\x94\x93\x94)\x81\x94}\x94(\x8c\nproxy_type\x94\x8c\x05kafka\x94\x8c\x08callback\x94\x89\x8c\nactor_name\x94\x8c\x0cfabric-vm-am\x94\x8c\nactor_guid\x94\x8c\x12actor.core.util.ID\x94\x8c\x02ID\x94\x93\x94)\x81\x94}\x94\x8c\x02id\x94\x8c\x11fabric-vm-am-guid\x94sb\x8c\x04auth\x94\x8c\x18actor.security.AuthToken\x94\x8c\tAuthToken\x94\x93\x94)\x81\x94}\x94(\x8c\x04name\x94h\x0f\x8c\x04guid\x94h\x14ub\x8c\x0bkafka_topic\x94\x8c\x12fabric-vm-am-topic\x94\x8c\x04type\x94K\x03\x8c\x10bootstrap_server\x94\x8c\x0elocalhost:9092\x94\x8c\x0fschema_registry\x94\x8c\x15http://localhost:8081\x94\x8c\x0fkey_schema_file\x94\x8cK/Users/komalthareja/renci/code/fabric/ActorBase/message_bus/schema/key.avsc\x94\x8c\x11value_schema_file\x94\x8cO/Users/komalthareja/renci/code/fabric/ActorBase/message_bus/schema/message.avsc\x94ub\x8c\x0fresource_ticket\x94N\x8c\told_units\x94K\x0fub.'
    print(update_ticket.to_dict())
    producer.produce_sync("topic2", update_ticket)

    get_slice = GetSlicesRequestAvro()
    get_slice.auth = auth
    get_slice.message_id = "msg11"
    get_slice.callback_topic = "test"
    get_slice.guid = "guid"

    print(get_slice.to_dict())

    producer.produce_sync("topic2", get_slice)

    result = ResultAvro()
    result.code = 0

    slice_res = GetSlicesResponseAvro()
    slice_res.message_id = "msg11"
    slice_res.status = result


    s1 = SliceAvro()
    s1.set_slice_name("abc")
    s1.set_slice_id("11111")
    s1.set_owner(auth)
    s1.set_description("abcd")
    prop = {}
    s1.set_config_properties(prop)
    s1.set_resource_type('site.vm')
    s1.set_client_slice(False)
    s1.set_broker_client_slice(False)

    slice_res.slices = []
    slice_res.slices.append(s1)

    print(slice_res.to_dict())

    producer.produce_sync("topic2", slice_res)

    res_req = GetReservationsRequestAvro()
    res_req.message_id = "abc123"
    res_req.callback_topic = "test"
    res_req.guid = "guid"

    print(res_req.to_dict())

    producer.produce_sync("topic2", res_req)

    res = ReservationMng()
    res.reservation_id = "abcd123"
    res.rtype = 'site.baremetalce'
    res.notices = 'noice'
    res.slice_id = "slice_1"
    res.start = 1264827600000
    res.end = 1927515600000
    res.requested_end = 1927515600000
    res.state = 2
    res.pending_state = 1

    res_list = [res]

    res_res = GetReservationsResponseAvro()
    res_res.message_id = res_req.message_id
    res_res.status = result
    res_res.reservations = res_list

    print(res_res.to_dict())

    producer.produce_sync("topic2", res_res)

    # Fallback to earliest to ensure all messages are consumed
    conf['group.id'] = "example_avro"
    conf['auto.offset.reset'] = "earliest"

    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("++++++++++++++++++++++CONSUMER+++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    # create a consumer
    consumer = AvroConsumerApi(conf, key_schema, val_schema, topics)

    # start a thread to consume messages
    consume_thread = Thread(target = consumer.consume_auto, daemon=True)
    #consume_thread = Thread(target = consumer.consume_sync, daemon=True)

    consume_thread.start()
    time.sleep(10)

    # trigger shutdown
    consumer.shutdown()

    # delete topics
    api.delete_topics(topics)
