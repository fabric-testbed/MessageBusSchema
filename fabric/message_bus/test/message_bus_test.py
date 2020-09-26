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
import unittest
from datetime import datetime

from fabric.message_bus.admin import AdminApi
from fabric.message_bus.consumer import AvroConsumerApi
from fabric.message_bus.messages.actor_avro import ActorAvro
from fabric.message_bus.messages.add_reservation_avro import AddReservationAvro
from fabric.message_bus.messages.add_reservations_avro import AddReservationsAvro
from fabric.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric.message_bus.messages.auth_avro import AuthAvro
from fabric.message_bus.messages.claim_avro import ClaimAvro
from fabric.message_bus.messages.claim_resources_avro import ClaimResourcesAvro
from fabric.message_bus.messages.close_avro import CloseAvro
from fabric.message_bus.messages.close_reservations_avro import CloseReservationsAvro
from fabric.message_bus.messages.demand_reservation_avro import DemandReservationAvro
from fabric.message_bus.messages.extend_lease_avro import ExtendLeaseAvro
from fabric.message_bus.messages.extend_reservation_avro import ExtendReservationAvro
from fabric.message_bus.messages.extend_ticket_avro import ExtendTicketAvro
from fabric.message_bus.messages.failed_rpc_avro import FailedRPCAvro
from fabric.message_bus.messages.get_actors_avro import GetActorsAvro
from fabric.message_bus.messages.get_pool_info_avro import GetPoolInfoAvro
from fabric.message_bus.messages.get_reservation_units_avro import GetReservationUnitsAvro
from fabric.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric.message_bus.messages.get_reservations_state_request_avro import GetReservationsStateRequestAvro
from fabric.message_bus.messages.get_unit_avro import GetUnitAvro
from fabric.message_bus.messages.modify_lease_avro import ModifyLeaseAvro
from fabric.message_bus.messages.pool_info_avro import PoolInfoAvro
from fabric.message_bus.messages.proxy_avro import ProxyAvro
from fabric.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric.message_bus.messages.result_actor_avro import ResultActorAvro
from fabric.message_bus.messages.result_pool_info_avro import ResultPoolInfoAvro
from fabric.message_bus.messages.result_proxy_avro import ResultProxyAvro
from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric.message_bus.messages.query_avro import QueryAvro
from fabric.message_bus.messages.query_result_avro import QueryResultAvro
from fabric.message_bus.messages.redeem_avro import RedeemAvro
from fabric.message_bus.messages.remove_reservation_avro import RemoveReservationAvro
from fabric.message_bus.messages.remove_slice_avro import RemoveSliceAvro
from fabric.message_bus.messages.reservation_avro import ReservationAvro
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.resource_data_avro import ResourceDataAvro
from fabric.message_bus.messages.resource_set_avro import ResourceSetAvro
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.result_string_avro import ResultStringAvro
from fabric.message_bus.messages.result_strings_avro import ResultStringsAvro
from fabric.message_bus.messages.result_unit_avro import ResultUnitAvro
from fabric.message_bus.messages.slice_avro import SliceAvro
from fabric.message_bus.messages.term_avro import TermAvro
from fabric.message_bus.messages.ticket_avro import TicketAvro
from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric.message_bus.messages.unit_avro import UnitAvro
from fabric.message_bus.messages.update_data_avro import UpdateDataAvro
from fabric.message_bus.messages.update_lease_avro import UpdateLeaseAvro
from fabric.message_bus.messages.update_reservation_avro import UpdateReservationAvro
from fabric.message_bus.messages.update_slice_avro import UpdateSliceAvro
from fabric.message_bus.messages.update_ticket_avro import UpdateTicketAvro
from fabric.message_bus.messages.message import IMessageAvro
from fabric.message_bus.producer import AvroProducerApi


class MessageBusTest(unittest.TestCase):
    def test_consumer_producer(self):
        from confluent_kafka import avro
        from threading import Thread
        import time

        conf = {'metadata.broker.list': 'localhost:19092',
                'security.protocol': 'SSL',
                'group.id': 'ssl-host',
                'ssl.ca.location': '../../../secrets/snakeoil-ca-1.crt',
                'ssl.key.location': '../../../secrets/kafkacat.client.key',
                'ssl.key.password': 'confluent',
                'ssl.certificate.location': '../../../secrets/kafkacat-ca1-signed.pem'
        }
        # Create Admin API object
        api = AdminApi(conf)

        for a in api.list_topics():
            print("Topic {}".format(a))

        topics = ['fabric-mb-public-test1', 'fabric-mb-public-test2']

        # create topics
        api.delete_topics(topics)
        api.create_topics(topics, num_partitions=1, replication_factor=1)

        # load AVRO schema
        file = open('../schema/key.avsc', "r")
        key_bytes = file.read()
        file.close()
        key_schema = avro.loads(key_bytes)
        file = open('../schema/message.avsc', "r")
        val_bytes = file.read()
        file.close()
        val_schema = avro.loads(val_bytes)

        conf['schema.registry.url']="http://localhost:8081"

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
        #print(query.to_dict())
        producer.produce_sync("fabric-mb-public-test1", query)

        # QueryResult
        query_result = QueryResultAvro()
        query_result.message_id = "msg2"
        query_result.request_id = "req2"
        query_result.properties = {"abc": "def"}
        query_result.auth = auth
        #print(query_result.to_dict())
        producer.produce_sync("fabric-mb-public-test2", query_result)

        # FailedRPC
        failed_rpc = FailedRPCAvro()
        failed_rpc.message_id = "msg3"
        failed_rpc.request_id = "req3"
        failed_rpc.reservation_id = "rsv_abc"
        failed_rpc.request_type = 1
        failed_rpc.error_details = "test error message"
        failed_rpc.auth = auth
        #print(failed_rpc.to_dict())
        producer.produce_sync("fabric-mb-public-test2", failed_rpc)

        claim_req = ClaimResourcesAvro()
        claim_req.guid = "dummy-guid"
        claim_req.auth = auth
        claim_req.broker_id = "brokerid"
        claim_req.reservation_id = "rsv_id"
        claim_req.message_id = "test_claim_1"
        claim_req.callback_topic = "test"
        claim_req.slice_id = "slice_1"

        #print(claim_req.to_dict())
        producer.produce_sync("fabric-mb-public-test2", claim_req)

        reservation = ReservationAvro()
        reservation.reservation_id = "res123"
        reservation.sequence = 1
        reservation.slice = SliceAvro()
        reservation.slice.guid = "slice-12"
        reservation.slice.slice_name = "test_slice"
        reservation.slice.description = "test description"
        reservation.slice.owner = auth
        reservation.term = TermAvro()
        reservation.term.start_time = 1593854111999
        reservation.term.end_time = 1593854111999
        reservation.term.new_start_time = 1593854111999
        reservation.resource_set = ResourceSetAvro()
        reservation.resource_set.units = 0
        reservation.resource_set.type = "type1"
        reservation.resource_set.resource_data = ResourceDataAvro()
        reservation.resource_set.resource_data.request_properties = {'type': 'site.vlan', 'label': 'Net AM',
                                                                     'attributescount': '1',
                                                                     'attribute.0.key': 'resource.class.invfortype',
                                                                     'resource.class.invfortype.type': '6',
                                                                     'resource.class.invfortype.value': 'actor.core.policy.SimplerUnitsInventory.SimplerUnitsInventory',
                                                                     'pool.name': 'Net AM'}

        reservation.resource_set.concrete = b'\x80\x04\x95\xb9\x02\x00\x00\x00\x00\x00\x00\x8c\x16actor.core.core.Ticket\x94\x8c\x06Ticket\x94\x93\x94)\x81\x94}\x94(\x8c\tauthority\x94\x8c,actor.core.proxies.kafka.KafkaAuthorityProxy\x94\x8c\x13KafkaAuthorityProxy\x94\x93\x94)\x81\x94}\x94(\x8c\nproxy_type\x94\x8c\x05kafka\x94\x8c\x08callback\x94\x89\x8c\nactor_name\x94\x8c\x0cfabric-vm-am\x94\x8c\nactor_guid\x94\x8c\x12actor.core.util.ID\x94\x8c\x02ID\x94\x93\x94)\x81\x94}\x94\x8c\x02id\x94\x8c\x11fabric-vm-am-guid\x94sb\x8c\x04auth\x94\x8c\x18actor.security.AuthToken\x94\x8c\tAuthToken\x94\x93\x94)\x81\x94}\x94(\x8c\x04name\x94h\x0f\x8c\x04guid\x94h\x14ub\x8c\x0bkafka_topic\x94\x8c\x12fabric-vm-am-topic\x94\x8c\x04type\x94K\x03\x8c\x10bootstrap_server\x94\x8c\x0elocalhost:9092\x94\x8c\x0fschema_registry\x94\x8c\x15http://localhost:8081\x94\x8c\x0fkey_schema_file\x94\x8cK/Users/komalthareja/renci/code/fabric/ActorBase/message_bus/schema/key.avsc\x94\x8c\x11value_schema_file\x94\x8cO/Users/komalthareja/renci/code/fabric/ActorBase/message_bus/schema/message.avsc\x94ub\x8c\x0fresource_ticket\x94N\x8c\told_units\x94K\x0fub.'

        claim = ClaimAvro()
        claim.auth = auth
        claim.message_id = "msg4"
        claim.callback_topic = "test"
        claim.reservation = reservation
        #print(claim.to_dict())
        producer.produce_sync("fabric-mb-public-test2", claim)

        # Redeem
        redeem = RedeemAvro()
        redeem.message_id = "msg4"
        redeem.callback_topic = "test"
        redeem.reservation = reservation
        redeem.auth = auth
        #print(redeem.to_dict())
        producer.produce_sync("fabric-mb-public-test2", redeem)

        update_ticket = UpdateTicketAvro()
        update_ticket.auth = auth
        update_ticket.message_id = "msg11"
        update_ticket.callback_topic = "test"
        update_ticket.reservation = reservation
        update_ticket.update_data = UpdateDataAvro()
        update_ticket.update_data.failed = False
        update_ticket.update_data.message = ""

        #print(update_ticket.to_dict())
        producer.produce_sync("fabric-mb-public-test2", update_ticket)

        get_slice = GetSlicesRequestAvro()
        get_slice.auth = auth
        get_slice.message_id = "msg11"
        get_slice.callback_topic = "test"
        get_slice.guid = "guid"

        #print(get_slice.to_dict())

        producer.produce_sync("fabric-mb-public-test2", get_slice)

        result = ResultAvro()
        result.code = 0

        slice_res = ResultSliceAvro()
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

        #print(slice_res.to_dict())

        producer.produce_sync("fabric-mb-public-test2", slice_res)

        res_req = GetReservationsRequestAvro()
        res_req.message_id = "abc123"
        res_req.callback_topic = "test"
        res_req.guid = "guid"
        res_req.reservation_id = "res123"
        res_req.reservation_type = "broker"
        res_req.auth = auth

        print(res_req.to_dict())

        producer.produce_sync("fabric-mb-public-test2", res_req)

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

        res_res = ResultReservationAvro()
        res_res.message_id = res_req.message_id
        res_res.status = result
        res_res.reservations = res_list

        #print(res_res.to_dict())

        producer.produce_sync("fabric-mb-public-test2", res_res)

        remove_slice = RemoveSliceAvro()
        remove_slice.message_id = "msg1"
        remove_slice.guid = 'guid1'
        remove_slice.slice_id = 'slice1'
        remove_slice.callback_topic = 'test_topic'
        remove_slice.auth = auth
        #print(remove_slice.to_dict())

        producer.produce_sync("fabric-mb-public-test2", remove_slice)

        status_resp = ResultStringAvro()
        status_resp.message_id = "msg1"
        status_resp.result = "abc"
        status_resp.status = result

        producer.produce_sync("fabric-mb-public-test2", status_resp)

        add_slice = AddSliceAvro()
        add_slice.message_id = "msg1"
        add_slice.guid = 'guid1'
        add_slice.slice_obj = s1
        add_slice.callback_topic = 'test_topic'
        add_slice.auth = auth
        # print(add_slice.to_dict())

        producer.produce_sync("fabric-mb-public-test2", add_slice)

        update_slice = UpdateSliceAvro()
        update_slice.message_id = "msg1"
        update_slice.guid = 'guid1'
        update_slice.slice_obj = s1
        update_slice.callback_topic = 'test_topic'
        update_slice.auth = auth
        # print(update_slice.to_dict())

        producer.produce_sync("fabric-mb-public-test2", update_slice)

        remove_res = RemoveReservationAvro()
        remove_res.message_id = "msg1"
        remove_res.guid = 'guid1'
        remove_res.reservation_id = 'rid1'
        remove_res.callback_topic = 'test_topic'
        remove_res.auth = auth
        # print(remove_res.to_dict())

        producer.produce_sync("fabric-mb-public-test2", remove_res)

        close_res = CloseReservationsAvro()
        close_res.message_id = "msg1"
        close_res.guid = 'guid1'
        close_res.reservation_id = 'rid1'
        close_res.callback_topic = 'test_topic'
        close_res.auth = auth
        # print(close_res.to_dict())

        producer.produce_sync("fabric-mb-public-test2", close_res)

        update_res = UpdateReservationAvro()
        update_res.message_id = "msg1"
        update_res.guid = 'guid1'
        update_res.reservation_obj = res
        update_res.callback_topic = 'test_topic'
        update_res.auth = auth
        print(update_res.to_dict())

        producer.produce_sync("fabric-mb-public-test2", update_res)

        ticket = TicketReservationAvro()
        ticket.reservation_id = "abcd123"
        ticket.rtype = 'site.baremetalce'
        ticket.notices = 'noice'
        ticket.slice_id = "slice_1"
        ticket.start = 1264827600000
        ticket.end = 1927515600000
        ticket.requested_end = 1927515600000
        ticket.state = 2
        ticket.pending_state = 1
        ticket.broker = "broker1"

        add_res = AddReservationAvro()
        add_res.message_id = "msg1"
        add_res.guid = 'guid1'
        add_res.reservation_obj = ticket
        add_res.callback_topic = 'test_topic'
        add_res.auth = auth
        print(add_res.to_dict())

        producer.produce_sync("fabric-mb-public-test2", add_res)

        add_ress = AddReservationsAvro()
        add_ress.message_id = "msg1"
        add_ress.guid = 'guid1'
        add_ress.reservation_list = []
        add_ress.reservation_list.append(ticket)
        add_ress.callback_topic = 'test_topic'
        add_ress.auth = auth
        print(add_ress.to_dict())

        producer.produce_sync("fabric-mb-public-test2", add_ress)

        demand_res = DemandReservationAvro()
        demand_res.message_id = "msg1"
        demand_res.guid = 'guid1'
        demand_res.reservation_obj = res
        demand_res.callback_topic = 'test_topic'
        demand_res.auth = auth
        print(demand_res.to_dict())

        producer.produce_sync("fabric-mb-public-test2", demand_res)

        extend_res = ExtendReservationAvro()
        extend_res.message_id = "msg1"
        extend_res.guid = 'guid1'
        extend_res.callback_topic = 'test_topic'
        extend_res.auth = auth
        extend_res.reservation_id = "rid1"
        extend_res.new_units = -1
        extend_res.new_resource_type = "abc"
        extend_res.request_properties = {'abcd':'eee'}
        extend_res.config_properties = {'abcd':'eee'}
        extend_res.end_time = int(datetime.now().timestamp())
        print(extend_res.to_dict())

        producer.produce_sync("fabric-mb-public-test2", extend_res)

        close = CloseAvro()
        close.message_id = "msg1"
        close.reservation = reservation
        close.callback_topic = "topic1"
        close.auth = auth

        producer.produce_sync("fabric-mb-public-test2", close)

        extend_lease = ExtendLeaseAvro()
        extend_lease.message_id = "msg1"
        extend_lease.reservation = reservation
        extend_lease.callback_topic = "topic1"
        extend_lease.auth = auth

        producer.produce_sync("fabric-mb-public-test2", extend_lease)

        extend_ticket = ExtendTicketAvro()
        extend_ticket.message_id = "msg1"
        extend_ticket.reservation = reservation
        extend_ticket.callback_topic = "topic1"
        extend_ticket.auth = auth

        producer.produce_sync("fabric-mb-public-test2", extend_ticket)

        modify_lease = ModifyLeaseAvro()
        modify_lease.message_id = "msg1"
        modify_lease.reservation = reservation
        modify_lease.callback_topic = "topic1"
        modify_lease.auth = auth

        producer.produce_sync("fabric-mb-public-test2", modify_lease)

        update_lease = UpdateLeaseAvro()
        update_lease.message_id = "msg1"
        update_lease.reservation = reservation
        update_lease.callback_topic = "topic1"
        update_lease.update_data = UpdateDataAvro()
        update_lease.update_data.failed = False
        update_lease.update_data.message = "success"
        update_lease.auth = auth

        producer.produce_sync("fabric-mb-public-test2", update_lease)

        ticket = TicketAvro()
        ticket.message_id = "msg1"
        ticket.reservation = reservation
        ticket.callback_topic = "topic1"
        ticket.auth = auth

        producer.produce_sync("fabric-mb-public-test2", ticket)

        res_state_req = GetReservationsStateRequestAvro()
        res_state_req.guid = "gud1"
        res_state_req.message_id = "msg1"
        res_state_req.reservation_ids = []
        res_state_req.reservation_ids.append("a1")
        res_state_req.callback_topic = "topic1"
        res_state_req.auth = auth

        producer.produce_sync("fabric-mb-public-test2", res_state_req)

        res_strings = ResultStringsAvro()
        res_strings.status = result
        res_strings.message_id = "msg1"
        res_strings.result = []
        res_strings.result.append("r1")


        producer.produce_sync("fabric-mb-public-test2", res_strings)

        res_state = ResultReservationStateAvro()
        res_state.status = result
        res_state.message_id = "msg1"
        res_state.reservation_states = []
        ss = ReservationStateAvro()
        ss.state = 1
        ss.pending_state = 2
        res_state.reservation_states.append(ss)

        producer.produce_sync("fabric-mb-public-test2", res_state)

        ru = GetReservationUnitsAvro()
        ru.message_id = "msg1"
        ru.reservation_id = "rid1"
        ru.guid = "gud1"
        ru.message_id = "msg1"
        ru.auth = auth
        ru.callback_topic = "test"

        producer.produce_sync("fabric-mb-public-test2", ru)

        ruu = GetUnitAvro()
        ruu.message_id = "msg1"
        ruu.unit_id = "uid1"
        ruu.guid = "gud1"
        ruu.message_id = "msg1"
        ruu.auth = auth
        ruu.callback_topic = "test"

        producer.produce_sync("fabric-mb-public-test2", ruu)

        pool_info = GetPoolInfoAvro()
        pool_info.message_id = "msg1"
        pool_info.guid = "gud1"
        pool_info.message_id = "msg1"
        pool_info.auth = auth
        pool_info.callback_topic = "test"
        pool_info.broker_id = "broker11"

        producer.produce_sync("fabric-mb-public-test2", pool_info)

        actors_req = GetActorsAvro()
        actors_req.message_id = "msg1"
        actors_req.guid = "gud1"
        actors_req.message_id = "msg1"
        actors_req.auth = auth
        actors_req.callback_topic = "test"
        actors_req.type = "Broker"

        producer.produce_sync("fabric-mb-public-test2", actors_req)

        result_unit = ResultUnitAvro()
        unit = UnitAvro()
        unit.properties = {'test':'value'}
        result_unit.message_id = "msg1"
        result_unit.status = result
        result_unit.units = []
        result_unit.units.append(unit)

        producer.produce_sync("fabric-mb-public-test2", result_unit)

        result_proxy = ResultProxyAvro()
        proxy = ProxyAvro()
        proxy.protocol = "kafka"
        proxy.name = "name"
        proxy.guid = "guid"
        proxy.type = "abcd"
        proxy.kafka_topic = "kafka_topic"
        result_proxy.message_id = "msg1"
        result_proxy.status = result
        result_proxy.proxies = []
        result_proxy.proxies.append(proxy)

        producer.produce_sync("fabric-mb-public-test2", result_proxy)

        result_pool = ResultPoolInfoAvro()
        pool = PoolInfoAvro()
        pool.name = "abc"
        pool.type = "typ1"
        pool.properties = {'test': 'value'}
        result_pool.message_id = "msg1"
        result_pool.status = result
        result_pool.pools = []
        result_pool.pools.append(pool)

        producer.produce_sync("fabric-mb-public-test2", result_pool)

        result_actor = ResultActorAvro()
        actor = ActorAvro()
        actor.name = "abcd"
        actor.owner = auth
        actor.description = "desc"
        actor.policy_module = "pol"
        actor.policy_class = "cll"
        actor.policy_guid = "guid"
        actor.actor_module = "module"
        actor.actor_class = "class"
        actor.id = "a1"
        result_actor.message_id = "msg1"
        result_actor.status = result
        result_actor.actors = []
        result_actor.actors.append(actor)

        producer.produce_sync("fabric-mb-public-test2", result_actor)


        # Fallback to earliest to ensure all messages are consumed
        conf['auto.offset.reset'] = "earliest"

        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print("++++++++++++++++++++++CONSUMER+++++++++++++++++++++")
        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        class TestConsumer(AvroConsumerApi):
            def set_parent(self, parent):
                self.parent = parent

            def handle_message(self, message: IMessageAvro):
                if message.get_message_name() == IMessageAvro.Claim:
                   self.parent.validate_claim(message, claim)

                elif message.get_message_name() == IMessageAvro.Close:
                    self.parent.validate_close(message, close)

                elif message.get_message_name() == IMessageAvro.ExtendLease:
                    self.parent.validate_extend_lease(message, extend_lease)

                elif message.get_message_name() == IMessageAvro.ExtendTicket:
                    self.parent.validate_extend_ticket(message, extend_ticket)

                elif message.get_message_name() == IMessageAvro.FailedRPC:
                   self.parent.validate_failed_rpc(message, failed_rpc)

                elif message.get_message_name() == IMessageAvro.ModifyLease:
                    self.parent.validate_modify_lease(message, modify_lease)

                elif message.get_message_name() == IMessageAvro.Query:
                   self.parent.validate_query(message, query)

                elif message.get_message_name() == IMessageAvro.QueryResult:
                   self.parent.validate_query_result(message, query_result)

                elif message.get_message_name() == IMessageAvro.Redeem:
                   self.parent.validate_redeem(message, redeem)

                elif message.get_message_name() == IMessageAvro.UpdateLease:
                   self.parent.validate_update_lease(message, update_lease)

                elif message.get_message_name() == IMessageAvro.UpdateTicket:
                   self.parent.validate_update_ticket(message, update_ticket)

                elif message.get_message_name() == IMessageAvro.Ticket:
                   self.parent.validate_ticket(message, ticket)

                elif message.get_message_name() == IMessageAvro.ClaimResources:
                   self.parent.validate_claim_resources(message, claim_req)

                elif message.get_message_name() == IMessageAvro.AddSlice:
                    self.parent.validate_add_slice(message, add_slice)

                elif message.get_message_name() == IMessageAvro.UpdateSlice:
                    self.parent.validate_update_slice(message, update_slice)

                elif message.get_message_name() == IMessageAvro.RemoveSlice:
                    self.parent.validate_remove_slice(message, remove_slice)

                elif message.get_message_name() == IMessageAvro.RemoveReservation:
                    self.parent.validate_remove_reservation(message, remove_res)

                elif message.get_message_name() == IMessageAvro.CloseReservations:
                    self.parent.validate_close_reservation(message, close_res)

                elif message.get_message_name() == IMessageAvro.UpdateReservation:
                    self.parent.validate_update_reservation(message, update_res)

                elif message.get_message_name() == IMessageAvro.AddReservation:
                    self.parent.validate_add_reservation(message, add_res)

                elif message.get_message_name() == IMessageAvro.AddReservations:
                    self.parent.validate_add_reservations(message, add_ress)

                elif message.get_message_name() == IMessageAvro.DemandReservation:
                    self.parent.validate_demand_reservation(message, demand_res)

                elif message.get_message_name() == IMessageAvro.ExtendReservation:
                    self.parent.validate_extend_reservation(message, extend_res)

                elif message.get_message_name() == IMessageAvro.GetSlicesRequest:
                   self.parent.validate_get_slices_request(message, get_slice)

                elif message.get_message_name() == IMessageAvro.GetReservationsRequest:
                   self.parent.validate_get_reservations_request(message, res_req)

                elif message.get_message_name() == IMessageAvro.GetReservationsStateRequest:
                   self.parent.validate_get_reservations_state_request(message, res_state_req)

                elif message.get_message_name() == IMessageAvro.GetUnitRequest:
                   self.parent.validate_get_unit_request(message, ruu)

                elif message.get_message_name() == IMessageAvro.GetReservationUnitsRequest:
                   self.parent.validate_get_reservations_unit_request(message, ru)

                elif message.get_message_name() == IMessageAvro.GetPoolInfoRequest:
                   self.parent.validate_get_pool_info_request(message, pool_info)

                elif message.get_message_name() == IMessageAvro.GetActorsRequest:
                   self.parent.validate_get_actors_request(message, actors_req)

                elif message.get_message_name() == IMessageAvro.ResultSlice:
                   self.parent.validate_get_slices_response(message, slice_res)

                elif message.get_message_name() == IMessageAvro.ResultReservation:
                   self.parent.validate_get_reservations_response(message, res_res)

                elif message.get_message_name() == IMessageAvro.ResultString:
                    self.parent.validate_status_response(message, status_resp)

                elif message.get_message_name() == IMessageAvro.ResultStrings:
                    self.parent.validate_result_strings(message, res_strings)

                elif message.get_message_name() == IMessageAvro.ResultReservationState:
                    self.parent.validate_result_reservation_state(message, res_state)

                elif message.get_message_name() == IMessageAvro.ResultUnits:
                    self.parent.validate_result_units(message, result_unit)

                elif message.get_message_name() == IMessageAvro.ResultProxy:
                    self.parent.validate_result_proxy(message, result_proxy)

                elif message.get_message_name() == IMessageAvro.ResultPool:
                    self.parent.validate_result_pool(message, result_pool)

                elif message.get_message_name() == IMessageAvro.ResultActor:
                    self.parent.validate_result_actor(message, result_actor)

        # create a consumer
        consumer = TestConsumer(conf, key_schema, val_schema, topics)
        consumer.set_parent(self)

        # start a thread to consume messages
        consume_thread = Thread(target=consumer.consume_auto, daemon=True)
        # consume_thread = Thread(target = consumer.consume_sync, daemon=True)

        consume_thread.start()
        time.sleep(10)

        # trigger shutdown
        consumer.shutdown()

        # delete topics
        api.delete_topics(topics)

    def validate_query(self, incoming: QueryAvro, outgoing: QueryAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.properties, outgoing.properties)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_query_result(self, incoming: QueryResultAvro, outgoing: QueryResultAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.request_id, outgoing.request_id)
        self.assertEqual(incoming.properties, outgoing.properties)
        self.assertEqual(incoming.auth, outgoing.auth)

    def validate_failed_rpc(self, incoming: FailedRPCAvro, outgoing: FailedRPCAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.error_details, outgoing.error_details)
        self.assertEqual(incoming.request_type, outgoing.request_type)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.request_id, outgoing.request_id)
        self.assertEqual(incoming.auth, outgoing.auth)

    def validate_claim_resources(self, incoming: ClaimResourcesAvro, outgoing: ClaimResourcesAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.broker_id, outgoing.broker_id)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.slice_id, outgoing.slice_id)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_claim(self, incoming: ClaimAvro, outgoing: ClaimAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_close(self, incoming: CloseAvro, outgoing: CloseAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_extend_lease(self, incoming: ExtendLeaseAvro, outgoing: ExtendLeaseAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_extend_ticket(self, incoming: ExtendTicketAvro, outgoing: ExtendTicketAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_modify_lease(self, incoming: ModifyLeaseAvro, outgoing: ModifyLeaseAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_update_lease(self, incoming: UpdateLeaseAvro, outgoing: UpdateLeaseAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.update_data, outgoing.update_data)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_redeem(self, incoming: RedeemAvro, outgoing: RedeemAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_update_ticket(self, incoming: UpdateTicketAvro, outgoing: UpdateTicketAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.update_data, outgoing.update_data)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_ticket(self, incoming: TicketAvro, outgoing: TicketAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_get_slices_request(self, incoming: GetSlicesRequestAvro, outgoing: GetSlicesRequestAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.slice_id, outgoing.slice_id)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_get_slices_response(self, incoming: ResultSliceAvro, outgoing: ResultSliceAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.slices, outgoing.slices)
        self.assertEqual(incoming.status, outgoing.status)

    def validate_get_reservations_request(self, incoming: GetReservationsRequestAvro, outgoing: GetReservationsRequestAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.slice_id, outgoing.slice_id)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.reservation_state, outgoing.reservation_state)
        self.assertEqual(incoming.type, outgoing.type)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_get_reservations_state_request(self, incoming: GetReservationsStateRequestAvro, outgoing: GetReservationsStateRequestAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation_ids, outgoing.reservation_ids)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_get_reservations_response(self, incoming: ResultReservationAvro, outgoing: ResultReservationAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservations, outgoing.reservations)
        self.assertEqual(incoming.status, outgoing.status)

    def validate_remove_slice(self, incoming: RemoveSliceAvro, outgoing: RemoveSliceAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.slice_id, outgoing.slice_id)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_status_response(self, incoming: ResultStringAvro, outgoing: ResultStringAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.result_str, outgoing.result_str)
        self.assertEqual(incoming.status, outgoing.status)

    def validate_result_strings(self, incoming: ResultStringsAvro, outgoing: ResultStringsAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.result, outgoing.result)
        self.assertEqual(incoming.status, outgoing.status)

    def validate_result_reservation_state(self, incoming: ResultReservationStateAvro, outgoing: ResultReservationStateAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.status, outgoing.status)
        self.assertEqual(len(incoming.reservation_states), len(outgoing.reservation_states))
        for i in range(len(incoming.reservation_states)):
            self.assertEqual(incoming.reservation_states[i], outgoing.reservation_states[i])

    def validate_add_slice(self, incoming: AddSliceAvro, outgoing: AddSliceAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.slice_obj, outgoing.slice_obj)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_update_slice(self, incoming: UpdateSliceAvro, outgoing: UpdateSliceAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.slice_obj, outgoing.slice_obj)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_remove_reservation(self, incoming: RemoveReservationAvro, outgoing: RemoveReservationAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_close_reservation(self, incoming: CloseReservationsAvro, outgoing: CloseReservationsAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.slice_id, outgoing.slice_id)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_update_reservation(self, incoming: UpdateReservationAvro, outgoing: UpdateReservationAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.reservation_obj, outgoing.reservation_obj)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_add_reservation(self, incoming: AddReservationAvro, outgoing: AddReservationAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.reservation_obj, outgoing.reservation_obj)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_add_reservations(self, incoming: AddReservationsAvro, outgoing: AddReservationsAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)
        self.assertEqual(len(incoming.reservation_list), len(outgoing.reservation_list))
        for i in range(len(incoming.reservation_list)):
            self.assertEqual(incoming.reservation_list[i], outgoing.reservation_list[i])

    def validate_demand_reservation(self, incoming: DemandReservationAvro, outgoing: DemandReservationAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.reservation_obj, outgoing.reservation_obj)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_extend_reservation(self, incoming: ExtendReservationAvro, outgoing: ExtendReservationAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.end_time, outgoing.end_time)
        self.assertEqual(incoming.new_resource_type, outgoing.new_resource_type)
        self.assertEqual(incoming.new_units, outgoing.new_units)
        self.assertEqual(incoming.request_properties, outgoing.request_properties)
        self.assertEqual(incoming.config_properties, outgoing.config_properties)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_result_units(self, incoming: ResultUnitAvro, outgoing: ResultUnitAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.status, outgoing.status)
        self.assertEqual(len(incoming.units), len(outgoing.units))
        for i in range(len(incoming.units)):
            self.assertEqual(incoming.units[i], outgoing.units[i])

    def validate_result_proxy(self, incoming: ResultProxyAvro, outgoing: ResultProxyAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.status, outgoing.status)
        self.assertEqual(len(incoming.proxies), len(outgoing.proxies))
        for i in range(len(incoming.proxies)):
            self.assertEqual(incoming.proxies[i], outgoing.proxies[i])

    def validate_result_pool(self, incoming: ResultPoolInfoAvro, outgoing: ResultPoolInfoAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.status, outgoing.status)
        self.assertEqual(len(incoming.pools), len(outgoing.pools))
        for i in range(len(incoming.pools)):
            self.assertEqual(incoming.pools[i], outgoing.pools[i])

    def validate_result_actor(self, incoming: ResultActorAvro, outgoing: ResultActorAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.status, outgoing.status)
        self.assertEqual(len(incoming.actors), len(outgoing.actors))
        for i in range(len(incoming.actors)):
            self.assertEqual(incoming.actors[i], outgoing.actors[i])

    def validate_get_unit_request(self, incoming: GetUnitAvro, outgoing: GetUnitAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.unit_id, outgoing.unit_id)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_get_reservations_unit_request(self, incoming: GetReservationUnitsAvro, outgoing: GetReservationUnitsAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_get_pool_info_request(self, incoming: GetPoolInfoAvro, outgoing: GetPoolInfoAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.broker_id, outgoing.broker_id)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_get_actors_request(self, incoming: GetActorsAvro, outgoing: GetActorsAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.type, outgoing.type)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)