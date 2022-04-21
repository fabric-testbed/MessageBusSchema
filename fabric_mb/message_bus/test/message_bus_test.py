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
Module to test various messages
"""

import unittest
from datetime import datetime, timezone

from fabric_mb.message_bus.admin import AdminApi
from fabric_mb.message_bus.consumer import AvroConsumerApi
from fabric_mb.message_bus.messages.actor_avro import ActorAvro
from fabric_mb.message_bus.messages.add_peer_avro import AddPeerAvro
from fabric_mb.message_bus.messages.add_reservation_avro import AddReservationAvro
from fabric_mb.message_bus.messages.add_reservations_avro import AddReservationsAvro
from fabric_mb.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric_mb.message_bus.messages.add_update_reservation_record import AddUpdateReservationRecord
from fabric_mb.message_bus.messages.add_update_slice_record import AddUpdateSliceRecord
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.claim_delegation_avro import ClaimDelegationAvro
from fabric_mb.message_bus.messages.claim_resources_avro import ClaimResourcesAvro
from fabric_mb.message_bus.messages.close_avro import CloseAvro
from fabric_mb.message_bus.messages.close_reservations_avro import CloseReservationsAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.demand_reservation_avro import DemandReservationAvro
from fabric_mb.message_bus.messages.extend_lease_avro import ExtendLeaseAvro
from fabric_mb.message_bus.messages.extend_reservation_avro import ExtendReservationAvro
from fabric_mb.message_bus.messages.extend_ticket_avro import ExtendTicketAvro
from fabric_mb.message_bus.messages.failed_rpc_avro import FailedRpcAvro
from fabric_mb.message_bus.messages.get_actors_request_avro import GetActorsRequestAvro
from fabric_mb.message_bus.messages.get_delegations_avro import GetDelegationsAvro
from fabric_mb.message_bus.messages.get_broker_query_model_request_avro import GetBrokerQueryModelRequestAvro
from fabric_mb.message_bus.messages.get_reservation_units_request_avro import GetReservationUnitsRequestAvro
from fabric_mb.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric_mb.message_bus.messages.get_reservations_state_request_avro import GetReservationsStateRequestAvro
from fabric_mb.message_bus.messages.get_unit_request_avro import GetUnitRequestAvro
from fabric_mb.message_bus.messages.maintenance_request_avro import MaintenanceRequestAvro
from fabric_mb.message_bus.messages.modify_lease_avro import ModifyLeaseAvro
from fabric_mb.message_bus.messages.broker_query_model_avro import BrokerQueryModelAvro
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.request_by_id_record import RequestByIdRecord
from fabric_mb.message_bus.messages.reservation_or_delegation_record import ReservationOrDelegationRecord
from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric_mb.message_bus.messages.resource_ticket_avro import ResourceTicketAvro
from fabric_mb.message_bus.messages.result_actor_avro import ResultActorAvro
from fabric_mb.message_bus.messages.result_broker_query_model_avro import ResultBrokerQueryModelAvro
from fabric_mb.message_bus.messages.result_proxy_avro import ResultProxyAvro
from fabric_mb.message_bus.messages.result_record_list import ResultRecordList
from fabric_mb.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric_mb.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric_mb.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric_mb.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric_mb.message_bus.messages.query_avro import QueryAvro
from fabric_mb.message_bus.messages.query_result_avro import QueryResultAvro
from fabric_mb.message_bus.messages.redeem_avro import RedeemAvro
from fabric_mb.message_bus.messages.remove_reservation_avro import RemoveReservationAvro
from fabric_mb.message_bus.messages.remove_slice_avro import RemoveSliceAvro
from fabric_mb.message_bus.messages.reservation_avro import ReservationAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.resource_set_avro import ResourceSetAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.result_string_avro import ResultStringAvro
from fabric_mb.message_bus.messages.result_strings_avro import ResultStringsAvro
from fabric_mb.message_bus.messages.result_units_avro import ResultUnitsAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fabric_mb.message_bus.messages.term_avro import TermAvro
from fabric_mb.message_bus.messages.ticket import Ticket
from fabric_mb.message_bus.messages.ticket_avro import TicketAvro
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.unit_avro import UnitAvro
from fabric_mb.message_bus.messages.update_data_avro import UpdateDataAvro
from fabric_mb.message_bus.messages.update_delegation_avro import UpdateDelegationAvro
from fabric_mb.message_bus.messages.update_lease_avro import UpdateLeaseAvro
from fabric_mb.message_bus.messages.update_reservation_avro import UpdateReservationAvro
from fabric_mb.message_bus.messages.update_slice_avro import UpdateSliceAvro
from fabric_mb.message_bus.messages.update_ticket_avro import UpdateTicketAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.producer import AvroProducerApi


class MessageBusTest(unittest.TestCase):
    """
    Implements test functions
    """
    def test_consumer_producer(self):
        from threading import Thread
        import time

        conf = {'metadata.broker.list': 'localhost:19092',
                'security.protocol': 'SSL',
                'ssl.ca.location': '../../../secrets/snakeoil-ca-1.crt',
                'ssl.key.location': '../../../secrets/kafkacat.client.key',
                'ssl.key.password': 'confluent',
                'ssl.certificate.location': '../../../secrets/kafkacat-ca1-signed.pem'}
        # Create Admin API object
        api = AdminApi(conf=conf)

        for a in api.list_topics():
            print("Topic {}".format(a))

        topics = ['fabric_mb-mb-public-test1', 'fabric_mb-mb-public-test2']

        # create topics
        api.delete_topics(topics)
        api.create_topics(topics, num_partitions=1, replication_factor=1)

        # load AVRO schema
        key_schema = "../schema/key.avsc"
        value_schema = "../schema/message.avsc"

        conf['schema.registry.url'] = "http://localhost:8081"

        # create a producer
        producer = AvroProducerApi(producer_conf=conf, key_schema_location=key_schema,
                                   value_schema_location=value_schema)

        # push messages to topics

        id_token = 'id_token'
        # udd
        udd = UpdateDataAvro()
        udd.message = "message"
        udd.failed = False

        auth = AuthAvro()
        auth.guid = "testguid"
        auth.name = "testactor"
        auth.oidc_sub_claim = "test-oidc"

        # query
        query = QueryAvro()
        query.message_id = "msg1"
        query.callback_topic = "topic"
        query.properties = {"abc": "def"}
        query.auth = auth
        query.id_token = id_token
        #print(query.to_dict())
        producer.produce("fabric_mb-mb-public-test1", query)

        # query_result
        query_result = QueryResultAvro()
        query_result.message_id = "msg2"
        query_result.request_id = "req2"
        query_result.properties = {"abc": "def"}
        query_result.auth = auth
        #print(query_result.to_dict())
        producer.produce("fabric_mb-mb-public-test2", query_result)

        # FailedRPC
        failed_rpc = FailedRpcAvro()
        failed_rpc.message_id = "msg3"
        failed_rpc.request_id = "req3"
        failed_rpc.reservation_id = "rsv_abc"
        failed_rpc.request_type = 1
        failed_rpc.error_details = "test error message"
        failed_rpc.auth = auth
        #print(failed_rpc.to_dict())
        producer.produce("fabric_mb-mb-public-test2", failed_rpc)

        claim_req = ClaimResourcesAvro()
        claim_req.guid = "dummy-guid"
        claim_req.auth = auth
        claim_req.broker_id = "brokerid"
        claim_req.reservation_id = "rsv_id"
        claim_req.delegation_id = "dlg_id"
        claim_req.message_id = "test_claim_1"
        claim_req.callback_topic = "test"
        claim_req.slice_id = "slice_1"
        claim_req.id_token = id_token

        #print(claim_req.to_dict())
        producer.produce("fabric_mb-mb-public-test2", claim_req)


        reservation = ReservationAvro()
        reservation.reservation_id = "res123"
        reservation.sequence = 1
        reservation.slice = SliceAvro()
        reservation.slice.guid = "slice-12"
        reservation.slice.slice_name = "test_slice"
        reservation.slice.description = "test description"
        reservation.slice.owner = auth
        term = TermAvro()
        term.start_time = 1593854111999
        term.end_time = 1593854111999
        term.new_start_time = 1593854111999

        reservation.term = term

        reservation.resource_set = ResourceSetAvro()
        reservation.resource_set.units = 0
        reservation.resource_set.type = "type1"

        unit = UnitAvro()
        unit.properties = {'test': 'value'}
        unit.rtype = "abc"
        unit.state = 1
        unit.sequence = 0
        unit.reservation_id = 'res_123'
        unit.actor_id = 'act_1'
        unit.slice_id = 'slc_2'
        reservation.resource_set.unit_set = []
        reservation.resource_set.unit_set.append(unit)

        ticket = Ticket()
        ticket.authority = auth
        ticket.old_units = 0
        ticket.delegation_id = "dlg123"
        rt = ResourceTicketAvro()
        rt.units = 1
        rt.holder = "ab1"
        rt.issuer = "si1"
        rt.type = "rty1"
        rt.properties = {"foo": "bar"}
        rt.guid = "gid"
        rt.term = term
        ticket.resource_ticket = rt
        reservation.resource_set.ticket = ticket

        delegation = DelegationAvro()
        delegation.delegation_id = "dlg123"
        delegation.sequence = 1
        delegation.slice = SliceAvro()
        delegation.slice.guid = "slice-12"
        delegation.slice.slice_name = "test_slice"
        delegation.slice.description = "test description"
        delegation.slice.owner = auth

        claimd = ClaimDelegationAvro()
        claimd.auth = auth
        claimd.message_id = "msg4"
        claimd.callback_topic = "test"
        claimd.delegation = delegation
        claimd.id_token = id_token
        #print(claim.to_dict())
        producer.produce("fabric_mb-mb-public-test2", claimd)

        # redeem
        redeem = RedeemAvro()
        redeem.message_id = "msg4"
        redeem.callback_topic = "test"
        redeem.reservation = reservation
        redeem.auth = auth
        #print(redeem.to_dict())
        producer.produce("fabric_mb-mb-public-test2", redeem)

        update_ticket = UpdateTicketAvro()
        update_ticket.auth = auth
        update_ticket.message_id = "msg11"
        update_ticket.callback_topic = "test"
        update_ticket.reservation = reservation
        update_ticket.update_data = UpdateDataAvro()
        update_ticket.update_data.failed = False
        update_ticket.update_data.message = ""

        #print(update_ticket.to_dict())
        producer.produce("fabric_mb-mb-public-test2", update_ticket)

        update_d = UpdateDelegationAvro()
        update_d.auth = auth
        update_d.message_id = "msg11"
        update_d.callback_topic = "test"
        update_d.delegation = delegation
        update_d.update_data = UpdateDataAvro()
        update_d.update_data.failed = False
        update_d.update_data.message = ""
        update_d.id_token = id_token

        #print(update_ticket.to_dict())
        producer.produce("fabric_mb-mb-public-test2", update_d)

        get_slice = GetSlicesRequestAvro()
        get_slice.auth = auth
        get_slice.message_id = "msg11"
        get_slice.callback_topic = "test"
        get_slice.guid = "guid"
        get_slice.id_token = id_token

        #print(get_slice.to_dict())

        producer.produce("fabric_mb-mb-public-test2", get_slice)

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

        producer.produce("fabric_mb-mb-public-test2", slice_res)

        res_req = GetReservationsRequestAvro()
        res_req.message_id = "abc123"
        res_req.callback_topic = "test"
        res_req.guid = "guid"
        res_req.reservation_id = "res123"
        res_req.reservation_type = "broker"
        res_req.auth = auth
        res_req.id_token = id_token

        print(res_req.to_dict())

        producer.produce("fabric_mb-mb-public-test2", res_req)

        del_req = GetDelegationsAvro()
        del_req.message_id = "msg1"
        del_req.callback_topic = "test"
        del_req.guid = "guid"
        del_req.delegation_id = "1"
        del_req.auth = auth
        del_req.id_token = id_token

        print(del_req.to_dict())

        producer.produce("fabric_mb-mb-public-test2", del_req)

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

        producer.produce("fabric_mb-mb-public-test2", res_res)
        remove_slice = RemoveSliceAvro()
        remove_slice.message_id = "msg1"
        remove_slice.guid = 'guid1'
        remove_slice.slice_id = 'slice1'
        remove_slice.callback_topic = 'test_topic'
        remove_slice.auth = auth
        #print(remove_slice.to_dict())

        producer.produce("fabric_mb-mb-public-test2", remove_slice)

        status_resp = ResultStringAvro()
        status_resp.message_id = "msg1"
        status_resp.result = "abc"
        status_resp.status = result

        producer.produce("fabric_mb-mb-public-test2", status_resp)

        add_slice = AddSliceAvro()
        add_slice.message_id = "msg1"
        add_slice.guid = 'guid1'
        add_slice.slice_obj = s1
        add_slice.callback_topic = 'test_topic'
        add_slice.auth = auth
        # print(add_slice.to_dict())

        producer.produce("fabric_mb-mb-public-test2", add_slice)

        update_slice = UpdateSliceAvro()
        update_slice.message_id = "msg1"
        update_slice.guid = 'guid1'
        update_slice.slice_obj = s1
        update_slice.callback_topic = 'test_topic'
        update_slice.auth = auth
        # print(update_slice.to_dict())

        producer.produce("fabric_mb-mb-public-test2", update_slice)

        remove_res = RemoveReservationAvro()
        remove_res.message_id = "msg1"
        remove_res.guid = 'guid1'
        remove_res.reservation_id = 'rid1'
        remove_res.callback_topic = 'test_topic'
        remove_res.auth = auth
        # print(remove_res.to_dict())

        producer.produce("fabric_mb-mb-public-test2", remove_res)

        close_res = CloseReservationsAvro()
        close_res.message_id = "msg1"
        close_res.guid = 'guid1'
        close_res.reservation_id = 'rid1'
        close_res.callback_topic = 'test_topic'
        close_res.auth = auth
        # print(close_res.to_dict())

        producer.produce("fabric_mb-mb-public-test2", close_res)

        update_res = UpdateReservationAvro()
        update_res.message_id = "msg1"
        update_res.guid = 'guid1'
        update_res.reservation_obj = res
        update_res.callback_topic = 'test_topic'
        update_res.auth = auth
        print(update_res.to_dict())

        producer.produce("fabric_mb-mb-public-test2", update_res)

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

        producer.produce("fabric_mb-mb-public-test2", add_res)

        add_ress = AddReservationsAvro()
        add_ress.message_id = "msg1"
        add_ress.guid = 'guid1'
        add_ress.reservation_list = []
        add_ress.reservation_list.append(ticket)
        add_ress.callback_topic = 'test_topic'
        add_ress.auth = auth
        print(add_ress.to_dict())

        producer.produce("fabric_mb-mb-public-test2", add_ress)

        demand_res = DemandReservationAvro()
        demand_res.message_id = "msg1"
        demand_res.guid = 'guid1'
        demand_res.reservation_obj = res
        demand_res.callback_topic = 'test_topic'
        demand_res.auth = auth
        print(demand_res.to_dict())

        producer.produce("fabric_mb-mb-public-test2", demand_res)

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
        extend_res.end_time = int(datetime.now(timezone.utc).timestamp())
        print(extend_res.to_dict())

        producer.produce("fabric_mb-mb-public-test2", extend_res)

        close = CloseAvro()
        close.message_id = "msg1"
        close.reservation = reservation
        close.callback_topic = "topic1"
        close.auth = auth

        producer.produce("fabric_mb-mb-public-test2", close)

        extend_lease = ExtendLeaseAvro()
        extend_lease.message_id = "msg1"
        extend_lease.reservation = reservation
        extend_lease.callback_topic = "topic1"
        extend_lease.auth = auth

        producer.produce("fabric_mb-mb-public-test2", extend_lease)

        extend_ticket = ExtendTicketAvro()
        extend_ticket.message_id = "msg1"
        extend_ticket.reservation = reservation
        extend_ticket.callback_topic = "topic1"
        extend_ticket.auth = auth

        producer.produce("fabric_mb-mb-public-test2", extend_ticket)

        modify_lease = ModifyLeaseAvro()
        modify_lease.message_id = "msg1"
        modify_lease.reservation = reservation
        modify_lease.callback_topic = "topic1"
        modify_lease.auth = auth

        producer.produce("fabric_mb-mb-public-test2", modify_lease)

        update_lease = UpdateLeaseAvro()
        update_lease.message_id = "msg1"
        update_lease.reservation = reservation
        update_lease.callback_topic = "topic1"
        update_lease.update_data = UpdateDataAvro()
        update_lease.update_data.failed = False
        update_lease.update_data.message = "success"
        update_lease.auth = auth

        producer.produce("fabric_mb-mb-public-test2", update_lease)

        ticket = TicketAvro()
        ticket.message_id = "msg1"
        ticket.reservation = reservation
        ticket.callback_topic = "topic1"
        ticket.auth = auth

        producer.produce("fabric_mb-mb-public-test2", ticket)

        res_state_req = GetReservationsStateRequestAvro()
        res_state_req.guid = "gud1"
        res_state_req.message_id = "msg1"
        res_state_req.reservation_ids = []
        res_state_req.reservation_ids.append("a1")
        res_state_req.callback_topic = "topic1"
        res_state_req.auth = auth
        res_state_req.id_token = id_token

        producer.produce("fabric_mb-mb-public-test2", res_state_req)

        res_strings = ResultStringsAvro()
        res_strings.status = result
        res_strings.message_id = "msg1"
        res_strings.result = []
        res_strings.result.append("r1")

        producer.produce("fabric_mb-mb-public-test2", res_strings)

        res_state = ResultReservationStateAvro()
        res_state.status = result
        res_state.message_id = "msg1"
        res_state.reservation_states = []
        ss = ReservationStateAvro()
        ss.state = 1
        ss.pending_state = 2
        ss.rid = "rid1"
        res_state.reservation_states.append(ss)

        producer.produce("fabric_mb-mb-public-test2", res_state)

        ru = GetReservationUnitsRequestAvro()
        ru.message_id = "msg1"
        ru.reservation_id = "rid1"
        ru.guid = "gud1"
        ru.message_id = "msg1"
        ru.auth = auth
        ru.callback_topic = "test"
        ru.id_token = id_token

        producer.produce("fabric_mb-mb-public-test2", ru)

        ruu = GetUnitRequestAvro()
        ruu.message_id = "msg1"
        ruu.unit_id = "uid1"
        ruu.guid = "gud1"
        ruu.message_id = "msg1"
        ruu.auth = auth
        ruu.callback_topic = "test"
        ruu.id_token = id_token

        producer.produce("fabric_mb-mb-public-test2", ruu)

        bqm_query = GetBrokerQueryModelRequestAvro()
        bqm_query.message_id = "msg1"
        bqm_query.guid = "gud1"
        bqm_query.message_id = "msg1"
        bqm_query.auth = auth
        bqm_query.callback_topic = "test"
        bqm_query.broker_id = "broker11"
        bqm_query.id_token = id_token

        producer.produce("fabric_mb-mb-public-test2", bqm_query)


        actors_req = GetActorsRequestAvro()
        actors_req.message_id = "msg1"
        actors_req.guid = "gud1"
        actors_req.message_id = "msg1"
        actors_req.auth = auth
        actors_req.callback_topic = "test"
        actors_req.type = "Broker"
        actors_req.id_token = id_token

        producer.produce("fabric_mb-mb-public-test2", actors_req)

        result_unit = ResultUnitsAvro()
        result_unit.message_id = "msg1"
        result_unit.status = result
        result_unit.units = []
        result_unit.units.append(unit)

        producer.produce("fabric_mb-mb-public-test2", result_unit)

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

        producer.produce("fabric_mb-mb-public-test2", result_proxy)

        result_model = ResultBrokerQueryModelAvro()
        result_model.model = BrokerQueryModelAvro()
        result_model.model.level = 1
        with open('./abqm.graphml', 'r') as f:
            result_model.model.model = f.read()
        result_model.message_id = "msg1"
        result_model.status = result

        producer.produce("fabric_mb-mb-public-test2", result_model)

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

        producer.produce("fabric_mb-mb-public-test2", result_actor)

        props = {"mode": "True"}
        maint_req = MaintenanceRequestAvro(properties=props, actor_guid="am", callback_topic="test", id_token="id_token",
                                           message_id="mesg-id-1")
        producer.produce("fabric_mb-mb-public-test2", maint_req)

        #add_peer_req = AddPeerAvro(peer=proxy, callback_topic="test", id_token="token", message_id="mg-1")
        #producer.produce("fabric_mb-mb-public-test2", add_peer_req)

        # Fallback to earliest to ensure all messages are consumed
        conf['auto.offset.reset'] = "earliest"

        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print("++++++++++++++++++++++CONSUMER+++++++++++++++++++++")
        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        class TestConsumer(AvroConsumerApi):
            def set_parent(self, parent):
                self.parent = parent

            def handle_message(self, message: AbcMessageAvro):
                if message.get_message_name() == AbcMessageAvro.query:
                    self.parent.validate_query(message, query)

                elif message.get_message_name() == AbcMessageAvro.query_result:
                    self.parent.validate_query_result(message, query_result)

                elif message.get_message_name() == AbcMessageAvro.failed_rpc:
                    self.parent.validate_failed_rpc(message, failed_rpc)

                elif message.get_message_name() == AbcMessageAvro.claim_resources:
                    self.parent.validate_request_by_id(message, claim_req)

                elif message.get_message_name() == AbcMessageAvro.claim_delegation:
                    self.parent.validate_reservation_or_delegation_record(message, claimd)
                
                elif message.get_message_name() == AbcMessageAvro.redeem:
                    self.parent.validate_reservation_or_delegation_record(message, redeem)

                elif message.get_message_name() == AbcMessageAvro.update_ticket:
                    self.parent.validate_reservation_or_delegation_record(message, update_ticket)

                elif message.get_message_name() == AbcMessageAvro.update_delegation:
                    self.parent.validate_reservation_or_delegation_record(message, update_d)

                elif message.get_message_name() == AbcMessageAvro.get_slices_request:
                    self.parent.validate_request_by_id(message, get_slice)

                elif message.get_message_name() == AbcMessageAvro.result_slice:
                    self.parent.validate_result_record(message, slice_res)

                elif message.get_message_name() == AbcMessageAvro.get_reservations_request:
                    self.parent.validate_request_by_id(message, res_req)

                elif message.get_message_name() == AbcMessageAvro.get_delegations:
                    self.parent.validate_request_by_id(message, del_req)

                elif message.get_message_name() == AbcMessageAvro.result_reservation:
                    self.parent.validate_result_record(message, res_res)

                elif message.get_message_name() == AbcMessageAvro.remove_slice:
                    self.parent.validate_request_by_id(message, remove_slice)

                elif message.get_message_name() == AbcMessageAvro.result_string:
                    self.parent.validate_result_string(message, status_resp)

                elif message.get_message_name() == AbcMessageAvro.add_slice:
                    self.parent.validate_add_update_slice(message, add_slice)

                elif message.get_message_name() == AbcMessageAvro.update_slice:
                    self.parent.validate_add_update_slice(message, update_slice)

                elif message.get_message_name() == AbcMessageAvro.remove_reservation:
                    self.parent.validate_request_by_id(message, remove_res)

                elif message.get_message_name() == AbcMessageAvro.close_reservations:
                    self.parent.validate_request_by_id(message, close_res)

                elif message.get_message_name() == AbcMessageAvro.update_reservation:
                    self.parent.validate_add_update_reservation_record(message, update_res)

                elif message.get_message_name() == AbcMessageAvro.add_reservation:
                    self.parent.validate_add_update_reservation_record(message, add_res)

                elif message.get_message_name() == AbcMessageAvro.add_reservations:
                    self.parent.validate_add_reservations(message, add_ress)

                elif message.get_message_name() == AbcMessageAvro.demand_reservation:
                    self.parent.validate_add_update_reservation_record(message, demand_res)

                elif message.get_message_name() == AbcMessageAvro.extend_reservation:
                    self.parent.validate_extend_reservation(message, extend_res)

                elif message.get_message_name() == AbcMessageAvro.close:
                    self.parent.validate_reservation_or_delegation_record(message, close)

                elif message.get_message_name() == AbcMessageAvro.extend_lease:
                    self.parent.validate_reservation_or_delegation_record(message, extend_lease)

                elif message.get_message_name() == AbcMessageAvro.extend_ticket:
                    self.parent.validate_reservation_or_delegation_record(message, extend_ticket)

                elif message.get_message_name() == AbcMessageAvro.modify_lease:
                    self.parent.validate_reservation_or_delegation_record(message, modify_lease)

                elif message.get_message_name() == AbcMessageAvro.update_lease:
                    self.parent.validate_reservation_or_delegation_record(message, update_lease)

                elif message.get_message_name() == AbcMessageAvro.ticket:
                    self.parent.validate_ticket(message, ticket)

                elif message.get_message_name() == AbcMessageAvro.get_reservations_state_request:
                    self.parent.validate_get_reservations_state_request(message, res_state_req)

                elif message.get_message_name() == AbcMessageAvro.result_strings:
                    self.parent.validate_result_strings(message, res_strings)

                elif message.get_message_name() == AbcMessageAvro.result_reservation_state:
                    self.parent.validate_result_record(message, res_state)

                elif message.get_message_name() == AbcMessageAvro.get_reservation_units_request:
                    self.parent.validate_request_by_id(message, ru)

                elif message.get_message_name() == AbcMessageAvro.get_unit_request:
                    self.parent.validate_request_by_id(message, ruu)

                elif message.get_message_name() == AbcMessageAvro.get_broker_query_model_request:
                    self.parent.validate_request_by_id(message, bqm_query)

                elif message.get_message_name() == AbcMessageAvro.get_actors_request:
                    self.parent.validate_request_by_id(message, actors_req)                

                elif message.get_message_name() == AbcMessageAvro.result_reservation:
                    self.parent.validate_result_record(message, res_res)

                elif message.get_message_name() == AbcMessageAvro.result_units:
                    self.parent.validate_result_record(message, result_unit)

                elif message.get_message_name() == AbcMessageAvro.result_proxy:
                    self.parent.validate_result_record(message, result_proxy)

                elif message.get_message_name() == AbcMessageAvro.result_broker_query_model:
                    self.parent.validate_result_record(message, result_model)

                elif message.get_message_name() == AbcMessageAvro.result_actor:
                    self.parent.validate_result_record(message, result_actor)

                elif message.get_message_name() == AbcMessageAvro.maintenance_request:
                    self.parent.assertEqual(message, maint_req)

                #elif message.get_message_name() == AbcMessageAvro.add_peer:
                #    self.parent.assertEqual(message, add_peer_req)

        # create a consumer
        conf['group.id'] = 'ssl-host'
        consumer = TestConsumer(consumer_conf=conf, key_schema_location=key_schema,
                                value_schema_location=value_schema, topics=topics)
        consumer.set_parent(self)

        # start a thread to consume messages
        consume_thread = Thread(target=consumer.consume, daemon=True)

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

    def validate_failed_rpc(self, incoming: FailedRpcAvro, outgoing: FailedRpcAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.error_details, outgoing.error_details)
        self.assertEqual(incoming.request_type, outgoing.request_type)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.request_id, outgoing.request_id)
        self.assertEqual(incoming.auth, outgoing.auth)

    def validate_request_by_id(self, incoming: RequestByIdRecord, outgoing: RequestByIdRecord):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.slice_id, outgoing.slice_id)
        self.assertEqual(incoming.reservation_id, outgoing.reservation_id)
        self.assertEqual(incoming.delegation_id, outgoing.delegation_id)
        self.assertEqual(incoming.type, outgoing.type)
        self.assertEqual(incoming.unit_id, outgoing.unit_id)
        self.assertEqual(incoming.broker_id, outgoing.broker_id)
        self.assertEqual(incoming.reservation_state, outgoing.reservation_state)
        self.assertEqual(incoming.delegation_state, outgoing.delegation_state)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.id_token, outgoing.id_token)

    def validate_reservation_or_delegation_record(self, incoming: ReservationOrDelegationRecord,
                                                  outgoing: ReservationOrDelegationRecord):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.update_data, outgoing.update_data)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.id_token, outgoing.id_token)
        self.assertEqual(incoming.delegation, outgoing.delegation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_ticket(self, incoming: TicketAvro, outgoing: TicketAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation, outgoing.reservation)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_result_record(self, incoming: ResultRecordList, outgoing: ResultRecordList):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.status, outgoing.status)
        self.assertEqual(incoming.slices, outgoing.slices)
        self.assertEqual(incoming.reservations, outgoing.reservations)
        self.assertEqual(incoming.reservation_states, outgoing.reservation_states)
        self.assertEqual(incoming.units, outgoing.units)
        self.assertEqual(incoming.proxies, outgoing.proxies)
        self.assertEqual(incoming.model, outgoing.model)
        self.assertEqual(incoming.actors, outgoing.actors)
        self.assertEqual(incoming.delegations, outgoing.delegations)

    def validate_get_reservations_state_request(self, incoming: GetReservationsStateRequestAvro,
                                                outgoing: GetReservationsStateRequestAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.reservation_ids, outgoing.reservation_ids)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)

    def validate_result_string(self, incoming: ResultStringAvro, outgoing: ResultStringAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.result_str, outgoing.result_str)
        self.assertEqual(incoming.status, outgoing.status)

    def validate_result_strings(self, incoming: ResultStringsAvro, outgoing: ResultStringsAvro):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.result, outgoing.result)
        self.assertEqual(incoming.status, outgoing.status)

    def validate_add_update_slice(self, incoming: AddUpdateSliceRecord, outgoing: AddUpdateSliceRecord):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.slice_obj, outgoing.slice_obj)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.id_token, outgoing.id_token)

    def validate_add_update_reservation_record(self, incoming: AddUpdateReservationRecord, outgoing: AddUpdateReservationRecord):
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.message_id, outgoing.message_id)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.reservation_obj, outgoing.reservation_obj)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)
        self.assertEqual(incoming.id_token, outgoing.id_token)

    def validate_add_reservations(self, incoming: AddReservationsAvro, outgoing: AddReservationsAvro):
        self.assertEqual(incoming.auth, outgoing.auth)
        self.assertEqual(incoming.name, outgoing.name)
        self.assertEqual(incoming.guid, outgoing.guid)
        self.assertEqual(incoming.callback_topic, outgoing.callback_topic)
        self.assertEqual(len(incoming.reservation_list), len(outgoing.reservation_list))
        for i in range(len(incoming.reservation_list)):
            self.assertEqual(incoming.reservation_list[i], outgoing.reservation_list[i])

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
