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


from confluent_kafka.avro import AvroConsumer, SerializerError
from confluent_kafka.cimpl import KafkaError

from fabric.message_bus.base import Base
from fabric.message_bus.messages.add_reservation_avro import AddReservationAvro
from fabric.message_bus.messages.add_reservations_avro import AddReservationsAvro
from fabric.message_bus.messages.add_slice_avro import AddSliceAvro
from fabric.message_bus.messages.claim_avro import ClaimAvro
from fabric.message_bus.messages.claim_delegation_avro import ClaimDelegationAvro
from fabric.message_bus.messages.claim_resources_avro import ClaimResourcesAvro
from fabric.message_bus.messages.close_avro import CloseAvro
from fabric.message_bus.messages.close_reservations_avro import CloseReservationsAvro
from fabric.message_bus.messages.demand_reservation_avro import DemandReservationAvro
from fabric.message_bus.messages.extend_lease_avro import ExtendLeaseAvro
from fabric.message_bus.messages.extend_reservation_avro import ExtendReservationAvro
from fabric.message_bus.messages.extend_ticket_avro import ExtendTicketAvro
from fabric.message_bus.messages.failed_rpc_avro import FailedRPCAvro
from fabric.message_bus.messages.get_delegations_avro import GetDelegationsAvro
from fabric.message_bus.messages.get_pool_info_avro import GetPoolInfoAvro
from fabric.message_bus.messages.get_reservation_units_avro import GetReservationUnitsAvro
from fabric.message_bus.messages.get_reservations_request_avro import GetReservationsRequestAvro
from fabric.message_bus.messages.get_unit_avro import GetUnitAvro
from fabric.message_bus.messages.reclaim_avro import ReclaimAvro
from fabric.message_bus.messages.reclaim_delegation_avro import ReclaimDelegationAvro
from fabric.message_bus.messages.reclaim_resources_avro import ReclaimResourcesAvro
from fabric.message_bus.messages.result_actor_avro import ResultActorAvro
from fabric.message_bus.messages.result_delegation_avro import ResultDelegationAvro
from fabric.message_bus.messages.result_pool_info_avro import ResultPoolInfoAvro
from fabric.message_bus.messages.result_proxy_avro import ResultProxyAvro
from fabric.message_bus.messages.result_reservation_avro import ResultReservationAvro
from fabric.message_bus.messages.get_reservations_state_request_avro import GetReservationsStateRequestAvro
from fabric.message_bus.messages.result_reservation_state_avro import ResultReservationStateAvro
from fabric.message_bus.messages.get_slices_request_avro import GetSlicesRequestAvro
from fabric.message_bus.messages.result_slice_avro import ResultSliceAvro
from fabric.message_bus.messages.modify_lease_avro import ModifyLeaseAvro
from fabric.message_bus.messages.query_avro import QueryAvro
from fabric.message_bus.messages.query_result_avro import QueryResultAvro
from fabric.message_bus.messages.redeem_avro import RedeemAvro
from fabric.message_bus.messages.relinquish_avro import RelinquishAvro
from fabric.message_bus.messages.remove_reservation_avro import RemoveReservationAvro
from fabric.message_bus.messages.remove_slice_avro import RemoveSliceAvro
from fabric.message_bus.messages.result_string_avro import ResultStringAvro
from fabric.message_bus.messages.result_strings_avro import ResultStringsAvro
from fabric.message_bus.messages.result_unit_avro import ResultUnitAvro
from fabric.message_bus.messages.ticket_avro import TicketAvro
from fabric.message_bus.messages.update_delegation_avro import UpdateDelegationAvro
from fabric.message_bus.messages.update_lease_avro import UpdateLeaseAvro
from fabric.message_bus.messages.update_reservation_avro import UpdateReservationAvro
from fabric.message_bus.messages.update_slice_avro import UpdateSliceAvro
from fabric.message_bus.messages.update_ticket_avro import UpdateTicketAvro
from fabric.message_bus.messages.message import IMessageAvro


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
        :param topic: topic name
        :param key: incoming message key
        :param value: incoming message value
        :return:
        """
        self.log_debug("Message received for topic " + topic)
        self.log_debug("Key = {}".format(key))
        self.log_debug("Value = {}".format(value))
        message = None
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
        elif value['name'] == IMessageAvro.Reclaim:
            message = ReclaimAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ClaimDelegation:
            message = ClaimDelegationAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ReclaimDelegation:
            message = ReclaimDelegationAvro()
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
        elif value['name'] == IMessageAvro.UpdateDelegation:
            message = UpdateDelegationAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.UpdateLease:
            message = UpdateLeaseAvro()
            message.from_dict(value)
        # Management Messages
        elif value['name'] == IMessageAvro.ClaimResources:
            message = ClaimResourcesAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ReclaimResources:
            message = ReclaimResourcesAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.RemoveSlice:
            message = RemoveSliceAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.AddSlice:
            message = AddSliceAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.UpdateSlice:
            message = UpdateSliceAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.RemoveReservation:
            message = RemoveReservationAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.CloseReservations:
            message = CloseReservationsAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.UpdateReservation:
            message = UpdateReservationAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.AddReservation:
            message = AddReservationAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.AddReservations:
            message = AddReservationsAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.DemandReservation:
            message = DemandReservationAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ExtendReservation:
            message = ExtendReservationAvro()
            message.from_dict(value)


        # Get Messages
        elif value['name'] == IMessageAvro.GetSlicesRequest:
            message = GetSlicesRequestAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetReservationsRequest:
            message = GetReservationsRequestAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetDelegations:
            message = GetDelegationsAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetReservationsStateRequest:
            message = GetReservationsStateRequestAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetReservationUnitsRequest:
            message = GetReservationUnitsAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetUnitRequest:
            message = GetUnitAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetPoolInfoRequest:
            message = GetPoolInfoAvro()
            message.from_dict(value)

        # Responses
        elif value['name'] == IMessageAvro.ResultSlice:
            message = ResultSliceAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultReservation:
            message = ResultReservationAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultDelegation:
            message = ResultDelegationAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultReservationState:
            message = ResultReservationStateAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultStrings:
            message = ResultStringsAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultString:
            message = ResultStringAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultUnits:
            message = ResultUnitAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultProxy:
            message = ResultProxyAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultPool:
            message = ResultPoolInfoAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.ResultActor:
            message = ResultActorAvro()
            message.from_dict(value)
        else:
            self.log_error("Unsupported message: {}".format(value))
            return

        self.handle_message(message=message)

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
        self.consumer.subscribe(self.topics)

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
