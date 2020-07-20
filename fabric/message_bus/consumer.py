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

from fabric.message_bus.admin import AdminApi
from fabric.message_bus.base import Base
from fabric.message_bus.messages.AddSliceAvro import AddSliceAvro
from fabric.message_bus.messages.AuthAvro import AuthAvro
from fabric.message_bus.messages.ClaimAvro import ClaimAvro
from fabric.message_bus.messages.ClaimResourcesAvro import ClaimResourcesAvro
from fabric.message_bus.messages.ClaimResourcesResponseAvro import ClaimResourcesResponseAvro
from fabric.message_bus.messages.CloseAvro import CloseAvro
from fabric.message_bus.messages.CloseReservationsAvro import CloseReservationsAvro
from fabric.message_bus.messages.ExtendLeaseAvro import ExtendLeaseAvro
from fabric.message_bus.messages.ExtendTicketAvro import ExtendTicketAvro
from fabric.message_bus.messages.FailedRPCAvro import FailedRPCAvro
from fabric.message_bus.messages.GetReservationsRequestAvro import GetReservationsRequestAvro
from fabric.message_bus.messages.GetReservationsResponseAvro import GetReservationsResponseAvro
from fabric.message_bus.messages.GetReservationsStateRequestAvro import GetReservationsStateRequestAvro
from fabric.message_bus.messages.GetReservationsStateResponseAvro import GetReservationsStateResponseAvro
from fabric.message_bus.messages.GetSlicesRequestAvro import GetSlicesRequestAvro
from fabric.message_bus.messages.GetSlicesResponseAvro import GetSlicesResponseAvro
from fabric.message_bus.messages.ModifyLeaseAvro import ModifyLeaseAvro
from fabric.message_bus.messages.QueryAvro import QueryAvro
from fabric.message_bus.messages.QueryResultAvro import QueryResultAvro
from fabric.message_bus.messages.RedeemAvro import RedeemAvro
from fabric.message_bus.messages.RelinquishAvro import RelinquishAvro
from fabric.message_bus.messages.RemoveReservationAvro import RemoveReservationAvro
from fabric.message_bus.messages.RemoveSliceAvro import RemoveSliceAvro
from fabric.message_bus.messages.ReservationAvro import ReservationAvro
from fabric.message_bus.messages.ReservationMng import ReservationMng
from fabric.message_bus.messages.ResourceDataAvro import ResourceDataAvro
from fabric.message_bus.messages.ResourceSetAvro import ResourceSetAvro
from fabric.message_bus.messages.ResultAvro import ResultAvro
from fabric.message_bus.messages.SliceAvro import SliceAvro
from fabric.message_bus.messages.StatusResponseAvro import StatusResponseAvro
from fabric.message_bus.messages.TermAvro import TermAvro
from fabric.message_bus.messages.TicketAvro import TicketAvro
from fabric.message_bus.messages.UpdateDataAvro import UpdateDataAvro
from fabric.message_bus.messages.UpdateLeaseAvro import UpdateLeaseAvro
from fabric.message_bus.messages.UpdateReservationAvro import UpdateReservationAvro
from fabric.message_bus.messages.UpdateSliceAvro import UpdateSliceAvro
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
        elif value['name'] == IMessageAvro.RemoveSlice:
            message = RemoveSliceAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.StatusResponse:
            message = StatusResponseAvro()
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
        elif value['name'] == IMessageAvro.GetReservationsStateRequest:
            message = GetReservationsStateRequestAvro()
            message.from_dict(value)
        elif value['name'] == IMessageAvro.GetReservationsStateResponse:
            message = GetReservationsStateResponseAvro()
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
