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
Implements Avro Message Base class
"""
from abc import ABC, abstractmethod
from uuid import uuid4


class AbcMessageAvro(ABC):
    """
        Implements the base class for storing the deserialized Avro record. Each message is uniquely identified by
        a globally unique identifier. It must be inherited to include Actor specific fields and to_dict implementation.
        New Avro schema must be defined as per the inherited class and should be used for the producer/consumer
    """
    """
    NAMING Convention: Respective Class Name for each message should look like: <Message Name>Avro
    """
    claim_delegation = "ClaimDelegation"
    reclaim_delegation = "ReclaimDelegation"
    close = "Close"
    extend_lease = "ExtendLease"
    extend_ticket = "ExtendTicket"
    failed_rpc = "FailedRpc"
    modify_lease = "ModifyLease"
    query = "Query"
    query_result = "QueryResult"
    redeem = "Redeem"
    relinquish = "Relinquish"
    update_lease = "UpdateLease"
    update_ticket = "UpdateTicket"
    ticket = "Ticket"
    update_delegation = "UpdateDelegation"

    # Management APIs
    claim_resources = "ClaimResources"
    reclaim_resources = "ReclaimResources"
    remove_slice = "RemoveSlice"
    add_slice = "AddSlice"
    update_slice = "UpdateSlice"
    remove_reservation = "RemoveReservation"
    close_reservations = "CloseReservations"
    update_reservation = "UpdateReservation"
    add_reservation = "AddReservation"
    add_reservations = "AddReservations"
    demand_reservation = "DemandReservation"
    extend_reservation = "ExtendReservation"

    get_reservations_state_request = "GetReservationsStateRequest"
    get_slices_request = "GetSlicesRequest"
    get_reservations_request = "GetReservationsRequest"
    get_delegations = "GetDelegations"
    get_reservation_units_request = "GetReservationUnitsRequest"
    get_unit_request = "GetUnitRequest"
    get_broker_query_model_request = "GetBrokerQueryModelRequest"
    get_actors_request = "GetActorsRequest"

    result_slice = "ResultSlice"
    result_reservation = "ResultReservation"
    result_delegation = "ResultDelegation"
    result_reservation_state = "ResultReservationState"
    result_strings = "ResultStrings"
    result_string = "ResultString"
    result_units = "ResultUnits"
    result_proxy = "ResultProxy"
    result_broker_query_model = "ResultBrokerQueryModel"
    result_actor = "ResultActor"

    def __init__(self):
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()
        self.message_id = None
        self.name = None
        self.callback_topic = None
        self.kafka_error = None

    @abstractmethod
    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """

    @abstractmethod
    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        """
        Returns the message name
        """
        return self.name

    def get_callback_topic(self) -> str:
        """
        Returns the callback topic
        """
        return self.callback_topic

    def get_id(self) -> str:
        """
        Returns the id
        """
        return self.id.__str__()

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.get_message_name() is None or self.get_message_id() is None or self.get_id() is None:
            ret_val = False
        return ret_val

    def set_kafka_error(self, kafka_error):
        self.kafka_error = kafka_error

    def get_kafka_error(self):
        return self.kafka_error