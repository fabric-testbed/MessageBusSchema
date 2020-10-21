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


class IMessageAvro:
    """
        Implements the base class for storing the deserialized Avro record. Each message is uniquely identified by
        a globally unique identifier. It must be inherited to include Actor specific fields and to_dict implementation.
        New Avro schema must be defined as per the inherited class and should be used for the producer/consumer
    """
    Claim = "Claim"
    ClaimDelegation = "ClaimDelegation"
    Reclaim = "Reclaim"
    ReclaimDelegation = "ReclaimDelegation"
    Close = "Close"
    ExtendLease = "ExtendLease"
    ExtendTicket = "ExtendTicket"
    FailedRPC = "FailedRPC"
    ModifyLease = "ModifyLease"
    Query = "Query"
    QueryResult = "QueryResult"
    Redeem = "Redeem"
    Relinquish = "Relinquish"
    UpdateLease = "UpdateLease"
    UpdateTicket = "UpdateTicket"
    Ticket = "Ticket"
    UpdateDelegation = "UpdateDelegation"

    # Management APIs
    ClaimResources = "ClaimResources"
    ReclaimResources = "ReclaimResources"
    RemoveSlice = "RemoveSlice"
    AddSlice = "AddSlice"
    UpdateSlice = "UpdateSlice"
    RemoveReservation = "RemoveReservation"
    CloseReservations = "CloseReservations"
    UpdateReservation = "UpdateReservation"
    AddReservation = "AddReservation"
    AddReservations = "AddReservations"
    DemandReservation = "DemandReservation"
    ExtendReservation = "ExtendReservation"

    GetReservationsStateRequest = "GetReservationsStateRequest"
    GetSlicesRequest = "GetSlicesRequest"
    GetReservationsRequest = "GetReservationsRequest"
    GetDelegations = "GetDelegations"
    GetReservationUnitsRequest = "GetReservationUnitsRequest"
    GetUnitRequest = "GetUnitRequest"
    GetPoolInfoRequest = "GetPoolInfoRequest"
    GetActorsRequest = "GetActorsRequest"

    ResultSlice = "ResultSlice"
    ResultReservation = "ResultReservation"
    ResultDelegation = "ResultDelegation"
    ResultReservationState = "ResultReservationState"
    ResultStrings = "ResultStrings"
    ResultString = "ResultString"
    ResultUnits = "ResultUnits"
    ResultProxy = "ResultProxy"
    ResultPool = "ResultPool"
    ResultActor = "ResultActor"


    def to_dict(self) -> dict:
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        raise NotImplementedError

    def from_dict(self, value: dict):
        raise NotImplementedError

    def get_message_id(self) -> str:
        raise NotImplementedError

    def get_message_name(self) -> str:
        raise NotImplementedError

    def get_callback_topic(self) -> str:
        raise NotImplementedError

    def get_id(self) -> str:
        raise NotImplementedError

    def validate(self) -> bool:
        ret_val = True
        if self.get_message_name() is None or self.get_message_id() is None or self.get_id() is None:
            ret_val = False
        return ret_val