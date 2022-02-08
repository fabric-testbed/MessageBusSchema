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
from typing import List
from uuid import uuid4

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.actor_avro import ActorAvro
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric_mb.message_bus.messages.lease_reservation_state_avro import LeaseReservationStateAvro
from fabric_mb.message_bus.messages.broker_query_model_avro import BrokerQueryModelAvro
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric_mb.message_bus.messages.unit_avro import UnitAvro


class ResultRecordList(AbcMessageAvro):
    """
    Implements Avro representation of a Result Record List
    """
    def __init__(self):
        super(ResultRecordList, self).__init__()
        self.status = None
        self.slices = None
        self.reservations = None
        self.reservation_states = None
        self.units = None
        self.proxies = None
        self.model = None
        self.actors = None
        self.delegations = None

    def from_dict_slices(self, value: list):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            for s in value:
                slice_obj = SliceAvro()
                slice_obj.from_dict(s)
                if self.slices is None:
                    self.slices = []
                self.slices.append(slice_obj)

    def from_dict_reservations(self, value: list):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            for s in value:
                res_obj = None
                if s.get(Constants.NAME) == LeaseReservationAvro.__name__:
                    res_obj = LeaseReservationAvro()
                elif s.get(Constants.NAME) == TicketReservationAvro.__name__:
                    res_obj = TicketReservationAvro()
                else:
                    res_obj = ReservationMng()
                res_obj.from_dict(s)
                if self.reservations is None:
                    self.reservations = []
                self.reservations.append(res_obj)

    def from_dict_reservation_states(self, value: list):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            self.reservation_states = []
            for rs in value:
                rs_state = None
                if rs.get(Constants.NAME) == LeaseReservationStateAvro.__name__:
                    rs_state = LeaseReservationStateAvro()
                else:
                    rs_state = ReservationStateAvro()
                rs_state.from_dict(rs)
                self.reservation_states.append(rs_state)

    def from_dict_units(self, value: list):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            self.units = []
            for u in value:
                unit = UnitAvro()
                unit.from_dict(u)
                self.units.append(unit)

    def from_dict_proxies(self, value: list):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            for p in value:
                proxy_obj = ProxyAvro()
                proxy_obj.from_dict(p)
                if self.proxies is None:
                    self.proxies = []
                self.proxies.append(proxy_obj)

    def from_dict_model(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            self.model = BrokerQueryModelAvro()
            self.model.from_dict(value)

    def from_dict_actors(self, value: list):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            for s in value:
                actor_obj = ActorAvro()
                actor_obj.from_dict(s)
                if self.actors is None:
                    self.actors = []
                self.actors.append(actor_obj)

    def from_dict_delegations(self, value: list):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            for d in value:
                del_obj = DelegationAvro()
                del_obj.from_dict(d)
                if self.delegations is None:
                    self.delegations = []
                self.delegations.append(del_obj)

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.STATUS:
                    self.status = ResultAvro()
                    self.status.from_dict(value=v)
                elif k == Constants.SLICES:
                    self.from_dict_slices(value=v)
                elif k == Constants.RESERVATIONS:
                    self.from_dict_reservations(value=v)
                elif k == Constants.RESERVATION_STATES:
                    self.from_dict_reservation_states(value=v)
                elif k == Constants.UNITS:
                    self.from_dict_units(value=v)
                elif k == Constants.PROXIES:
                    self.from_dict_proxies(value=v)
                elif k == Constants.MODEL:
                    self.from_dict_model(value=v)
                elif k == Constants.ACTORS:
                    self.from_dict_actors(value=v)
                elif k == Constants.DELEGATIONS:
                    self.from_dict_delegations(value=v)
                else:
                    self.__dict__[k] = v

    def set_status(self, status: ResultAvro):
        """
        Set status
        """
        self.status = status

    def get_status(self) -> ResultAvro:
        """
        Return status
        """
        return self.status

    def get_slices(self) -> List[SliceAvro]:
        """
        Return slices
        """
        return self.slices

    def get_reservations(self) -> List[ReservationMng]:
        """
        Return reservations
        """
        return self.reservations

    def get_reservation_states(self) -> List[ReservationStateAvro]:
        """
        Return reservation states
        """
        return self.reservation_states

    def get_units(self) -> List[UnitAvro]:
        """
        Return units
        """
        return self.units

    def get_proxies(self) -> List[ProxyAvro]:
        """
        Return proxies
        """
        return self.proxies

    def get_model(self) -> BrokerQueryModelAvro:
        """
        Return model
        """
        return self.model

    def get_actors(self) -> List[ActorAvro]:
        """
        Return actors
        """
        return self.actors

    def get_delegations(self) -> List[DelegationAvro]:
        """
        Return delegations
        """
        return self.delegations

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()
        if self.status is None:
            ret_val = False
        return ret_val
