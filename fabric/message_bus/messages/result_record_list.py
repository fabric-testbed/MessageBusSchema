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
Implements Avro representation of a Result Record List
"""
from typing import List
from uuid import uuid4

from fabric.message_bus.messages.actor_avro import ActorAvro
from fabric.message_bus.messages.delegation_avro import DelegationAvro
from fabric.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric.message_bus.messages.lease_reservation_state_avro import LeaseReservationStateAvro
from fabric.message_bus.messages.pool_info_avro import PoolInfoAvro
from fabric.message_bus.messages.proxy_avro import ProxyAvro
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.reservation_state_avro import ReservationStateAvro
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.message import IMessageAvro
from fabric.message_bus.messages.slice_avro import SliceAvro
from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric.message_bus.messages.unit_avro import UnitAvro


class ResultRecordList(IMessageAvro):
    """
    Implements Avro representation of a Result Record List
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "status", "slices", "reservations", "reservation_states", "units",
                 "proxies", "pools", "actors", "delegations", "id"]

    def __init__(self):
        self.name = IMessageAvro.ResultDelegation
        self.message_id = None
        self.status = None
        self.slices = None
        self.reservations = None
        self.reservation_states = None
        self.units = None
        self.proxies = None
        self.pools = None
        self.actors = None
        self.delegations = None
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.message_id = value['message_id']
        self.status = ResultAvro()
        self.status.from_dict(value['status'])
        slices_list = value.get('slices', None)
        if slices_list is not None:
            for s in slices_list:
                slice_obj = SliceAvro()
                slice_obj.from_dict(s)
                if self.slices is None:
                    self.slices = []
                self.slices.append(slice_obj)

        reservations_list = value.get('reservations', None)
        if reservations_list is not None:
            for s in reservations_list:
                res_obj = None
                if s.get('name') == LeaseReservationAvro.__name__:
                    res_obj = LeaseReservationAvro()
                elif s.get('name') == TicketReservationAvro.__name__:
                    res_obj = TicketReservationAvro()
                else:
                    res_obj = ReservationMng()
                res_obj.from_dict(s)
                if self.reservations is None:
                    self.reservations = []
                self.reservations.append(res_obj)

        rs_list = value.get("reservation_states", None)
        if rs_list is not None:
            self.reservation_states = []
            for rs in rs_list:
                rs_state = None
                if rs.get('name') == LeaseReservationStateAvro.__name__:
                    rs_state = LeaseReservationStateAvro()
                else:
                    rs_state = ReservationStateAvro()
                rs_state.from_dict(rs)
                self.reservation_states.append(rs_state)

        temp_units = value.get('units', None)
        if temp_units is not None:
            self.units = []
            for u in temp_units:
                unit = UnitAvro()
                unit.from_dict(u)
                self.units.append(unit)

        proxies_list = value.get('proxies', None)
        if proxies_list is not None:
            for p in proxies_list:
                proxy_obj = ProxyAvro()
                proxy_obj.from_dict(p)
                if self.proxies is None:
                    self.proxies = []
                self.proxies.append(proxy_obj)

        pools_list = value.get('pools', None)
        if pools_list is not None:
            for p in pools_list:
                pool_obj = PoolInfoAvro()
                pool_obj.from_dict(p)
                if self.pools is None:
                    self.pools = []
                self.pools.append(pool_obj)

        actors_list = value.get('actors', None)
        if actors_list is not None:
            for s in actors_list:
                actor_obj = ActorAvro()
                actor_obj.from_dict(s)
                if self.actors is None:
                    self.actors = []
                self.actors.append(actor_obj)

        delegations_list = value.get('delegations', None)
        if delegations_list is not None:
            for d in delegations_list:
                del_obj = DelegationAvro()
                del_obj.from_dict(d)
                if self.delegations is None:
                    self.delegations = []
                self.delegations.append(del_obj)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise Exception("Invalid arguments")

        result = {
            "name": self.name,
            "message_id": self.message_id,
            "status": self.status.to_dict()
        }
        if self.slices is not None:
            temp = []
            for s in self.slices:
                temp.append(s.to_dict())
            result["slices"] = temp
        if self.reservations is not None:
            temp = []
            for s in self.reservations:
                temp.append(s.to_dict())
            result["reservations"] = temp
        if self.reservation_states is not None:
            rs_list = []
            for state in self.reservation_states:
                rs_list.append(state.to_dict())
            result['reservation_states'] = rs_list
        if self.units is not None:
            temp = []
            for u in self.units:
                temp.append(u.to_dict())
            result["units"] = temp
        if self.proxies is not None:
            temp = []
            for s in self.proxies:
                temp.append(s.to_dict())
            result["proxies"] = temp
        if self.pools is not None:
            result["pools"] = []
            for p in self.pools:
                result["pools"].append(p.to_dict())
        if self.actors is not None:
            temp = []
            for s in self.actors:
                temp.append(s.to_dict())
            result["actors"] = temp
        if self.delegations is not None:
            temp = []
            for s in self.delegations:
                temp.append(s.to_dict())
            result["delegations"] = temp
        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        return self.name

    def __str__(self):
        return "name: {} message_id: {} status: {} slices: {} reservations: {} reservation_states: {} units: {} " \
               "proxies: {} pools: {} actors: {} delegations: {}".\
            format(self.name, self.message_id, self.status, self.slices, self.reservations, self.reservation_states,
                   self.units, self.proxies, self.pools, self.actors, self.delegations)

    def get_id(self) -> str:
        return self.id.__str__()

    def set_status(self, status: ResultAvro):
        """
        Set status
        """
        self.status = status

    def get_callback_topic(self) -> str:
        return None

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

    def get_pools(self) -> List[PoolInfoAvro]:
        """
        Return pools
        """
        return self.pools

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
