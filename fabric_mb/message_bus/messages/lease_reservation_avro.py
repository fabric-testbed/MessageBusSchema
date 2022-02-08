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
from __future__ import annotations

from typing import List

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.reservation_predecessor_avro import ReservationPredecessorAvro
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro


class LeaseReservationAvro(TicketReservationAvro):
    """
    Implements Avro representation of a Lease Reservation
    """
    def __init__(self):
        super(LeaseReservationAvro, self).__init__()
        self.authority = None
        self.join_state = None
        self.leased_units = None
        self.redeem_processors = []
        self.name = self.__class__.__name__

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.REDEEM_PREDECESSORS:
                    for pred in v:
                        predecessor = ReservationPredecessorAvro()
                        predecessor.from_dict(value=pred)
                        self.redeem_processors.append(predecessor)
                else:
                    self.__dict__[k] = v

    def print(self):
        """
        Print reservation on console
        Used by management cli
        """
        print("")
        print("Reservation ID: {} Slice ID: {}".format(self.reservation_id, self.slice_id))
        if self.rtype is not None or self.notices is not None:
            print("Resource Type: {} Notices: {}".format(self.rtype, self.notices))

        if self.start is not None or self.end is not None or self.requested_end is not None:
            print("Start: {} End: {} Requested End: {}".format(self.start, self.end, self.requested_end))

        if self.units is not None or self.state is not None or self.pending_state is not None:
            print("Units: {} State: {} Pending State: {}".format(self.units, self.state, self.pending_state))

        print("Broker: {}".format(self.broker))

        if self.ticket is not None:
            print("ticket properties: {}".format(self.ticket))

        if self.renewable is not None:
            print("Renewable: {}".format(self.renewable))

        if self.renew_time is not None:
            print("Renew Time: {}".format(self.renew_time))

        print("Authority: {}".format(self.authority))

        if self.join_state is not None:
            print("Join State: {}".format(self.join_state))

        if self.leased_units is not None:
            print("Leased Units: {}".format(self.leased_units))

        if self.redeem_processors is not None:
            index = 0
            for rp in self.redeem_processors:
                print("redeem Predecessor# {}: {}".format(index, rp))
                index += 1

        if self.local is not None:
            print("Local Properties: {}".format(self.local))
        if self.config is not None:
            print("Config Properties: {}".format(self.config))
        if self.request is not None:
            print("Request Properties: {}".format(self.request))
        if self.resource is not None:
            print("Resource Properties: {}".format(self.resource))
        print("")

    def get_authority(self) -> str:
        """
        Return authority
        @return authority
        """
        return self.authority

    def set_authority(self, value: str):
        """
        Set authority
        @param value value
        """
        self.authority = value

    def get_join_state(self) -> int:
        """
        Return Join State
        @return join state
        """
        return self.join_state

    def set_join_state(self, value: int):
        """
        Set join state
        @param value value
        """
        self.join_state = value

    def get_leased_units(self) -> int:
        """
        Return number of leased units
        @return leased units
        """
        return self.leased_units

    def set_leased_units(self, value: int):
        """
        Set leased units
        @param value value
        """
        self.leased_units = value

    def get_redeem_predecessors(self) -> List[ReservationPredecessorAvro]:
        """
        Return redeem processors
        @return redeem processors
        """
        return self.redeem_processors
