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
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng


class TicketReservationAvro(ReservationMng):
    """
    Implements Avro representation of a ticket Reservation from Management Interface
    """
    def __init__(self):
        super(TicketReservationAvro, self).__init__()
        self.broker = None
        self.ticket = None
        self.renewable = None
        self.renew_time = None
        self.name = self.__class__.__name__

    def print(self):
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

        if self.local is not None:
            print("Local Properties: {}".format(self.local))
        if self.config is not None:
            print("Config Properties: {}".format(self.config))
        if self.request is not None:
            print("Request Properties: {}".format(self.request))
        if self.resource is not None:
            print("Resource Properties: {}".format(self.resource))
        print("")

    def get_broker(self) -> str:
        """
        Returns broker
        @return broker
        """
        return self.broker

    def set_broker(self, value: str):
        """
        Set broker
        @param value value
        """
        self.broker = value

    def get_ticket_properties(self) -> dict:
        """
        Returns ticket properties
        @return dict
        """
        return self.ticket

    def set_ticket_properties(self, value: dict):
        """
        Set ticket properties
        @param value value
        """
        self.ticket = value

    def is_renewable(self) -> bool:
        """
        Returns true if renewable else False
        @return true if renewable else False
        """
        return self.renewable

    def set_renewable(self, value: bool):
        """
        Set renewable
        @param value value
        """
        self.renewable = value

    def get_renew_time(self) -> int:
        """
        Return renew time
        @return renew time
        """
        return self.renew_time

    def set_renew_time(self, value: int):
        """
        Set renew time
        @param value value
        """
        self.renew_time = value
