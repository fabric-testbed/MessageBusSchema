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
Implements Avro representation of a ticket Reservation from Management Interface
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng


class TicketReservationAvro(ReservationMng):
    """
    Implements Avro representation of a ticket Reservation from Management Interface
    """
    def __init__(self):
        super().__init__()
        self.broker = None
        self.ticket = None
        self.renewable = None
        self.renew_time = None
        self.name = self.__class__.__name__

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        super().from_dict(value)
        self.broker = value.get('broker', None)
        self.ticket = value.get('ticket', None)
        self.renewable = value.get('renewable', None)
        self.renew_time = value.get('renew_time', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = super().to_dict()
        if result is None:
            result = {}

        if self.broker is not None:
            result['broker'] = self.broker

        if self.ticket is not None:
            result['ticket'] = self.ticket

        if self.renewable is not None:
            result['renewable'] = self.renewable

        if self.renew_time is not None:
            result['renew_time'] = self.renew_time

        return result

    def __str__(self):
        prev_result = super().__str__()
        return "{} broker: {} ticket: {} renewable: {} renew_time: {}".format(prev_result, self.broker, self.ticket,
                                                                              self.renewable, self.renew_time)

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

    def __eq__(self, other):
        if not isinstance(other, TicketReservationAvro):
            return False

        return self.name == other.name and self.reservation_id == other.reservation_id and \
            self.slice_id == other.slice_id and self.start == other.start and self.end == other.end and \
            self.requested_end == other.requested_end and self.rtype == other.rtype and self.units == other.units and \
            self.state == other.state and self.pending_state == other.pending_state and self.local == other.local and \
            self.request == other.request and self.resource == other.resource and self.notices == other.notices and \
            self.broker == other.broker and self.ticket == other.ticket and self.renewable == other.renewable and \
            self.renewable == other.renew_time
