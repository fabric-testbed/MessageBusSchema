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
from fabric_mb.message_bus.messages.reservation_state_avro import ReservationStateAvro


class LeaseReservationStateAvro(ReservationStateAvro):
    """
    Implements Avro representation of a Lease Reservation State
    """
    def __init__(self):
        super(LeaseReservationStateAvro, self).__init__()
        self.name = self.__class__.__name__
        self.joining = None

    def get_joining(self) -> int:
        """
        Return join state
        """
        return self.joining

    def set_joining(self, value: int):
        """
        Set join state
        @param value value
        """
        self.joining = value

    def from_dict(self, values: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.name = values.get('name', None)
        self.state = values.get('state', None)
        self.pending_state = values.get('pending_state', None)
        self.joining = values.get('joining', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            'name':self.name,
            'state':self.state,
            'pending_state': self.pending_state}
        if self.joining is not None:
            result['joining'] = self.joining
        return result

    def __str__(self):
        return "state: {} pending_state: {} joining: {}".format(self.state, self.pending_state, self.joining)
