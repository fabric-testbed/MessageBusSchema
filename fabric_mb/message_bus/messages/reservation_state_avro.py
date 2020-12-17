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
Implements Avro representation of a Reservation State
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class ReservationStateAvro:
    """
    Implements Avro representation of a Reservation State
    """
    def __init__(self):
        self.name = self.__class__.__name__
        self.state = None
        self.pending_state = None

    def get_state(self) -> int:
        """
        Return state
        """
        return self.state

    def set_state(self, value: int):
        """
        Set state
        @param value value
        """
        self.state = value

    def get_pending_state(self) -> int:
        """
        Get Pending State
        """
        return self.pending_state

    def set_pending_state(self, value: int):
        """
        Set pending state
        @param value value
        """
        self.pending_state = value

    def from_dict(self, values: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.name = values.get('name', None)
        self.state = values.get('state', None)
        self.pending_state = values.get('pending_state', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        return {
            'name':self.name,
            'state':self.state,
            'pending_state': self.pending_state}

    def __str__(self):
        return "state: {} pending_state: {}".format(self.state, self.pending_state)

    def __eq__(self, other):
        if not isinstance(other, ReservationStateAvro):
            return False

        return self.state == other.state and self.pending_state == other.pending_state

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.state is None or self.pending_state is None:
            ret_val = False
        return ret_val
