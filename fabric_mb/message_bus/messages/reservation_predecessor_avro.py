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
Implements Avro representation of a Reservation Predecessor
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class ReservationPredecessorAvro:
    """
    Implements Avro representation of a Reservation Predecessor
    """
    def __init__(self):
        self.reservation_id = None
        self.filter = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.reservation_id = value.get('reservation_id', None)
        self.filter = value.get('filter', None)

    def to_dict(self) -> dict:
        """
                The Avro Python library does not support code generation.
                For this reason we must provide a dict representation of our class for serialization.
                :return dict representing the class
                """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {'reservation_id': self.reservation_id,
                  'filter': self.filter}
        return result

    def __str__(self):
        return "reservation_id: {} filter: {}".format(self.reservation_id, self.filter)

    def get_reservation_id(self) -> str:
        """
        Return reservation id
        @return reservation id
        """
        return self.reservation_id

    def set_reservation_id(self, value: str):
        """
        Set reservation id
        @param value value
        """
        self.reservation_id = value

    def get_filter_properties(self) -> dict:
        """
        Get filter properties
        @return filter
        """
        return self.filter

    def set_filter_properties(self, value: dict):
        """
        Set filter properties
        @param value value
        """
        self.filter = value

    def __eq__(self, other):
        if not isinstance(other, ReservationPredecessorAvro):
            return False

        return self.reservation_id == other.reservation_id and self.filter == other.filter

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.reservation_id is None or self.filter is None:
            ret_val = False
        return ret_val
