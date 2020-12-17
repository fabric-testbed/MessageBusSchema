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
Implements Avro representation of a Term
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class TermAvro:
    """
    Implements Avro representation of a Term
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["start_time", "end_time", "ticket_time", "new_start_time"]

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.ticket_time = None
        self.new_start_time = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.start_time = value.get('start_time', None)
        self.end_time = value.get('end_time', None)
        self.ticket_time = value.get('ticket_time', None)
        self.new_start_time = value.get('new_start_time', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "start_time": self.start_time,
            "end_time": self.end_time
        }
        if self.ticket_time is not None:
            result["ticket_time"] = self.ticket_time

        if self.new_start_time is not None:
            result["new_start_time"] = self.new_start_time
        return result

    def __str__(self):
        return "start_time: {} end_time: {} ticket_time: {} new_start_time: {}"\
            .format(self.start_time, self.end_time, self.ticket_time, self.new_start_time)

    def __eq__(self, other):
        if not isinstance(other, TermAvro):
            return False

        return self.start_time == other.start_time and self.ticket_time == other.ticket_time and \
               self.end_time == other.end_time and self.new_start_time == other.new_start_time

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.start_time is None or self.end_time is None or self.new_start_time is None:

            ret_val = False
        return ret_val
