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


class TermAvro:
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["start_time", "end_time", "ticket_time", "new_start_time"]

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.ticket_time = None
        self.new_start_time = None

    def from_dict(self, value: dict):
        if 'start_time' in value and value['start_time'] != "null":
            self.start_time = value['start_time']

        if 'end_time' in value and value['end_time'] != "null":
            self.end_time = value['end_time']

        if 'ticket_time' in value and value['ticket_time'] != "null":
            self.ticket_time = value['ticket_time']

        if 'new_start_time' in value and value['new_start_time'] != "null":
            self.new_start_time = value['new_start_time']

    def to_dict(self) -> dict:
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        result = {
            "start_time": self.start_time,
            "end_time": self.end_time
        }
        if self.ticket_time is not None:
            result["ticket_time"] = self.ticket_time

        if self.new_start_time is not None:
            result["new_start_time"] =  self.new_start_time
        return result

    def __str__(self):
        return "start_time: {} end_time: {} ticket_time: {} new_start_time: {}"\
            .format(self.start_time, self.end_time, self.ticket_time, self.new_start_time)

    def __eq__(self, other):
        if not isinstance(other, TermAvro):
            return False

        return self.start_time == other.start_time and self.ticket_time == other.ticket_time and \
               self.end_time == other.end_time and self.new_start_time == other.new_start_time
