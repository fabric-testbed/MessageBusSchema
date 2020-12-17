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
Implements Avro representation of a Reservation
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.resource_set_avro import ResourceSetAvro
from fabric_mb.message_bus.messages.slice_avro import SliceAvro
from fabric_mb.message_bus.messages.term_avro import TermAvro


class ReservationAvro:
    """
    Implements Avro representation of a Reservation
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["reservation_id", "slice", "resource_set", "term", "sequence"]

    def __init__(self):
        self.reservation_id = None
        self.slice = None
        self.resource_set = None
        self.term = None
        self.sequence = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.reservation_id = value['reservation_id']
        self.sequence = value.get('sequence', None)
        self.slice = SliceAvro()
        self.slice.from_dict(value['slice'])
        self.term = TermAvro()
        self.term.from_dict(value['term'])
        if 'resource_set' in value and value['resource_set'] != "null":
            self.resource_set = ResourceSetAvro()
            self.resource_set.from_dict(value['resource_set'])

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "reservation_id": self.reservation_id,
            "slice": self.slice.to_dict(),
            "term": self.term.to_dict(),
        }
        if self.sequence is not None:
            result['sequence'] = self.sequence
        if self.resource_set is not None:
            result['resource_set'] = self.resource_set.to_dict()
        return result

    def __str__(self):
        return "reservation_id: {} slice: {} term: {} sequence: {} resource_set: {}".format(self.reservation_id,
                                                                                            self.slice,
                                                                                            self.term,
                                                                                            self.sequence,
                                                                                            self.resource_set)

    def __eq__(self, other):
        if not isinstance(other, ReservationAvro):
            return False
        return self.reservation_id == other.reservation_id and self.slice == other.slice and \
               self.term == other.term and self.resource_set == other.resource_set

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.reservation_id is None or self.slice is None or self.term is None:
            ret_val = False
        return ret_val
