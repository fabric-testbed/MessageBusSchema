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
import pickle

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.ticket import Ticket
from fabric_mb.message_bus.messages.unit_avro import UnitAvro


class ResourceSetAvro:
    """
    Implements Avro representation of a Reservation Resource Set
    """
    def __init__(self):
        self.units = None
        self.type = None
        self.sliver = None
        self.ticket = None
        self.unit_set = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.units = value['units']
        self.type = value['type']
        self.sliver = value.get('sliver', None)
        temp = value.get('ticket', None)
        if temp is not None:
            self.ticket = Ticket()
            self.ticket.from_dict(value=temp)

        temp = value.get('unit_set', None)
        if temp is not None:
            self.unit_set = []
            for t in temp:
                u = UnitAvro()
                u.from_dict(value=t)
                self.unit_set.append(u)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "units": self.units,
            "type": self.type
        }
        if self.sliver is not None:
            result['sliver'] = self.sliver
        if self.ticket is not None:
            result['ticket'] = self.ticket.to_dict()
        if self.unit_set is not None:
            value = []
            for u in self.unit_set:
                value.append(u.to_dict())
            result['unit_set'] = value
        return result

    def __str__(self):
        return f"units: {self.units} type: {self.type} sliver: {self.sliver} ticket: [{self.ticket}] " \
               f"unit_set: [{self.unit_set}]"

    def __eq__(self, other):
        if not isinstance(other, ResourceSetAvro):
            return False

        ret_val = self.units == other.units and self.type == other.type and \
                  self.sliver == other.sliver and self.ticket == other.ticket

        if ret_val and self.unit_set is not None and other.unit_set is not None and \
                len(self.unit_set) == len(other.unit_set):
            count = 0
            length = len(self.unit_set)
            for i in range(length):
                if self.unit_set[i] == other.unit_set[i]:
                    count += 1
            if length == count:
                ret_val = True

        return ret_val

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.units is None or self.type is None:
            ret_val = False
        return ret_val

    def set_sliver(self, sliver):
        if sliver is not None:
            self.sliver = pickle.dumps(sliver)

    def get_sliver(self):
        if self.sliver is not None:
            return pickle.loads(self.sliver)
        return self.sliver
