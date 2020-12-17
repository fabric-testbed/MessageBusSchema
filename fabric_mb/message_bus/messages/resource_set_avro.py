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
Implements Avro representation of a Reservation Resource Set
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.resource_data_avro import ResourceDataAvro


class ResourceSetAvro:
    """
    Implements Avro representation of a Reservation Resource Set
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["units", "type", "resource_data", "concrete"]

    def __init__(self):
        self.units = None
        self.type = None
        self.resource_data = None
        self.concrete = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.units = value['units']
        self.type = value['type']
        if value.get('resource_data', None) is not None:
            self.resource_data = ResourceDataAvro()
            self.resource_data.from_dict(value['resource_data'])
        if value.get('concrete', None) is not None:
            self.concrete = value['concrete']

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
        if self.resource_data is not None:
            result['resource_data'] = self.resource_data.to_dict()
        if self.concrete is not None:
            result['concrete'] = self.concrete
        return result

    def __str__(self):
        return "units: {} type: {} resource_data: {} concrete: {}"\
            .format(self.units, self.type, self.resource_data, self.concrete)

    def __eq__(self, other):
        if not isinstance(other, ResourceSetAvro):
            return False

        return self.units == other.units and self.type == other.type and \
               self.resource_data == other.resource_data and self.concrete == other.concrete

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.units is None or self.type is None:
            ret_val = False
        return ret_val
