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
Implements Avro representation of a Reservation Resource Data
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class ResourceDataAvro:
    """
    Implements Avro representation of a Reservation Resource Data
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["request_properties", "config_properties", "resource_properties"]

    def __init__(self):
        self.request_properties = None
        self.config_properties = None
        self.resource_properties = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.request_properties = value.get('request_properties', None)
        self.config_properties = value.get('config_properties', None)
        self.resource_properties = value.get('resource_properties', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {}
        if self.config_properties is not None and len(self.config_properties) > 0:
            result['config_properties'] = self.config_properties

        if self.request_properties is not None and len(self.request_properties) > 0:
            result['request_properties'] = self.request_properties

        if self.resource_properties is not None and len(self.resource_properties) > 0:
            result['resource_properties'] = self.resource_properties

        return result

    def __str__(self):
        return "request_properties: {} config_properties: {} resource_properties: {}"\
            .format(self.request_properties, self.config_properties, self.resource_properties)

    def __eq__(self, other):
        if not isinstance(other, ResourceDataAvro):
            return False

        return self.request_properties == other.request_properties and \
               self.config_properties == other.config_properties and \
               self.resource_properties == other.resource_properties

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        return ret_val
