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
Implements Avro representation of a Pool Info
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class PoolInfoAvro:
    """
    Implements Avro representation of a Pool Info
    """
    def __init__(self):
        self.name = None
        self.type = None
        self.properties = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.name = value.get('name', None)
        self.type = value.get('type', None)
        self.properties = value.get('properties', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "name": self.name,
            "type": self.type,
            "properties": self.properties
        }
        return result

    def __str__(self):
        return "name: {} type: {} properties: {}".format(self.name, self.type, self.properties)

    def get_properties(self) -> dict:
        """
        Return properties
        @return properties
        """
        return self.properties

    def set_properties(self, value: dict):
        """
        Set properties
        @param value value
        """
        self.properties = value

    def set_name(self, name: str):
        """
        Set name
        @param name name
        """
        self.name = name

    def get_name(self) -> str:
        """
        Return name
        @return name
        """
        return self.name

    def set_type(self, type: str):
        """
        Set type
        @param type type
        """
        self.type = type

    def get_type(self) -> str:
        """
        Return type
        @return type
        """
        return self.type

    def __eq__(self, other):
        if not isinstance(other, PoolInfoAvro):
            return False

        return self.name == other.name and self.type == other.type and self.properties == other.properties

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.name is None or self.type is None or self.properties is None:
            ret_val = False
        return ret_val
