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
Implements Avro representation of an Auth Token
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class AuthAvro:
    """
    Implements Avro representation of an Auth Token
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "guid", "id_token"]

    def __init__(self):
        self.name = None
        self.guid = None
        self.id_token = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.name = value['name']
        if 'guid' in value and value['guid'] != "null":
            self.guid = value['guid']
        if 'id_token' in value and value['id_token'] != "null":
            self.id_token = value['id_token']

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "name": self.name
        }
        if self.guid is not None:
            result['guid'] = self.guid

        if self.id_token is not None:
            result['id_token'] = self.id_token
        return result

    def __str__(self):
        return "name: {} guid: {} id_token: {}".format(self.name, self.guid, self.id_token)

    def __eq__(self, other):
        if not isinstance(other, AuthAvro):
            return False
        return self.name == other.name and self.guid == other.guid and self.id_token == other.id_token

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        if self.name is None:
            return False
        return True
