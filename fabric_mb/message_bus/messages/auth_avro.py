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
    __slots__ = ["name", "guid", "oidc_sub_claim", "email"]

    def __init__(self):
        self.name = None
        self.guid = None
        self.oidc_sub_claim = None
        self.email = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.name = value.get('name', None)
        self.guid = value.get('guid', None)
        self.oidc_sub_claim = value.get('oidc_sub_claim', None)
        self.email = value.get('email', None)

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

        if self.oidc_sub_claim is not None:
            result['oidc_sub_claim'] = self.oidc_sub_claim

        if self.email is not None:
            result['email'] = self.email
        return result

    def __str__(self):
        return f"name: {self.name} guid: {self.guid} oidc_sub_claim: {self.oidc_sub_claim} email: {self.email}"

    def __eq__(self, other):
        if not isinstance(other, AuthAvro):
            return False
        return self.name == other.name and self.guid == other.guid and self.oidc_sub_claim == other.oidc_sub_claim and \
               self.email == self.email

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        if self.name is None:
            return False
        return True

    def get_name(self) -> str:
        return self.name

    def get_guid(self) -> str:
        return self.guid

    def get_oidc_sub_claim(self) -> str:
        return self.oidc_sub_claim

    def get_email(self) -> str:
        return self.email