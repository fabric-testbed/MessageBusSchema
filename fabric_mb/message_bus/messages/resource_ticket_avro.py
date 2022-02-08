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
from fabric_mb.message_bus.messages.abc_object_avro import AbcObjectAvro
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.term_avro import TermAvro


class ResourceTicketAvro(AbcObjectAvro):
    def __init__(self):
        self.guid = None
        self.term = None
        self.units = 0
        self.properties = None
        self.type = None
        self.issuer = None
        self.holder = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.TERM:
                    self.__dict__[k] = TermAvro()
                    self.__dict__[k].from_dict(value=v)
                else:
                    self.__dict__[k] = v

    def get_guid(self) -> str:
        return self.guid

    def get_units(self) -> int:
        return self.units

    def get_term(self) -> TermAvro:
        return self.term

    def get_properties(self) -> dict:
        return self.properties

    def get_type(self) -> str:
        return self.type

    def get_issuer(self) -> str:
        return self.issuer

    def get_holder(self) -> str:
        return self.holder

    def set_guid(self, guid: str):
        self.guid = guid

    def set_units(self, units: int):
        self.units = units

    def set_term(self, term: TermAvro):
        self.term = term

    def set_properties(self, properties: dict):
        self.properties = properties

    def set_type(self, rtype: str):
        self.type = rtype

    def set_issuer(self, issuer: str):
        self.issuer = issuer

    def set_holder(self, holder: str):
        self.holder = holder

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.guid is None or self.units is None or self.type is None:
            ret_val = False
        return ret_val
