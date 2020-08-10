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


class PoolInfoAvro:
    def __init__(self):
        self.name = None
        self.type = None
        self.properties = None

    def from_dict(self, value: dict):
        self.name = value.get('name', None)
        self.type = value.get('type', None)
        self.properties = value.get('properties', None)

    def to_dict(self) -> dict:
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        if not self.validate():
            raise Exception("Invalid arguments")
        
        result = {
            "name": self.name,
            "type": self.type,
            "properties": self.properties
        }
        return result

    def __str__(self):
        return "name: {} type: {} properties: {}".format(self.name, self.type, self.properties)

    def get_properties(self) -> dict:
        return self.properties

    def set_properties(self, value: dict):
        self.properties = value

    def set_name(self, name: str):
        self.name = name

    def get_name(self) -> str:
        return self.name

    def set_type(self, type: str):
        self.type = type

    def get_type(self) -> str:
        return self.type

    def __eq__(self, other):
        if not isinstance(other, PoolInfoAvro):
            return False

        return self.name == other.name and self.type == other.type and self.properties == other.properties

    def validate(self) -> bool:
        ret_val = True
        if self.name is None or self.type is None or self.properties is None:
            ret_val = False
        return ret_val