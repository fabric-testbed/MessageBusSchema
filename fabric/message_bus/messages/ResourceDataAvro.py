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


class ResourceDataAvro:
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["request_properties", "config_properties", "resource_properties"]

    def __init__(self):
        self.request_properties = None
        self.config_properties = None
        self.resource_properties = None

    def from_dict(self, value: dict):
        if 'request_properties' in value and value['request_properties'] != "null":
            self.request_properties = value['request_properties']

        if 'config_properties' in value and value['config_properties'] != "null":
            self.config_properties = value['config_properties']

        if 'resource_properties' in value and value['resource_properties'] != "null":
            self.resource_properties = value['resource_properties']

    def to_dict(self) -> dict:
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
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
