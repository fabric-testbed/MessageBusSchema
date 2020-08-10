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


class ProxyAvro:
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["protocol", "name", "guid", "type", "kafka_topic"]

    def __init__(self):
        self.protocol = None
        self.name = None
        self.guid = None
        self.type = None
        self.kafka_topic = None

    def from_dict(self, value: dict):
        self.protocol = value.get('protocol', None)
        self.name = value.get('name', None)
        self.guid = value.get('guid', None)
        self.type = value.get('type', None)
        self.kafka_topic = value.get('kafka_topic', None)

    def to_dict(self) -> dict:
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        if not self.validate():
            raise Exception("Invalid arguments")

        result = {
            "protocol": self.protocol,
            "name": self.name,
            "guid": self.guid,
            "type": self.type,
            "kafka_topic": self.kafka_topic
        }
        return result

    def set_name(self, name: str):
        self.name = name

    def set_guid(self, guid: str):
        self.guid = guid

    def get_name(self) -> str:
        return self.name

    def get_guid(self) -> str:
        return self.guid

    def set_protocol(self, protocol: str):
        self.protocol = protocol

    def set_type(self, type: str):
        self.type = type

    def get_protocol(self) -> str:
        return self.protocol

    def get_type(self) -> str:
        return self.type

    def set_kafka_topic(self, kafka_topic: str):
        self.kafka_topic = kafka_topic

    def get_kafka_topic(self) -> str:
        return self.kafka_topic

    def __eq__(self, other):
        if not isinstance(other, ProxyAvro):
            return False

        return self.name == other.name and self.type == other.type and self.guid == other.guid and \
               self.kafka_topic == other.kafka_topic and self.protocol == other.protocol

    def validate(self) -> bool:
        ret_val = True
        if self.name is None or self.type is None or self.guid is None or self.protocol is None:
            ret_val = False
        return ret_val