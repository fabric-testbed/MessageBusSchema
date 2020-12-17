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
Implements Avro representation of a Proxy
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class ProxyAvro:
    """
    Implements Avro representation of a Proxy
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["protocol", "name", "guid", "type", "kafka_topic"]

    def __init__(self):
        self.protocol = None
        self.name = None
        self.guid = None
        self.type = None
        self.kafka_topic = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.protocol = value.get('protocol', None)
        self.name = value.get('name', None)
        self.guid = value.get('guid', None)
        self.type = value.get('type', None)
        self.kafka_topic = value.get('kafka_topic', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "protocol": self.protocol,
            "name": self.name,
            "guid": self.guid,
            "type": self.type,
            "kafka_topic": self.kafka_topic
        }
        return result

    def set_name(self, name: str):
        """
        Set name
        @param name name
        """
        self.name = name

    def set_guid(self, guid: str):
        """
        Set guid
        @param guid guid
        """
        self.guid = guid

    def get_name(self) -> str:
        """
        Return name
        """
        return self.name

    def get_guid(self) -> str:
        """
        Return guid
        """
        return self.guid

    def set_protocol(self, protocol: str):
        """
        Set protocol
        @param protocol protocol
        """
        self.protocol = protocol

    def set_type(self, type: str):
        """
        Set type
        @param type type
        """
        self.type = type

    def get_protocol(self) -> str:
        """
        Get Protocol
        """
        return self.protocol

    def get_type(self) -> str:
        """
        Get type
        """
        return self.type

    def set_kafka_topic(self, kafka_topic: str):
        """
        Set kafka topic
        @param kafka_topic kafka topic
        """
        self.kafka_topic = kafka_topic

    def get_kafka_topic(self) -> str:
        """
        Return kafka topic
        """
        return self.kafka_topic

    def __eq__(self, other):
        if not isinstance(other, ProxyAvro):
            return False

        return self.name == other.name and self.type == other.type and self.guid == other.guid and \
               self.kafka_topic == other.kafka_topic and self.protocol == other.protocol

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.name is None or self.type is None or self.guid is None or self.protocol is None:
            ret_val = False
        return ret_val
