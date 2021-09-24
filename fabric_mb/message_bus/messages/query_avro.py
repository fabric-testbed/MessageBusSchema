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
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro


class QueryAvro(AbcMessageAvro):
    """
    Implements Avro representation of a query Message
    """
    def __init__(self):
        super(QueryAvro, self).__init__()
        self.name = AbcMessageAvro.query
        self.properties = None
        self.auth = None
        self.id_token = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value['name'] != AbcMessageAvro.query:
            raise MessageBusException("Invalid message")
        self.message_id = value['message_id']
        self.callback_topic = value['callback_topic']
        self.properties = value['properties']
        self.id_token = value.get("id_token", None)
        auth_temp = value.get('auth', None)
        if auth_temp is not None:
            self.auth = AuthAvro()
            self.auth.from_dict(value['auth'])

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
            "message_id": self.message_id,
            "callback_topic": self.callback_topic,
            "properties": self.properties,
            "id_token": self.id_token
        }
        if self.auth is not None:
            result['auth'] = self.auth.to_dict()
        return result

    def get_id_token(self) -> str:
        """
        Return id token
        """
        return self.id_token

    def __str__(self):
        return "name: {} message_id: {} callback_topic: {} properties: {} id_token: {}".format(
            self.name, self.message_id, self.callback_topic, self.properties, self.id_token)

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()
        if self.auth is None or self.callback_topic is None or self.properties is None:
            ret_val = False
        return ret_val
