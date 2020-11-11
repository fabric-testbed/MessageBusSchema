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
Implements Avro representation of a Get Actors Message
"""
from uuid import uuid4

from fabric.message_bus.messages.auth_avro import AuthAvro
from fabric.message_bus.messages.message import IMessageAvro


class GetActorsAvro(IMessageAvro):
    """
    Implements Avro representation of a Get Actors Message
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "guid", "auth", "callback_topic", "type", "id_token", "id"]

    def __init__(self):
        self.name = IMessageAvro.GetActorsRequest
        self.message_id = None
        self.guid = None
        self.auth = None
        self.callback_topic = None
        self.type = None
        self.id_token = None
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value['name'] != IMessageAvro.GetActorsRequest:
            raise Exception("Invalid message")
        self.message_id = value['message_id']
        self.guid = value['guid']
        self.callback_topic = value['callback_topic']
        self.type = value.get("type", None)
        self.id_token = value.get('id_token', None)

        if value.get('auth', None) is not None:
            self.auth = AuthAvro()
            self.auth.from_dict(value['auth'])

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise Exception("Invalid arguments")
        result = {
            "name": self.name,
            "message_id": self.message_id,
            "guid": self.guid,
            "callback_topic": self.callback_topic,
            "id_token": self.id_token
        }
        if self.type is not None:
            result['type'] = type

        if self.auth is not None:
            result['auth'] = self.auth.to_dict()

        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        return self.name

    def get_callback_topic(self) -> str:
        return self.callback_topic

    def get_id(self) -> str:
        return self.id.__str__()

    def get_id_token(self) -> str:
        """
        Return identity token
        """
        return self.id_token

    def __str__(self):
        return "name: {} message_id: {} guid: {} auth: {} callback_topic: {} type: {} id_token: {}".format(
            self.name, self.message_id, self.guid, self.auth, self.callback_topic, self.type, self.id_token)

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()

        if self.guid is None or self.auth is None or self.callback_topic is None:
            ret_val = False

        return ret_val
