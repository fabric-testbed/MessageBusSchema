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
Implements Avro representation of a Failed RPC Message
"""
from uuid import uuid4

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.message import IMessageAvro


class FailedRpcAvro(IMessageAvro):
    """
    Implements Avro representation of a Failed RPC Message
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "request_id", "properties", "auth", "id"]

    def __init__(self):
        self.name = IMessageAvro.failed_rpc
        self.message_id = None
        self.request_type = None
        self.request_id = None
        self.reservation_id = None
        self.error_details = None
        self.auth = None
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value['name'] != IMessageAvro.failed_rpc:
            raise MessageBusException("Invalid message")
        self.message_id = value['message_id']
        self.request_type = value['request_type']
        self.request_id = value.get('request_id', None)
        self.reservation_id = value.get('reservation_id', None)
        self.error_details = value["error_details"]
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
            "error_details": self.error_details,
            "request_type": self.request_type
        }
        if self.auth is not None:
            result['auth'] = self.auth.to_dict()
        if self.reservation_id is not None:
            result["reservation_id"] = self.reservation_id
        if self.request_id is not None:
            result["request_id"] = self.request_id
        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        return self.name

    def __str__(self):
        return "name: {} message_id: {} request_id: {} request_type: {} reservation_id:{} error_details:{}"\
            .format(self.name, self.message_id, self.request_id, self.request_type, self.reservation_id,
                    self.error_details)

    def get_id(self) -> str:
        return self.id.__str__()

    def get_callback_topic(self) -> str:
        return None

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()

        if self.auth is None or self.request_id is None or self.request_type is None:
            ret_val = False

        return ret_val
