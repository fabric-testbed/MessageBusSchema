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


class FailedRpcAvro(AbcMessageAvro):
    """
    Implements Avro representation of a Failed RPC Message
    """
    def __init__(self):
        super(FailedRpcAvro, self).__init__()
        self.name = AbcMessageAvro.failed_rpc
        self.request_type = None
        self.request_id = None
        self.reservation_id = None
        self.error_details = None
        self.auth = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value['name'] != AbcMessageAvro.failed_rpc:
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

    def __str__(self):
        return "name: {} message_id: {} request_id: {} request_type: {} reservation_id:{} error_details:{}"\
            .format(self.name, self.message_id, self.request_id, self.request_type, self.reservation_id,
                    self.error_details)

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()

        if self.auth is None or self.request_id is None or self.request_type is None:
            ret_val = False

        return ret_val
