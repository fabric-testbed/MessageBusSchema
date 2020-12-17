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
Implements Avro representation of a Result Message containing String
"""

from __future__ import annotations
from uuid import uuid4

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.message import IMessageAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro


class ResultStringAvro(IMessageAvro):
    """
    Implements Avro representation of a Result Message containing String
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "result_str", "id"]

    def __init__(self):
        self.name = IMessageAvro.result_string
        self.message_id = None
        self.status = None
        self.result_str = None
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value['name'] != IMessageAvro.result_string:
            raise MessageBusException("Invalid message")
        self.message_id = value['message_id']
        self.status = ResultAvro()
        self.status.from_dict(value['status'])
        self.result_str = value.get('result_str', None)

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
            "status": self.status.to_dict()
        }
        if self.result_str is not None:
            result["result_str"] = self.result_str
        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        return self.name

    def __str__(self):
        return "name: {} message_id: {} status: {} result_str: {}".format(self.name, self.message_id, self.status,
                                                                          self.result_str)

    def get_status(self) -> ResultAvro:
        """
        Return status
        """
        return self.status

    def set_status(self, value: ResultAvro):
        """
        Set status
        @param value value
        """
        self.status = value

    def get_id(self) -> str:
        return self.id.__str__()

    def get_result(self) -> str:
        """
        Return result string
        """
        return self.result_str

    def set_result(self, result: str):
        """
        Set result string
        """
        self.result_str = result

    def get_callback_topic(self) -> str:
        return None

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()
        if self.status is None:
            ret_val = False
        return ret_val
