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
from __future__ import annotations

from typing import List

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.result_avro import ResultAvro


class ResultStringsAvro(AbcMessageAvro):
    """
    Implements Avro representation of a Result Message containing Strings
    """

    def __init__(self):
        super(ResultStringsAvro, self).__init__()
        self.name = AbcMessageAvro.result_strings
        self.status = None
        self.result = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value['name'] != AbcMessageAvro.result_strings:
            raise MessageBusException("Invalid message")
        self.message_id = value['message_id']
        self.status = ResultAvro()
        self.status.from_dict(value['status'])
        self.result = value.get('result', None)

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
        if self.result is not None:
            result["result"] = self.result
        return result

    def __str__(self):
        return "name: {} message_id: {} status: {} result: {}".format(
            self.name, self.message_id, self.status, self.result)

    def get_status(self) -> ResultAvro:
        """
        Returns the status
        """
        return self.status

    def set_status(self, value: ResultAvro):
        """
        Set status
        @param value value
        """
        self.status = value

    def get_result(self) -> List[str]:
        """
        Returns the result
        """
        return self.result

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()
        if self.status is None:
            ret_val = False
        return ret_val
