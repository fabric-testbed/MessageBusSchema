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
from uuid import uuid4

from fabric.message_bus.messages.message import IMessageAvro
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.unit_avro import UnitAvro


class ResultUnitAvro(IMessageAvro):
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "units", "callback_topic", "id"]

    def __init__(self):
        self.name = IMessageAvro.ResultUnits
        self.message_id = None
        self.status = None
        self.units = None
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def get_status(self) -> ResultAvro:
        return self.status

    def set_status(self, value: ResultAvro):
        self.status = value

    def get_units(self) -> list:
        return self.units

    def from_dict(self, value: dict):
        if value['name'] != IMessageAvro.ResultUnits:
            raise Exception("Invalid message")
        self.message_id = value['message_id']
        self.status = ResultAvro()
        self.status.from_dict(value['status'])
        temp_units = value.get('units', None)
        if temp_units is not None:
            self.units = []
            for u in temp_units:
                unit = UnitAvro()
                unit.from_dict(u)
                self.units.append(unit)

    def to_dict(self) -> dict:
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        if not self.validate():
            raise Exception("Invalid arguments")

        result = {
            "name": self.name,
            "message_id": self.message_id,
            "status": self.status.to_dict()
        }
        if self.units is not None:
            temp = []
            for u in self.units:
                temp.append(u.to_dict())
            result["units"] = temp
        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        return self.name

    def get_id(self) -> str:
        return self.id.__str__()

    def __str__(self):
        return "name: {} message_id: {} status: {} units: {}".format(self.name, self.message_id, self.status, self.units)

    def get_callback_topic(self) -> str:
        return None

    def validate(self) -> bool:
        ret_val = super().validate()
        if self.status is None:
            ret_val = False
        return ret_val