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

from fabric.message_bus.messages.AuthAvro import AuthAvro
from fabric.message_bus.messages.ReservationAvro import ReservationAvro
from fabric.message_bus.messages.message import IMessageAvro


class ModifyLeaseAvro(IMessageAvro):
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "callback_topic", "reservation", "auth", "id"]

    def __init__(self):
        self.name = IMessageAvro.ModifyLease
        self.message_id = None
        self.reservation = None
        self.callback_topic = None
        self.auth = None
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def from_dict(self, value: dict):
        if value['name'] != IMessageAvro.ModifyLease:
            raise Exception("Invalid message")
        self.message_id = value['message_id']
        self.callback_topic = value['callback_topic']
        auth_temp = value.get('auth', None)
        if auth_temp is not None:
            self.auth = AuthAvro()
            self.auth.from_dict(value['auth'])
        res_dict = value.get('reservation', None)
        if res_dict is not None:
            self.reservation = ReservationAvro()
            self.reservation.from_dict(res_dict)

    def to_dict(self) -> dict:
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        result = {
            "name": self.name,
            "message_id": self.message_id,
            "callback_topic": self.callback_topic,
            "reservation": self.reservation.to_dict()
        }
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

    def __str__(self):
        return "name: {} message_id: {} callback_topic: {} reservation: {}".format(self.name, self.message_id, self.callback_topic, self.reservation)

    def get_id(self) -> str:
        return self.id.__str__()