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
Implements Avro representation of Messages containing Reservation or Delegation
"""
from uuid import uuid4

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro
from fabric_mb.message_bus.messages.reservation_avro import ReservationAvro
from fabric_mb.message_bus.messages.message import IMessageAvro
from fabric_mb.message_bus.messages.update_data_avro import UpdateDataAvro


class ReservationOrDelegationRecord(IMessageAvro):
    """
    Implements Avro representation of Messages containing Reservation or Delegation
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "callback_topic", "update_data", "reservation", "delegation", "auth",
                 "id_token", "id"]

    def __init__(self):
        self.name = None
        self.message_id = None
        self.callback_topic = None
        self.update_data = None
        self.reservation = None
        self.delegation = None
        self.auth = None
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
        self.message_id = value.get('message_id', None)
        self.callback_topic = value.get('callback_topic', None)
        udd = value.get('update_data', None)
        if udd is not None:
            self.update_data = UpdateDataAvro()
            self.update_data.from_dict(udd)
        res_dict = value.get('reservation', None)
        if res_dict is not None:
            self.reservation = ReservationAvro()
            self.reservation.from_dict(res_dict)
        del_dict = value.get('delegation', None)
        if del_dict is not None:
            self.delegation = DelegationAvro()
            self.delegation.from_dict(del_dict)
        auth_temp = value.get('auth', None)
        if auth_temp is not None:
            self.auth = AuthAvro()
            self.auth.from_dict(value['auth'])
        self.id_token = value.get('id_token', None)

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
            "callback_topic": self.callback_topic
        }
        if self.update_data is not None:
            result['update_data'] = self.update_data.to_dict()
        if self.reservation is not None:
            result['reservation'] = self.reservation.to_dict()
        if self.delegation is not None:
            result['delegation'] = self.delegation.to_dict()
        if self.auth is not None:
            result['auth'] = self.auth.to_dict()
        if self.id_token is not None:
            result['id_token'] = self.id_token
        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        return self.name

    def __str__(self):
        self.name = None
        self.message_id = None
        self.callback_topic = None
        self.update_data = None
        self.reservation = None
        self.delegation = None
        self.auth = None
        self.id_token = None

        return "name: {} message_id: {} callback_topic: {} update_data: {} reservation: {} " \
               "delegation: {} auth: {} id_token: {}".\
            format(self.name, self.message_id, self.callback_topic, self.update_data, self.reservation,
                   self.delegation, self.auth, self.id_token)

    def get_id(self) -> str:
        return self.id.__str__()

    def get_callback_topic(self) -> str:
        return self.callback_topic
