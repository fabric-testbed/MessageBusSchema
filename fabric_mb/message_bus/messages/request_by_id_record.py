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
Implements Avro representation of a Get Request Message
"""
from uuid import uuid4

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.message import IMessageAvro


class RequestByIdRecord(IMessageAvro):
    """
    Implements Avro representation of a Get Request Message
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "callback_topic", "message_id", "guid", "slice_id", "reservation_id", "delegation_id", "type",
                 "unit_id", "broker_id", "reservation_state", "delegation_state", "auth", "id_token", "id"]

    def __init__(self):
        self.name = None
        self.callback_topic = None
        self.message_id = None
        self.guid = None
        self.slice_id = None
        self.reservation_id = None
        self.delegation_id = None
        self.type = None
        self.unit_id = None
        self.broker_id = None
        self.reservation_state = None
        self.delegation_state = None
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
        self.callback_topic = value.get('callback_topic', None)
        self.message_id = value.get('message_id', None)
        self.guid = value.get('guid', None)
        self.slice_id = value.get('slice_id', None)
        self.reservation_id = value.get('reservation_id', None)
        self.delegation_id = value.get('delegation_id', None)
        self.type = value.get('type', None)
        self.unit_id = value.get('unit_id', None)
        self.broker_id = value.get('broker_id', None)
        self.reservation_state = value.get('reservation_state', None)
        self.delegation_state = value.get('delegation_state', None)
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
            raise MessageBusException("Invalid arguments")
        result = {
            "name": self.name,
            "callback_topic": self.callback_topic,
            "message_id": self.message_id,
            "guid": self.guid
        }
        if self.slice_id is not None:
            result['slice_id'] = self.slice_id
        if self.reservation_id is not None:
            result['reservation_id'] = self.reservation_id
        if self.delegation_id is not None:
            result['delegation_id'] = self.delegation_id
        if self.type is not None:
            result['type'] = self.type
        if self.unit_id is not None:
            result['unit_id'] = self.unit_id
        if self.broker_id is not None:
            result['broker_id'] = self.broker_id
        if self.reservation_state is not None:
            result['reservation_state'] = self.reservation_state
        if self.delegation_state is not None:
            result['delegation_state'] = self.delegation_state
        if self.id_token is not None:
            result['id_token'] = self.id_token

        if self.auth is not None:
            result['auth'] = self.auth.to_dict()

        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        """
        Returns the message name
        """
        return self.name

    def get_callback_topic(self) -> str:
        """
        Returns the callback topic
        """
        return self.callback_topic

    def get_id(self) -> str:
        """
        Returns the id
        """
        return self.id.__str__()

    def get_slice_id(self) -> str:
        """
        Returns the slice_id
        """
        return self.slice_id

    def get_reservation_id(self) -> str:
        """
        Returns the reservation if
        """
        return self.reservation_id

    def get_delegation_id(self) -> str:
        """
        Returns the delegation id
        """
        return self.delegation_id

    def get_type(self) -> str:
        """
        Returns the type
        """
        return self.type

    def get_unit_id(self) -> str:
        """
        Returns the unit_id
        """
        return self.unit_id

    def get_broker_id(self) -> str:
        """
        Returns the broker_id
        """
        return self.broker_id

    def get_reservation_state(self) -> int:
        """
        Returns the reservation_state
        """
        return self.reservation_state

    def get_delegation_state(self) -> int:
        """
        Returns the delegation_state
        """
        return self.delegation_state

    def get_id_token(self) -> str:
        """
        Returns the id token
        """
        return self.id_token

    def __str__(self):
        return "name: {} callback_topic: {} message_id: {} guid: {} slice_id: {} reservation_id: {} " \
               "delegation_id: {} " \
               "type: {} unit_id:{} broker_id: {} reservation_state: {} delegation_state: {} auth: {} id_token: {}".\
            format(self.name, self.callback_topic, self.message_id, self.guid, self.slice_id, self.reservation_id,
                   self.delegation_id, self.type, self.unit_id, self.broker_id, self.reservation_state,
                   self.delegation_state, self.auth, self.id_token)

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()

        if self.guid is None or self.auth is None or self.callback_topic is None:
            ret_val = False

        return ret_val
