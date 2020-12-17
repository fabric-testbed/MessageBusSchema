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
Implements Avro representation of an Add Reservations Message
"""
from typing import List
from uuid import uuid4

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro

from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.message import IMessageAvro


class AddReservationsAvro(IMessageAvro):
    """
    Implements Avro representation of an Add Reservations Message
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "guid", "auth", "reservation_list", "callback_topic", "id_token", "id"]

    def __init__(self):
        self.name = IMessageAvro.add_reservations
        self.message_id = None
        self.guid = None
        self.auth = None
        self.reservation_list = None
        self.callback_topic = None
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
        if value['name'] != IMessageAvro.add_reservations:
            raise MessageBusException("Invalid message")
        self.message_id = value.get('message_id', None)
        self.guid = value.get('guid', None)
        self.callback_topic = value.get('callback_topic', None)
        self.id_token = value.get('id_token', None)

        if value.get("reservation_list", None) is not None:
            res_list = value.get("reservation_list", None)
            self.reservation_list = []
            for r in res_list:
                res = TicketReservationAvro()
                res.from_dict(r)
                self.reservation_list.append(res)

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
            "message_id": self.message_id,
            "guid": self.guid,
            "callback_topic": self.callback_topic
        }
        if self.id_token is not None:
            result['id_token'] = self.id_token

        if self.auth is not None:
            result['auth'] = self.auth.to_dict()

        if self.reservation_list is not None:
            temp = []
            for r in self.reservation_list:
                temp.append(r.to_dict())
            result['reservation_list'] = temp

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
        Return id token
        """
        return self.id_token

    def get_reservation(self) -> List[TicketReservationAvro]:
        """
        Returns reservation list
        @return reservation list
        """
        return self.reservation_list

    def __str__(self):
        return "name: {} message_id: {} guid: {} auth: {} reservation_list: {} callback_topic: {} id_token: {}".format(
            self.name, self.message_id, self.guid, self.auth, self.reservation_list, self.callback_topic, self.id_token)

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if not super().validate() or self.guid is None or self.auth is None or self.callback_topic is None or \
                self.reservation_list is None:
            ret_val = False
        return ret_val
