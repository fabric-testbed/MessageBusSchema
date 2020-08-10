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

from fabric.message_bus.messages.lease_reservation_avro import LeaseReservationAvro
from fabric.message_bus.messages.reservation_mng import ReservationMng
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.ticket_reservation_avro import TicketReservationAvro
from fabric.message_bus.messages.message import IMessageAvro


class ResultReservationAvro(IMessageAvro):
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "status", "reservations", "id"]

    def __init__(self):
        self.name = IMessageAvro.ResultReservation
        self.message_id = None
        self.status = None
        self.reservations = None
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def from_dict(self, value: dict):
        if value['name'] != IMessageAvro.ResultReservation:
            raise Exception("Invalid message")
        self.message_id = value['message_id']
        self.status = ResultAvro()
        self.status.from_dict(value['status'])
        reservations_list = value.get('reservations', None)
        if reservations_list is not None:
            for s in reservations_list:
                res_obj = None
                if s.get('name') == LeaseReservationAvro.__class__.__name__:
                    res_obj = LeaseReservationAvro()
                elif s.get('name') == TicketReservationAvro.__class__.__name__:
                    res_obj = TicketReservationAvro()
                else:
                    res_obj = ReservationMng()
                res_obj.from_dict(s)
                if self.reservations is None:
                    self.reservations = []
                self.reservations.append(res_obj)

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
        if self.reservations is not None:
            temp = []
            for s in self.reservations:
                temp.append(s.to_dict())
            result["reservations"] = temp
        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        return self.name

    def __str__(self):
        return "name: {} message_id: {} status: {} reservations: {}".format(self.name, self.message_id, self.status, self.reservations)

    def get_id(self) -> str:
        return self.id.__str__()

    def set_status(self, status: ResultAvro):
        self.status = status

    def get_callback_topic(self) -> str:
        return None

    def validate(self) -> bool:
        ret_val = super().validate()
        if self.status is None:
            ret_val = False
        return ret_val