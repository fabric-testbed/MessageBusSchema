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
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.reservation_mng import ReservationMng
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro

from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro


class AddUpdateReservationRecord(AbcMessageAvro):
    """
    Implements Avro representation of an UpdateAddReservation Message
    """

    def __init__(self):
        super(AddUpdateReservationRecord, self).__init__()
        self.guid = None
        self.auth = None
        self.reservation_obj = None
        self.reservation_id = None
        self.id_token = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.RESERVATION_OBJ:
                    class_name = v.get(Constants.NAME, None)
                    if class_name == TicketReservationAvro.__name__:
                        self.reservation_obj = TicketReservationAvro()
                        self.reservation_obj.from_dict(value=v)
                    elif class_name == ReservationMng.__name__:
                        self.reservation_obj = ReservationMng()
                        self.reservation_obj.from_dict(value=v)
                elif k == Constants.AUTH:
                    self.auth = AuthAvro()
                    self.auth.from_dict(value=v)
                else:
                    self.__dict__[k] = v

    def get_reservation(self) -> ReservationMng:
        """
        Returns the reservation object
        @return reservation
        """
        return self.reservation_obj

    def get_reservation_id(self) -> str:
        """
        Return reservation id
        """
        return self.reservation_id

    def get_id_token(self) -> str:
        """
        Return id token
        """
        return self.id_token

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if not super().validate() or self.guid is None or self.auth is None or \
                self.callback_topic is None or self.reservation_obj is None:
            ret_val = False
        return ret_val
