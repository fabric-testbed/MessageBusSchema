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
from typing import List

from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.ticket_reservation_avro import TicketReservationAvro

from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro


class AddReservationsAvro(AbcMessageAvro):
    """
    Implements Avro representation of an Add Reservations Message
    """

    def __init__(self):
        super(AddReservationsAvro, self).__init__()
        self.name = AbcMessageAvro.add_reservations
        self.guid = None
        self.auth = None
        self.reservation_list = None
        self.id_token = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value[Constants.NAME] != AbcMessageAvro.add_reservations:
            raise MessageBusException(Constants.ERROR_INVALID_MESSAGE)
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.RESERVATION_LIST:
                    self.reservation_list = []
                    for r in v:
                        res = TicketReservationAvro()
                        res.from_dict(value=r)
                        self.reservation_list.append(res)
                elif k == Constants.AUTH:
                    self.auth = AuthAvro()
                    self.auth.from_dict(value=v)
                else:
                    self.__dict__[k] = v

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

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if not super().validate() or self.guid is None or self.auth is None or \
                self.callback_topic is None or self.reservation_list is None:
            ret_val = False
        return ret_val
