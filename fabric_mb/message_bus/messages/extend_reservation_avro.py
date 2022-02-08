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
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.constants import Constants


class ExtendReservationAvro(AbcMessageAvro):
    """
    Implements Avro representation of an Extend Reservation Message
    """
    def __init__(self):
        super(ExtendReservationAvro, self).__init__()
        self.name = AbcMessageAvro.extend_reservation
        self.guid = None
        self.auth = None
        self.reservation_id = None
        self.end_time = None
        self.new_units = -1
        self.new_resource_type = None
        self.request_properties = None
        self.config_properties = None
        self.id_token = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value[Constants.NAME] != AbcMessageAvro.extend_reservation:
            raise MessageBusException(Constants.ERROR_INVALID_MESSAGE)
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.AUTH:
                    self.__dict__[k] = AuthAvro()
                    self.__dict__[k].from_dict(value=v)
                else:
                    self.__dict__[k] = v

    def get_id_token(self) -> str:
        """
        Return identity token
        """
        return self.id_token

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()

        if self.guid is None or self.auth is None or self.callback_topic is None or self.reservation_id is None or \
                self.new_units is None or self.new_resource_type is None or self.request_properties is None or \
                self.config_properties is None:
            ret_val = False

        return ret_val
