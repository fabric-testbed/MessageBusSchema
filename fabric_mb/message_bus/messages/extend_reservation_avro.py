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
        if value['name'] != AbcMessageAvro.extend_reservation:
            raise MessageBusException("Invalid message")
        self.guid = value.get('guid', None)
        self.message_id = value.get('message_id', None)
        self.callback_topic = value.get('callback_topic', None)
        auth_temp = value.get('auth', None)
        if auth_temp is not None:
            self.auth = AuthAvro()
            self.auth.from_dict(value['auth'])
        self.reservation_id = value.get('reservation_id', None)
        self.end_time = value.get('end_time', None)
        self.new_units = value.get('new_units', None)
        self.new_resource_type = value.get('new_resource_type', None)
        self.request_properties = value.get('request_properties', None)
        self.config_properties = value.get('config_properties', None)
        self.id_token = value.get("id_token", None)

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
            "auth": self.auth.to_dict(),
            "reservation_id": self.reservation_id,
            "end_time": self.end_time,
            "new_units": self.new_units,
            "callback_topic": self.callback_topic
        }
        if self.id_token is not None:
            result["id_token"] = self.id_token

        if self.new_resource_type is not None:
            result['new_resource_type'] = self.new_resource_type
        if self.request_properties is not None:
            result["request_properties"] = self.request_properties
        if self.config_properties is not None:
            result["config_properties"] = self.config_properties

        return result

    def __str__(self):
        return "name: {} message_id: {} callback_topic: {} reservation_id: {} end_time: {} new_units: {} " \
               "new_resource_type: {} request_properties: {} config_properties: {} id_token: {}".\
            format(self.name, self.message_id, self.callback_topic, self.reservation_id, self.end_time, self.new_units,
                   self.new_resource_type, self.request_properties, self.config_properties, self.id_token)

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
