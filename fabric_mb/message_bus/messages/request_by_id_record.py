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
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro


class RequestByIdRecord(AbcMessageAvro):
    """
    Implements Avro representation of a Get Request Message
    """

    def __init__(self):
        super(RequestByIdRecord, self).__init__()
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
        self.slice_name = None
        self.level = None
        self.email = None
        self.graph_format = None
        self.site = None

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

    def get_slice_name(self) -> str:
        return self.slice_name

    def get_level(self) -> int:
        return self.level

    def set_level(self, value):
        self.level = value

    def set_email(self, email: str):
        self.email = email

    def get_email(self) -> str:
        return self.email

    def get_graph_format(self) -> int:
        return self.graph_format

    def set_graph_format(self, graph_format):
        self.graph_format = graph_format

    def set_site(self, site: str):
        self.site = site

    def get_site(self) -> str:
        return self.site

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()

        if self.guid is None or self.auth is None or self.callback_topic is None:
            ret_val = False

        return ret_val
