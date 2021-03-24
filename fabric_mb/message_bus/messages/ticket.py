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
from fabric_mb.message_bus.messages.resource_ticket_avro import ResourceTicketAvro


class Ticket:
    def __init__(self):
        self.authority = None
        self.old_units = 0
        self.resource_ticket = None
        self.delegation_id = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        temp = value.get('authority', None)
        if temp is not None:
            self.authority = AuthAvro()
            self.authority.from_dict(value=temp)
        self.old_units = value.get('old_units', 0)
        self.delegation_id = value.get('delegation_id', 0)
        temp = value.get('resource_ticket', None)
        if temp is not None:
            self.resource_ticket = ResourceTicketAvro()
            self.resource_ticket.from_dict(value=temp)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "old_units": self.old_units,
            "delegation_id": self.delegation_id
        }
        if self.authority is not None:
            result["authority"] = self.authority.to_dict()

        if self.resource_ticket is not None:
            result["resource_ticket"] = self.resource_ticket.to_dict()

        return result

    def get_authority(self) -> AuthAvro:
        return self.authority

    def get_resource_ticket(self) -> ResourceTicketAvro:
        return self.resource_ticket

    def get_old_units(self) -> int:
        return self.old_units

    def set_authority(self, authority: AuthAvro):
        self.authority = authority

    def set_resource_ticket(self, resource_ticket: ResourceTicketAvro):
        self.resource_ticket = resource_ticket

    def set_old_units(self, old_units: int):
        self.old_units = old_units

    def get_delegation_id(self) -> str:
        return self.delegation_id

    def set_delegation_id(self, delegation_id: str):
        self.delegation_id = delegation_id

    def __str__(self):
        return f"old_units: {self.old_units}, authority: {self.authority} " \
               f"resource_ticket: {self.resource_ticket} delegation_id: {self.delegation_id}"

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.old_units is None or self.resource_ticket is None or self.authority is None or \
                self.delegation_id is None:
            ret_val = False
        return ret_val

    def __eq__(self, other):
        if not isinstance(other, Ticket):
            return False

        return self.old_units == other.old_units and self.authority == other.authority and \
               self.resource_ticket == other.resource_ticket and self.delegation_id == other.delegation_id
