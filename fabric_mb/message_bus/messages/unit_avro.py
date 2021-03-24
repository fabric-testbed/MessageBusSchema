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
import pickle
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class UnitAvro:
    """
    Implements Avro representation of a Unit
    """
    def __init__(self):
        self.rtype = None
        self.parent_id = None
        self.state = None
        self.sequence = 0
        self.properties = None
        self.reservation_id = None
        self.slice_id = None
        self.actor_id = None
        self.sliver = None

    def get_unit_id(self) -> str:
        return self.reservation_id

    def get_properties(self) -> dict:
        """
        Return properties
        """
        return self.properties

    def get_resource_type(self) -> str:
        return self.rtype

    def get_parent_id(self) -> str:
        return self.parent_id

    def get_state(self) -> int:
        return self.state

    def get_sequence(self) -> int:
        return self.sequence

    def get_reservation_id(self) -> str:
        return self.reservation_id

    def get_slice_id(self) -> str:
        return self.slice_id

    def get_actor_id(self) -> str:
        return self.actor_id

    def set_properties(self, value: dict):
        """
        Set properties
        @param value value
        """
        self.properties = value

    def set_unit_id(self, uid: str):
        self.uid = uid

    def set_resource_type(self, rtype: str):
        self.rtype = rtype

    def set_parent_id(self, parent_id: str):
        self.parent_id = parent_id

    def set_state(self, state: int):
        self.state = state

    def set_sequence(self, sequence: int):
        self.sequence = sequence

    def set_reservation_id(self, reservation_id: str):
        self.reservation_id = reservation_id

    def set_slice_id(self, slice_id: str):
        self.slice_id = slice_id

    def set_actor_id(self, actor_id: str):
        self.actor_id = actor_id

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.rtype = value.get('rtype', None)
        self.parent_id = value.get('parent_id', None)
        self.state = value.get('state', None)
        self.sequence = value.get('sequence', None)
        self.reservation_id = value.get('reservation_id', None)
        self.slice_id = value.get('slice_id', None)
        self.actor_id = value.get('actor_id', None)
        self.properties = value.get('properties', None)
        self.sliver = value.get('sliver', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "properties": self.properties,
            "reservation_id": self.reservation_id
        }
        if self.rtype is not None:
            result['rtype'] = self.rtype

        if self.parent_id is not None:
            result['parent_id'] = self.parent_id

        if self.state is not None:
            result['state'] = self.state

        if self.sequence is not None:
            result['sequence'] = self.sequence

        if self.slice_id is not None:
            result['slice_id'] = self.slice_id

        if self.actor_id is not None:
            result['actor_id'] = self.actor_id

        if self.sliver is not None:
            result['sliver'] = self.sliver
        return result

    def __eq__(self, other):
        if not isinstance(other, UnitAvro):
            return False

        return self.properties == other.properties and self.rtype == other.rtype and \
               self.parent_id == other.parent_id and self.state == other.state and self.sequence == other.sequence and \
               self.reservation_id == other.reservation_id and self.slice_id == other.slice_id and \
               self.actor_id == other.actor_id and self.sliver == other.sliver

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True

        if self.properties is None or self.reservation_id is None:
            ret_val = False

        return ret_val

    def set_sliver(self, sliver):
        if sliver is not None:
            self.sliver = pickle.dumps(sliver)

    def get_sliver(self):
        if self.sliver is not None:
            return pickle.loads(self.sliver)
        return self.sliver
