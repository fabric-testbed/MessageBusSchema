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
from fabric_mb.message_bus.messages.abc_object_avro import AbcObjectAvro


class UnitAvro(AbcObjectAvro):
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
        self.reservation_id = uid

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
