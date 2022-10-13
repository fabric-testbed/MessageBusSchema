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
from fabric_mb.message_bus.messages.abc_object_avro import AbcObjectAvro
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.slice_avro import SliceAvro


class DelegationAvro(AbcObjectAvro):
    """
    Implements Avro representation of a Delegation
    """
    def __init__(self):
        self.delegation_id = None
        self.slice = None
        self.graph = None
        self.sequence = 0
        self.state = None
        self.delegation_name = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.SLICE:
                    self.__dict__[k] = SliceAvro()
                    self.__dict__[k].from_dict(value=v)
                else:
                    self.__dict__[k] = v

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.delegation_id is None or self.slice is None or self.sequence is None:
            ret_val = False
        return ret_val

    def get_delegation_id(self) -> str:
        """
        Return delegation id
        """
        return self.delegation_id

    def get_slice_object(self) -> SliceAvro:
        """
        Return slice object
        """
        return self.slice

    def get_graph(self) -> str:
        """
        Return delegation graph
        """
        return self.graph

    def get_sequence(self) -> int:
        """
        Return sequence number
        """
        return self.sequence

    def get_state(self) -> int:
        return self.state

    def get_name(self) -> str:
        return self.delegation_name

    def print(self):
        """
        Print on console
        """
        print("")
        print("Delegation ID: {} Slice ID: {}".format(self.delegation_id, self.slice.get_slice_id()))
        if self.delegation_name is not None:
            print("Delegation Name: {}".format(self.delegation_name))
        if self.sequence is not None:
            print("Sequence: {}".format(self.sequence))
        if self.state is not None:
            print(f"State: {self.state}")
        if self.graph is not None:
            print("Graph: {}".format(self.graph))
        print("")
