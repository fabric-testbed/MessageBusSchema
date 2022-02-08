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
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.ticket import Ticket
from fabric_mb.message_bus.messages.unit_avro import UnitAvro


class ResourceSetAvro(AbcObjectAvro):
    """
    Implements Avro representation of a Reservation Resource Set
    """
    def __init__(self):
        self.units = None
        self.type = None
        self.sliver = None
        self.ticket = None
        self.unit_set = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.TICKET:
                    self.__dict__[k] = Ticket()
                    self.__dict__[k].from_dict(value=v)
                elif k == Constants.UNIT_SET:
                    self.__dict__[k] = []
                    for u in v:
                        uu = UnitAvro()
                        uu.from_dict(value=u)
                        self.__dict__[k].append(uu)
                else:
                    self.__dict__[k] = v

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.units is None or self.type is None:
            ret_val = False
        return ret_val

    def set_sliver(self, sliver):
        if sliver is not None:
            self.sliver = pickle.dumps(sliver)

    def get_sliver(self):
        if self.sliver is not None:
            return pickle.loads(self.sliver)
        return self.sliver
