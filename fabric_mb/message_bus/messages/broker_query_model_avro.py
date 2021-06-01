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
"""
Implements Avro representation of a Pool Info
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException


class BrokerQueryModelAvro:
    """
    Implements Avro representation of a Broker Query Model
    """
    def __init__(self):
        self.level = None
        self.model = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.level = value.get('level', None)
        self.model = value.get('model', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "level": self.level,
            "model": self.model
        }
        return result

    def __str__(self):
        return "level: {} model: {}".format(self.level, self.model)

    def set_level(self, level: str):
        """
        Set level
        @param level level
        """
        self.level = level

    def get_level(self) -> str:
        """
        Return level
        @return level
        """
        return self.level

    def set_model(self, model: str):
        """
        Set model
        @param model model
        """
        self.model = model

    def get_model(self) -> str:
        """
        Return model
        @return model
        """
        return self.model

    def __eq__(self, other):
        if not isinstance(other, BrokerQueryModelAvro):
            return False

        return self.level == other.level and self.model == other.model

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.level is None or self.model is None:
            ret_val = False
        return ret_val
