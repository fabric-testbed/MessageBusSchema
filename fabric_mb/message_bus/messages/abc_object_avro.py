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
from abc import ABC, abstractmethod
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.constants import Constants


class AbcObjectAvro(ABC):
    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException(Constants.ERROR_INVALID_ARGUMENTS)

        result = self.__dict__.copy()

        for k in self.__dict__:
            if result[k] is None:
                result.pop(k)
            elif isinstance(result[k], AbcObjectAvro):
                result[k] = result[k].to_dict()
            elif isinstance(result[k], list):
                temp = []
                for elem in result[k]:
                    temp.append(elem.to_dict())
                result[k] = temp

        return result

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__:
                self.__dict__[k] = v

    def __str__(self):
        d = self.__dict__.copy()
        for k in self.__dict__:
            if d[k] is None:
                d.pop(k)
        if len(d) == 0:
            return ''
        ret = "{ "
        for i, v in d.items():
            ret = f"{ret} {i}: {v},"
        return ret[:-1] + "}"

    @abstractmethod
    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """

    def __eq__(self, other):
        assert isinstance(other, AbcObjectAvro)
        for f, v in self.__dict__.items():
            if v != other.__dict__[f]:
                return False
        return True
