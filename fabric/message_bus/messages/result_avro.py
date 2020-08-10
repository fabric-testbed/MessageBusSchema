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


class ResultAvro:
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["code", "message", "details"]

    def __init__(self):
        self.code = 0
        self.message = ""
        self.details = ""

    def from_dict(self, value: dict):
        self.code = value.get("code", None)
        self.message = value.get("message", None)
        self.details = value.get("details", None)

    def to_dict(self) -> dict:
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        if not self.validate():
            raise Exception("Invalid arguments")

        result = {
            "code": self.code
        }
        if self.message is not None:
            result["message"] = self.message
        if self.details is not None:
            result["details"] = self.details
        return result

    def __str__(self):
        return "code: {} message: {} details: {}".format(self.code, self.message, self.details)

    def get_code(self) -> int:
        return self.code

    def set_code(self, code: int):
        self.code = code

    def get_message(self) -> str:
        return self.message

    def set_message(self, msg: str):
        self.message = msg

    def get_details(self) -> str:
        return self.details

    def set_details(self, value: str):
        self.details = value

    def __eq__(self, other):
        if not isinstance(other, ResultAvro):
            return False

        return self.code == other.code and self.message == other.message and self.details == other.details

    def validate(self) -> bool:
        ret_val = True
        if self.code is None:
            ret_val = False
        return ret_val