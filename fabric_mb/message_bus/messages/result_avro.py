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


class ResultAvro(AbcObjectAvro):
    """
    Implements Avro representation of a Result Message Status
    """
    def __init__(self):
        self.code = 0
        self.message = ""
        self.details = ""

    def get_code(self) -> int:
        """
        Return Status code
        """
        return self.code

    def set_code(self, code: int):
        """
        Set status code
        @param code code
        """
        self.code = code

    def get_message(self) -> str:
        """
        Return status message
        @return status message
        """
        return self.message

    def set_message(self, msg: str):
        """
        Set status message
        @param msg msg
        """
        self.message = msg

    def get_details(self) -> str:
        """
        Return details
        @return details
        """
        return self.details

    def set_details(self, value: str):
        """
        Set details
        @param value value
        """
        self.details = value

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.code is None:
            ret_val = False
        return ret_val
