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


class PoaInfoAvro(AbcObjectAvro):
    """
    Implements Avro representation of a query Message
    """
    def __init__(self, *, operation: str = None, cpu_info = None, numa_info = None):
        self.operation = operation
        self.cpu_info = cpu_info
        self.numa_info = numa_info

    def get_cpu_info(self) -> dict:
        """
        Return Cpu Info
        """
        if self.cpu_info is not None:
            if not isinstance(self.cpu_info, dict):
                self.cpu_info = pickle.loads(self.cpu_info)
        return self.cpu_info

    def get_numa_info(self) -> dict:
        """
        Return Numa Info
        """
        if self.numa_info is not None:
            if not isinstance(self.numa_info, dict):
                self.numa_info = pickle.loads(self.numa_info)
        return self.numa_info

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.operation is None:
            ret_val = False
        return ret_val
