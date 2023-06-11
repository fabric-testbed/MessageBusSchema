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
from fabric_mb.message_bus.messages.auth_avro import AuthAvro


class PoaInfoAvro(AbcObjectAvro):
    """
    Implements Avro representation of a query Message
    """
    def __init__(self, *, operation: str = None, info=None, rid: str = None, auth: AuthAvro = None,
                 poa_id: str = None, project_id: str = None, slice_id: str = None, state: str = None,
                 error: str = None):
        self.operation = operation
        self.info = info
        self.rid = rid
        self.auth = auth
        self.poa_id = poa_id
        self.project_id = project_id
        self.slice_id = slice_id
        self.state = state
        self.error = error

    def get_info(self) -> dict:
        """
        Return Cpu Info
        """
        if self.info is not None:
            if not isinstance(self.info, dict):
                self.info = pickle.loads(self.info)
        return self.info

    def get_operation(self) -> str:
        return self.operation

    def get_rid(self) -> str:
        return self.rid

    def get_project_id(self) -> str:
        return self.project_id

    def get_auth(self) -> AuthAvro:
        return self.auth

    def get_poa_id(self) -> str:
        return self.poa_id

    def get_slice_id(self) -> str:
        return self.slice_id

    def get_state(self) -> str:
        return self.state

    def get_error(self) -> str:
        return self.error

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.operation is None:
            ret_val = False
        return ret_val
