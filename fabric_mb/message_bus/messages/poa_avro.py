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
from typing import List, Dict
from uuid import uuid4

from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.auth_avro import AuthAvro


class PoaAvro(AbcMessageAvro):
    """
    Implements Avro representation of a query Message
    """
    def __init__(self, *, operation: str = None, vcpu_cpu_map = None, node_set = None, callback_topic: str = None,
                 rid: str = None, project_id: str = None, id_token: str = None, message_id: str = None,
                 auth: AuthAvro = None, poa_id: str = None, sequence: int = None, slice_id: str = None,
                 keys=None):
        super(PoaAvro, self).__init__(callback_topic=callback_topic, id_token=id_token,
                                      name=AbcMessageAvro.poa, message_id=message_id)
        self.operation = operation
        self.auth = auth
        self.rid = rid
        self.vcpu_cpu_map = vcpu_cpu_map
        self.node_set = node_set
        self.keys = keys
        self.poa_id = poa_id
        self.project_id = project_id
        self.sequence = sequence
        self.slice_id = slice_id
        if self.poa_id is None:
            self.poa_id = uuid4().__str__()

    def get_keys(self) -> List[Dict[str, str]]:
        if self.keys is not None:
            if not isinstance(self.keys, List):
                self.keys = pickle.loads(self.keys)
        return self.keys

    def get_node_set(self) -> List[str]:
        if self.node_set is not None:
            if not isinstance(self.node_set, List):
                self.node_set = pickle.loads(self.node_set)
        return self.node_set

    def get_vcpu_cpu_map(self) -> List[Dict[str, str]]:
        if self.vcpu_cpu_map is not None:
            if not isinstance(self.vcpu_cpu_map, List):
                self.vcpu_cpu_map = pickle.loads(self.vcpu_cpu_map)
        return self.vcpu_cpu_map

    def get_operation(self) -> str:
        return self.operation

    def get_poa_id(self) -> str:
        return self.poa_id

    def get_rid(self) -> str:
        return self.rid

    def get_project_id(self) -> str:
        return self.project_id

    def get_sequence(self) -> int:
        return self.sequence

    def get_slice_id(self) -> str:
        return self.slice_id

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()
        if self.callback_topic is None or self.operation is None:
            ret_val = False
        return ret_val
