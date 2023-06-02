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
from typing import List, Dict

from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.auth_avro import AuthAvro


class PoaAvro(AbcMessageAvro):
    """
    Implements Avro representation of a query Message
    """
    def __init__(self, *, operation: str = None, vcpu_cpu_map: List[Dict[str, str]] = None,
                 node_set: List[str] = None, callback_topic: str = None, rid: str = None,
                 id_token: str = None, message_id: str = None, auth: AuthAvro = None):
        super(PoaAvro, self).__init__(callback_topic=callback_topic, id_token=id_token,
                                      name=AbcMessageAvro.poa, message_id=message_id)
        self.operation = operation
        self.auth = auth
        self.rid = rid
        self.vcpu_cpu_map = vcpu_cpu_map
        self.node_set = node_set

    def get_node_set(self) -> List[str]:
        return self.node_set

    def get_vcpu_cpu_map(self) -> List[Dict[str, str]]:
        return self.vcpu_cpu_map

    def get_operation(self) -> str:
        return self.operation

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()
        if self.callback_topic is None or self.operation is None:
            ret_val = False
        return ret_val
