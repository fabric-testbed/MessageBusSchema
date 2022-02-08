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
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.proxy_avro import ProxyAvro


class AddPeerAvro(AbcMessageAvro):
    """
    Implements Avro representation of an Add Peer Message
    """

    def __init__(self, *, peer: ProxyAvro = None, callback_topic: str = None, id_token: str = None,
                 message_id: str = None):
        super(AddPeerAvro, self).__init__(name=AbcMessageAvro.add_peer, callback_topic=callback_topic,
                                          id_token=id_token, message_id=message_id)
        self.name = AbcMessageAvro.add_peer
        self.peer = peer

    def from_dict(self, value: dict):
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.PEER:
                    self.__dict__[k] = ProxyAvro()
                    self.__dict__[k].from_dict(value=v)
                else:
                    self.__dict__[k] = v
