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
from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro


class MaintenanceRequestAvro(AbcMessageAvro):
    """
    Implements Avro representation of an Maintenance Request Message
    """

    def __init__(self, *, properties: dict = None, actor_guid: str = None, callback_topic: str = None, id_token: str = None,
                 message_id: str = None):
        super(MaintenanceRequestAvro, self).__init__(callback_topic=callback_topic, id_token=id_token,
                                                     name=AbcMessageAvro.maintenance_request, message_id=message_id)
        self.actor_guid = actor_guid
        self.properties = properties

    def set_properties(self, properties: dict):
        self.properties = properties

    def get_properties(self) -> dict:
        return self.properties
