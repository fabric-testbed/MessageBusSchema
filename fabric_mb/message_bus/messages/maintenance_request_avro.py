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
from typing import Dict, List

from fabric_mb.message_bus.messages.abc_message_avro import AbcMessageAvro
from fabric_mb.message_bus.messages.constants import Constants
from fabric_mb.message_bus.messages.site_avro import SiteAvro


class MaintenanceRequestAvro(AbcMessageAvro):
    """
    Implements Avro representation of an Maintenance Request Message
    """
    def __init__(self, *, properties: Dict[str, str] = None, actor_guid: str = None, callback_topic: str = None,
                 id_token: str = None, message_id: str = None, sites: List[SiteAvro] = None):
        super(MaintenanceRequestAvro, self).__init__(callback_topic=callback_topic, id_token=id_token,
                                                     name=AbcMessageAvro.maintenance_request, message_id=message_id)
        self.actor_guid = actor_guid
        self.properties = properties
        self.sites = sites

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.SITES:
                    self.from_dict_sites(value=v)
                else:
                    self.__dict__[k] = v

    def from_dict_sites(self, value: list):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value is not None:
            for s in value:
                site_obj = SiteAvro()
                site_obj.from_dict(s)
                if self.sites is None:
                    self.sites = []
                self.sites.append(site_obj)

    def set_properties(self, properties: Dict[str, str]):
        self.properties = properties

    def get_properties(self) -> Dict[str, str]:
        return self.properties

    def set_sites(self, sites: List[SiteAvro]):
        self.sites = sites

    def get_sites(self) -> List[SiteAvro]:
        return self.sites
