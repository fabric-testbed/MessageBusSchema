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
from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.constants import Constants


class ActorAvro(AbcObjectAvro):
    """
    Implements Avro representation of an Actor
    """
    def __init__(self):
        self.name = None
        self.type = None
        self.owner = None
        self.description = None
        self.policy_class = None
        self.policy_module = None
        self.actor_class = None
        self.actor_module = None
        self.online = None
        self.management_class = None
        self.management_module = None
        self.id = None
        self.policy_guid = None

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        for k, v in value.items():
            if k in self.__dict__ and v is not None:
                if k == Constants.OWNER:
                    self.__dict__[k] = AuthAvro()
                    self.__dict__[k].from_dict(value=v)
                else:
                    self.__dict__[k] = v

    def get_name(self) -> str:
        """
        Returns the name
        """
        return self.name

    def set_name(self, value: str):
        """
        Set name
        @param value value
        """
        self.name = value

    def set_type(self, value: int):
        """
        Set type
        @param value value
        """
        self.type = value

    def get_type(self) -> int:
        """
        Get Type
        @return type
        """
        return self.type

    def set_owner(self, value: AuthAvro):
        """
        Set owner
        @param value value
        """
        self.owner = value

    def get_owner(self) -> AuthAvro:
        """
        Return owner
        @return owner
        """
        return self.owner

    def set_description(self, value: str):
        """
        Set description
        @param value value
        """
        self.description = value

    def get_description(self) -> str:
        """
        Get description
        @return description
        """
        return self.description

    def get_policy_class(self) -> str:
        """
        Get Policy Class name
        @return policy class name
        """
        return self.policy_class

    def set_policy_class(self, value: str):
        """
        Set policy class name
        @param value value
        """
        self.policy_class = value

    def get_policy_module(self) -> str:
        """
        Get Policy module name
        @return policy module name
        """
        return self.policy_module

    def set_policy_module(self, value: str):
        """
        Set policy module name
        @param value value
        """
        self.policy_module = value

    def get_actor_class(self) -> str:
        """
        Get actor class name
        @return actor class name
        """
        return self.actor_class

    def set_actor_class(self, value: str):
        """
        Set actor class name
        @param value value
        """
        self.actor_class = value

    def get_actor_module(self) -> str:
        """
        Get actor module name
        @return actor module name
        """
        return self.actor_module

    def set_actor_module(self, value: str):
        """
        Set actor module name
        @param value value
        """
        self.actor_module = value

    def set_online(self, value: bool):
        """
        Set online
        @param value value
        """
        self.online = value

    def get_online(self) -> bool:
        """
        Get online
        @return online status
        """
        return self.online

    def get_management_class(self) -> str:
        """
        Get Management class
        """
        return self.management_class

    def set_management_class(self, value: str):
        """
        Set management class name
        @param value value
        """
        self.management_class = value

    def get_management_module(self) -> str:
        """
        Get management module
        """
        return self.management_module

    def set_management_module(self, value: str):
        """
        Set management module name
        @param value value
        """
        self.management_module = value

    def set_id(self, value: str):
        """
        Set id
        @param value value
        """
        self.id = value

    def get_id(self) -> str:
        """
        Return id
        @return id
        """
        return self.id

    def set_policy_guid(self, value: str):
        """
        Set policy guid
        @param value value
        """
        self.policy_guid = value

    def get_policy_guid(self) -> str:
        """
        Get policy guid
        """
        return self.policy_guid

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.name is None or self.owner is None or self.description is None or self.policy_module is None or \
            self.policy_class is None or self.policy_guid is None or self.actor_class is None or \
                self.actor_module is None or self.id is None:
            ret_val = False
        return ret_val
