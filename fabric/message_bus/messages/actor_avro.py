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
from fabric.message_bus.messages.auth_avro import AuthAvro


class ActorAvro:
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

    def __eq__(self, other):
        if not isinstance(other, ActorAvro):
            return False

        return self.name == other.name and self.type == other.type and self.description == other.description and \
               self.policy_class == other.policy_class and self.policy_module == other.policy_module and \
               self.actor_class == other.actor_class and self.actor_module == other.actor_module and \
               self.online == other.online and self.management_module == other.management_module and \
               self.management_class == other.management_class and self.id == other.id and \
               self.policy_guid == other.policy_guid

    def from_dict(self, value: dict):
        self.name = value.get('name', None)
        self.type = value.get('type', None)
        self.owner = value.get('owner', None)
        self.description = value.get('description', None)
        self.policy_class = value.get('policy_class', None)
        self.policy_module = value.get('policy_module', None)
        self.actor_class = value.get('actor_class', None)
        self.actor_module = value.get('actor_module', None)
        self.online = value.get('online', None)
        self.management_class = value.get('management_class', None)
        self.management_module = value.get('management_module', None)
        self.id = value.get('id', None)
        self.policy_guid = value.get('policy_guid', None)

    def to_dict(self) -> dict:
        if not self.validate():
            raise Exception("Invalid arguments")

        result = {
            "name": self.name
        }
        if self.type is not None:
            result['type'] = self.type
        if self.owner is not None:
            result['owner'] = self.owner.to_dict()
        if self.description is not None:
            result['description'] = self.description
        if self.policy_class is not None:
            result['policy_class'] = self.policy_class
        if self.policy_module is not None:
            result['policy_module'] = self.policy_module
        if self.actor_class is not None:
            result['actor_class'] = self.actor_class
        if self.actor_module is not None:
            result['actor_module'] = self.actor_module
        if self.online is not None:
            result['online'] = self.online
        if self.management_class is not None:
            result['management_class'] = self.management_class
        if self.management_module is not None:
            result['management_module'] = self.management_module
        if self.id is not None:
            result['id'] = self.id
        if self.policy_guid is not None:
            result['policy_guid'] = self.policy_guid
        return result

    def __str__(self):
        return "name: {} type: {} owner: {} description: {} policy_class: {} policy_module: {} actor_class: {}" \
               " actor_module: {} online: {} management_class: {} management_module: {} id: {} policy_guid: {}".format(
            self.name, self.type, self.owner, self.description, self.policy_class, self.policy_module, self.actor_class,
        self.actor_module, self.online, self.management_class, self.management_module, self.id, self.policy_guid)

    def get_name(self) -> str:
        return self.name

    def set_name(self, value: str):
        self.name = value

    def set_type(self, value: int):
        self.type = value

    def get_type(self) -> int:
        return self.type

    def set_owner(self, value: AuthAvro):
        self.owner = value

    def get_owner(self) -> AuthAvro:
        return self.owner

    def set_description(self, value: str):
        self.description = value

    def get_description(self) -> str:
        return self.description

    def get_policy_class(self) -> str:
        return self.policy_class

    def set_policy_class(self, value: str):
        self.policy_class = value

    def get_policy_module(self) -> str:
        return self.policy_module

    def set_policy_module(self, value: str):
        self.policy_module = value

    def get_actor_class(self) -> str:
        return self.actor_class

    def set_actor_class(self, value: str):
        self.actor_class = value

    def get_actor_module(self) -> str:
        return self.actor_module

    def set_actor_module(self, value: str):
        self.actor_module = value

    def set_online(self, value: bool):
        self.online = value

    def get_online(self) -> bool:
        return self.online

    def get_management_class(self) -> str:
        return self.management_class

    def set_management_class(self, value: str):
        self.management_class = value

    def get_management_module(self) -> str:
        return self.management_module

    def set_management_module(self, value: str):
        self.management_module = value

    def set_id(self, value: str):
        self.id = value

    def get_id(self) -> str:
        return self.id

    def set_policy_guid(self, value: str):
        self.policy_guid = value

    def get_policy_guid(self) -> str:
        return self.policy_guid

    def validate(self) -> bool:
        ret_val = True
        if self.name is None or self.owner is None or self.description is None or self.policy_module is None or \
            self.policy_class is None or self.policy_guid is None or self.actor_class is None or \
                self.actor_module is None or self.id is None:
            ret_val = False
        return ret_val