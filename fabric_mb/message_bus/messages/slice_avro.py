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
"""
Implements Avro representation of a Slice
"""
from fabric_mb.message_bus.message_bus_exception import MessageBusException
from fabric_mb.message_bus.messages.auth_avro import AuthAvro


class SliceAvro:
    """
    Implements Avro representation of a Slice
    """
    def __init__(self):
        self.slice_name = None
        self.guid = None
        self.owner = None
        self.description = None
        self.config_properties = None
        self.resource_type = None
        self.client_slice = False
        self.broker_client_slice = False
        self.graph_id = None
        self.state = None
        self.inventory = False

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        self.slice_name = value['slice_name']
        self.guid = value['guid']
        if value.get('owner', None) is not None:
            self.owner = AuthAvro()
            self.owner.from_dict(value['owner'])

        self.description = value.get('description', None)
        self.config_properties = value.get('config_properties', None)
        self.resource_type = value.get('resource_type', None)
        self.client_slice = value.get('client_slice', None)
        self.broker_client_slice = value.get('broker_client_slice', None)
        self.graph_id = value.get('graph_id', None)
        self.state = value.get('state', None)
        self.inventory = value.get('inventory', None)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise MessageBusException("Invalid arguments")

        result = {
            "slice_name": self.slice_name,
            "guid": self.guid
        }
        if self.owner is not None:
            result["owner"] = self.owner.to_dict()

        if self.description is not None:
            result["description"] = self.description

        if self.config_properties is not None:
            result["config_properties"] = self.config_properties

        if self.resource_type is not None:
            result["resource_type"] = self.resource_type

        if self.client_slice is not None:
            result["client_slice"] = self.client_slice

        if self.broker_client_slice is not None:
            result["broker_client_slice"] = self.broker_client_slice

        if self.graph_id is not None:
            result["graph_id"] = self.graph_id

        if self.state is not None:
            result['state'] = self.state

        if self.inventory is not None:
            result["inventory"] = self.inventory
        return result

    def __str__(self):
        return f"slice_name: {self.slice_name} guid: {self.guid} owner: {self.owner} description: {self.description} " \
               f"config_properties: {self.config_properties} " \
               f"resource_type: {self.resource_type} client_slice: {self.client_slice} " \
               f"broker_client_slice: {self.broker_client_slice} graph_id: {self.graph_id} state: {self.state} " \
               f"inventory: {self.inventory}"

    def print(self, all: bool = False):
        """
        Print object on console
        Used by management CLI
        """
        print("")
        print("Slice Name: {} Slice Guid: {}".format(self.slice_name, self.guid))
        if self.graph_id is not None:
            print("Graph ID: {}".format(self.graph_id))
        if all:
            if self.owner is not None:
                print(f"Slice owner: {self.owner}")
            if self.description is not None:
                print(f"Description: {self.description}")
            if self.resource_type is not None:
                print(f"resource_type: {self.resource_type}")
            if self.client_slice is not None:
                print(f"client_slice: {self.client_slice}")
            if self.broker_client_slice is not None:
                print(f"broker_client_slice: {self.broker_client_slice}")
            if self.inventory is not None:
                print(f"inventory: {self.inventory}")

            if self.config_properties is not None:
                print(f"config_properties: {self.config_properties}")
        print("")

    def set_slice_name(self, value: str):
        """
        Set slice name
        @param value value
        """
        self.slice_name = value

    def get_slice_name(self) -> str:
        """
        Get Slice name
        @return slice name
        """
        return self.slice_name

    def set_owner(self, value: AuthAvro):
        """
        Set owner
        @param value value
        """
        self.owner = value

    def get_owner(self) -> AuthAvro:
        """
        Get Slice owner
        @return slice owner
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
        Get Slice description
        @return slice description
        """
        return self.description

    def get_config_properties(self) -> dict:
        """
        Get config properties
        @return config properties
        """
        return self.config_properties

    def set_config_properties(self, value: dict):
        """
        Set config properties
        @param value value
        """
        self.config_properties = value

    def get_resource_type(self) -> str:
        """
        Get resource type
        @return resource type
        """
        return self.resource_type

    def set_resource_type(self, value: str):
        """
        Set resource type
        @param value value
        """
        self.resource_type = value

    def is_client_slice(self):
        """
        Return True if slice is client slice else False
        """
        return self.client_slice

    def set_client_slice(self, value: bool):
        """
        Set slice is client slice
        @param value value
        """
        self.client_slice = value

    def is_broker_client_slice(self):
        """
        Return True if slice is broker client slice else False
        """
        return self.broker_client_slice

    def set_broker_client_slice(self, value: bool):
        """
        Set slice as broker client slice
        @param value value
        """
        self.broker_client_slice = value

    def get_slice_id(self) -> str:
        """
        Return slice id
        @return slice id
        """
        return self.guid

    def set_slice_id(self, slice_id: str):
        """
        Set slice id
        @param value value
        """
        self.guid = slice_id

    def __eq__(self, other):
        if not isinstance(other, SliceAvro):
            return False

        return self.slice_name == other.slice_name and self.guid == other.guid and self.owner == other.owner and \
            self.description == other.description and self.config_properties == other.config_properties and \
            self.resource_type == other.resource_type and \
            self.client_slice == other.client_slice and self.broker_client_slice == other.broker_client_slice

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.slice_name is None or self.owner is None or self.description is None or \
                self.broker_client_slice is None or self.guid is None:
            ret_val = False
            print(f"name {self.slice_name} owner {self.owner} description: {self.description} broker_client_slice: "
                  f"{self.broker_client_slice} guid: {self.guid}")
        return ret_val

    def get_graph_id(self) -> str:
        """
        Return graph id
        @return graph id
        """
        return self.graph_id

    def get_state(self) -> int:
        """
        Return state
        @return state
        """
        return self.state

    def get_inventory(self) -> bool:
        return self.inventory
