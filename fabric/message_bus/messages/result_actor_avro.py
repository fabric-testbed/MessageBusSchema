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
Implements Avro representation of a Result Message containing Actors
"""
from typing import List
from uuid import uuid4

from fabric.message_bus.messages.actor_avro import ActorAvro
from fabric.message_bus.messages.result_avro import ResultAvro
from fabric.message_bus.messages.message import IMessageAvro


class ResultActorAvro(IMessageAvro):
    """
    Implements Avro representation of a Result Message containing Actors
    """
    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "message_id", "status", "actors", "id"]

    def __init__(self):
        self.name = IMessageAvro.ResultActor
        self.message_id = None
        self.status = None
        self.actors = None
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def from_dict(self, value: dict):
        """
        The Avro Python library does not support code generation.
        For this reason we must provide conversion from dict to our class for de-serialization
        :param value: incoming message dictionary
        """
        if value['name'] != IMessageAvro.ResultActor:
            raise Exception("Invalid message")
        self.message_id = value['message_id']
        self.status = ResultAvro()
        self.status.from_dict(value['status'])
        actors_list = value.get('actors', None)
        if actors_list is not None:
            for s in actors_list:
                actor_obj = ActorAvro()
                actor_obj.from_dict(s)
                if self.actors is None:
                    self.actors = []
                self.actors.append(actor_obj)

    def to_dict(self) -> dict:
        """
        The Avro Python library does not support code generation.
        For this reason we must provide a dict representation of our class for serialization.
        :return dict representing the class
        """
        if not self.validate():
            raise Exception("Invalid arguments")

        result = {
            "name": self.name,
            "message_id": self.message_id,
            "status": self.status.to_dict()
        }
        if self.actors is not None:
            temp = []
            for s in self.actors:
                temp.append(s.to_dict())
            result["actors"] = temp
        return result

    def get_message_id(self) -> str:
        """
        Returns the message_id
        """
        return self.message_id

    def get_message_name(self) -> str:
        """
        Returns the message name
        """
        return self.name

    def __str__(self):
        return "name: {} message_id: {} status: {} actors: {}".format(self.name, self.message_id, self.status,
                                                                      self.actors)

    def get_status(self) -> ResultAvro:
        """
        Returns the result status
        @return status
        """
        return self.status

    def set_status(self, value: ResultAvro):
        """
        Set Result status
        @param value: value
        """
        self.status = value

    def get_actors(self) -> List[ActorAvro]:
        """
        Return list of Actors
        @return list of actors
        """
        return self.actors

    def get_id(self) -> str:
        """
        Return id
        @return id
        """
        return self.id.__str__()

    def get_callback_topic(self) -> str:
        """
        Return callback topic
        @return callback topic
        """
        return None

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = super().validate()
        if self.status is None:
            ret_val = False
        return ret_val
