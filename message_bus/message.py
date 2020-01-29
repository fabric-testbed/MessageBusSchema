#!/usr/bin/env python3
# #######################################################################
# Copyright (c) 2020 RENCI. All rights reserved
# This material is the confidential property of RENCI or its
# licensors and may be used, reproduced, stored or transmitted only in
# accordance with a valid RENCI license or sublicense agreement.
# #######################################################################

from uuid import uuid4


class IMessage(object):
    """
        Implements the base class for storing the deserialized Avro record. Each message is uniquely identified by
        a globally unique identifier. It must be inherited to include Actor specific fields and to_dict implementation.
        New Avro schema must be defined as per the inherited class and should be used for the producer/consumer
    """

    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["slice_id", "id"]

    def __init__(self, slice_id):
        if type(slice_id) is dict:
            self.slice_id = slice_id["slice_id"]
        else:
            self.slice_id = slice_id
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return {
            "slice_id": self.slice_id
        }

    def print(self):
        print("slice_id:" + self.slice_id)
