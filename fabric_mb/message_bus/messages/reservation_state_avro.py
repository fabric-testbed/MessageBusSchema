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


class ReservationStateAvro(AbcObjectAvro):
    """
    Implements Avro representation of a Reservation State
    """
    def __init__(self):
        self.name = self.__class__.__name__
        self.rid = None
        self.state = None
        self.pending_state = None

    def set_reservation_id(self, rid: str):
        self.rid = rid

    def get_reservation_id(self) -> str:
        return self.rid

    def get_state(self) -> int:
        """
        Return state
        """
        return self.state

    def set_state(self, value: int):
        """
        Set state
        @param value value
        """
        self.state = value

    def get_pending_state(self) -> int:
        """
        Get Pending State
        """
        return self.pending_state

    def set_pending_state(self, value: int):
        """
        Set pending state
        @param value value
        """
        self.pending_state = value

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        ret_val = True
        if self.rid is None or self.state is None or self.pending_state is None:
            ret_val = False
        return ret_val
