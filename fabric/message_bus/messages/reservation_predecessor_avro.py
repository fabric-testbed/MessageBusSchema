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


class ReservationPredecessorAvro:
    def __init__(self):
        self.reservation_id = None
        self.filter = None

    def from_dict(self, value: dict):
        self.reservation_id = value.get('reservation_id', None)
        self.filter = value.get('filter', None)

    def to_dict(self) -> dict:
        if not self.validate():
            raise Exception("Invalid arguments")

        result = {'reservation_id': self.reservation_id,
                  'filter': self.filter}
        return result

    def __str__(self):
        return "reservation_id: {} filter: {}".format(self.reservation_id, self.filter)

    def get_reservation_id(self) -> str:
        return self.reservation_id

    def set_reservation_id(self, value: str):
        self.reservation_id = value

    def get_filter_properties(self) -> dict:
        return self.filter

    def set_filter_properties(self, value: dict):
        self.filter = value

    def __eq__(self, other):
        if not isinstance(other, ReservationPredecessorAvro):
            return False

        return self.reservation_id == other.reservation_id and self.filter == other.filter

    def validate(self) -> bool:
        ret_val = True
        if self.reservation_id is None or self.filter is None:
            ret_val = False
        return ret_val