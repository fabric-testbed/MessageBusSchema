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
from datetime import datetime
from typing import List

from fabric_mb.message_bus.messages.abc_object_avro import AbcObjectAvro


class SiteAvro(AbcObjectAvro):
    """
    Implements Avro representation of an Site
    """
    LEASE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S %z"

    def __init__(self, *, name: str = None, workers: str = None, deadline: str = None, state: int = None):
        self.name = name
        self.workers = workers
        self.deadline = deadline
        self.state = state

    def validate(self) -> bool:
        """
        Check if the object is valid and contains all mandatory fields
        :return True on success; False on failure
        """
        if self.name is None:
            return False
        return True

    def get_name(self) -> str:
        return self.name

    def get_workers(self) -> List[str] or None:
        if self.workers is None:
            return self.workers
        return self.workers.split(',')

    def get_deadline(self) -> datetime or None:
        if self.deadline is None:
            return self.deadline

        return datetime.strptime(self.deadline, self.LEASE_TIME_FORMAT)

    def get_state(self) -> int or None:
        return self.state
