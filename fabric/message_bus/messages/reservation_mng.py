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


class ReservationMng:
    def __init__(self):
        self.name = self.__class__.__name__
        self.reservation_id = None
        self.slice_id = None
        self.start = None
        self.end = None
        self.requested_end = None
        self.rtype = None
        self.units = None
        self.state = None
        self.pending_state = None
        self.local = None
        self.config = None
        self.request = None
        self.resource = None
        self.notices = None

    def from_dict(self, value: dict):
        self.name = value.get('name', None)
        self.reservation_id = value.get('reservation_id', None)
        self.slice_id = value.get('slice_id', None)
        self.start = value.get('start', None)
        self.end = value.get('end', None)
        self.requested_end = value.get('requested_end', None)
        self.rtype = value.get('rtype', None)
        self.units = value.get('units', None)
        self.state = value.get('state', None)
        self.pending_state = value.get('pending_state', None)
        self.local = value.get('local', None)
        self.config = value.get('config', None)
        self.request = value.get('request', None)
        self.resource = value.get('resource', None)
        self.notices = value.get('notices', None)

    def to_dict(self) -> dict:
        if not self.validate():
            raise Exception("Invalid arguments")

        result = {'name': self.name,
                  'reservation_id': self.reservation_id,
                  'rtype': self.rtype,
                  'notices': self.notices}
        if self.slice_id is not None:
            result['slice_id'] = self.slice_id

        if self.start is not None:
            result['start'] = self.start

        if self.end is not None:
            result['end'] = self.end

        if self.requested_end is not None:
            result['requested_end'] = self.requested_end

        if self.units is not None:
            result['units'] = self.units

        if self.state is not None:
            result['state'] = self.state

        if self.pending_state is not None:
            result['pending_state'] = self.pending_state

        if self.local is not None:
            result['local'] = self.local

        if self.config is not None:
            result['config'] = self.config

        if self.request is not None:
            result['request'] = self.request

        if self.resource is not None:
            result['resource'] = self.resource

        return result

    def print(self):
        print("")
        print("Reservation ID: {} Slice ID: {}".format(self.reservation_id, self.slice_id))
        if self.rtype is not None or self.notices is not None:
            print("Resource Type: {} Notices: {}".format(self.rtype, self.notices))

        if self.start is not None or self.end is not None or self.requested_end is not None:
            print("Start: {} End: {} Requested End: {}".format(self.start, self.end, self.requested_end))

        if self.units is not None or self.state is not None or self.pending_state is not None:
            print("Units: {} State: {} Pending State: {}".format(self.units, self.state, self.pending_state))

        if self.local is not None:
            print("Local Properties: {}".format(self.local))
        if self.config is not None:
            print("Config Properties: {}".format(self.config))
        if self.request is not None:
            print("Request Properties: {}".format(self.request))
        if self.resource is not None:
            print("Resource Properties: {}".format(self.resource))
        print("")

    def __str__(self):
        return "name: {} reservation_id: {} slice_id: {} start: {} end: {} requested_end: {} rtype: {} units: {} " \
               "state: {} pending_state: {} local: {} config: {} request: {} resource: {} notices: {}"\
            .format(self.name, self.reservation_id, self.slice_id, self.start, self.end, self.requested_end,
                    self.rtype, self.units, self.state, self.pending_state, self.local, self.config, self.request,
                    self.resource, self.notices)

    def get_reservation_id(self) -> str:
        return self.reservation_id

    def set_reservation_id(self, value: str):
        self.reservation_id = value

    def get_slice_id(self) -> str:
        return self.slice_id

    def set_slice_id(self, value: str):
        self.slice_id = value

    def get_start(self) -> int:
        return self.start

    def set_start(self, value: int):
        self.start = value

    def get_end(self) -> int:
        return self.end

    def set_end(self, value: int):
        self.end = value

    def get_requested_end(self) -> int:
        return self.requested_end

    def set_requested_end(self, value: int):
        self.requested_end = value

    def get_resource_type(self) -> str:
        return self.rtype

    def set_resource_type(self, value: str):
        self.rtype = value

    def get_units(self) -> int:
        return self.units

    def set_units(self, value: int):
        self.units = value

    def get_state(self) -> int:
        return self.state

    def set_state(self, value: int):
        self.state = value

    def get_pending_state(self) -> int:
        return self.pending_state

    def set_pending_state(self, value: int):
        self.pending_state = value

    def get_local_properties(self) -> dict:
        return self.local

    def set_local_properties(self, value: dict):
        self.local = value

    def get_config_properties(self) -> dict:
        return self.config

    def set_config_properties(self, value: dict):
        self.config = value

    def get_request_properties(self) -> dict:
        return self.request

    def set_request_properties(self, value: dict):
        self.request = value

    def get_resource_properties(self) -> dict:
        return self.resource

    def set_resource_properties(self, value: dict):
        self.resource = value

    def get_notices(self) -> str:
        return self.notices

    def set_notices(self, value: str):
        self.notices = value

    def __eq__(self, other):
        if not isinstance(other, ReservationMng):
            return False

        return self.name == other.name and self.reservation_id == other.reservation_id and \
            self.slice_id == other.slice_id and self.start == other.start and self.end == other.end and \
            self.requested_end == other.requested_end and self.rtype == other.rtype and self.units == other.units and \
            self.state == other.state and self.pending_state == other.pending_state and self.local == other.local and \
            self.request == other.request and self.resource == other.resource and self.notices == other.notices

    def validate(self) -> bool:
        ret_val = True
        if self.reservation_id is None or self.rtype is None or self.notices is None:
            print("reservation_id: {} rtype: {} notices: {}".format(self.reservation_id, self.rtype, self.notices))
            ret_val = False
        return ret_val