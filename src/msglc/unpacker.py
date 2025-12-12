#  Copyright (C) 2024-2025 Theodore Chang
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.

from abc import ABC, abstractmethod

import msgpack


class Unpacker(ABC):
    @abstractmethod
    def decode(self, data):
        raise NotImplementedError


class MsgpackUnpacker(Unpacker):
    def __init__(self):
        self._unpacker = msgpack.Unpacker()

    def decode(self, data):
        self._unpacker.feed(data)
        return self._unpacker.unpack()


try:
    import msgspec

    class MsgspecUnpacker(Unpacker):
        def __init__(self):
            self._unpacker = msgspec.msgpack.Decoder()

        def decode(self, data):
            return self._unpacker.decode(data)
except ImportError:
    MsgspecUnpacker = MsgpackUnpacker

try:
    import ormsgpack

    class OrmsgpackUnpacker(Unpacker):
        def decode(self, data):
            return ormsgpack.unpackb(data)

except ImportError:
    OrmsgpackUnpacker = MsgpackUnpacker
