#  Copyright (C) 2024-2026 Theodore Chang
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
from importlib.util import find_spec
from io import BytesIO

import msgpack


class LazyCodec(ABC):
    @abstractmethod
    def encode(self, data):
        raise NotImplementedError

    @abstractmethod
    def decode(self, data):
        raise NotImplementedError

    @abstractmethod
    def stream_decode(self, data):
        raise NotImplementedError


class MsgpackCodec(LazyCodec):
    def __init__(self):
        self._packer = msgpack.Packer()
        self._unpacker = msgpack.Unpacker()

    def encode(self, data):
        return self._packer.pack(data)

    def decode(self, data):
        self._unpacker.feed(data)
        return self._unpacker.unpack()

    def stream_decode(self, data):
        yield from msgpack.Unpacker(BytesIO(data))


if find_spec("msgspec"):
    import msgspec

    class MsgspecCodec(LazyCodec):
        def __init__(self):
            self._packer = msgspec.msgpack.Encoder()
            self._unpacker = msgspec.msgpack.Decoder()

        def encode(self, data):
            return self._packer.encode(data)

        def decode(self, data):
            return self._unpacker.decode(data)

        def stream_decode(self, data):
            yield from msgpack.Unpacker(BytesIO(data))
else:
    MsgspecCodec = MsgpackCodec


if find_spec("ormsgpack"):
    import ormsgpack

    class OrmsgpackCodec(LazyCodec):
        def encode(self, data):
            return ormsgpack.packb(data)

        def decode(self, data):
            return ormsgpack.unpackb(data)

        def stream_decode(self, data):
            yield from msgpack.Unpacker(BytesIO(data))
else:
    OrmsgpackCodec = MsgpackCodec
