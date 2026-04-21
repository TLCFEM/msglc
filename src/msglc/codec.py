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

import struct
from abc import ABC, abstractmethod
from importlib.util import find_spec
from inspect import isclass
from io import BytesIO

import cbor2
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

    @abstractmethod
    def write_array_header(self, n):
        raise NotImplementedError

    @abstractmethod
    def write_map_header(self, n):
        raise NotImplementedError


class CBORCodec(LazyCodec):
    def encode(self, data):
        return cbor2.dumps(data)

    def decode(self, data):
        return cbor2.loads(data)

    def stream_decode(self, data):
        decoder = cbor2.CBORDecoder(BytesIO(data))
        while True:
            try:
                yield decoder.decode()
            except cbor2.CBORDecodeEOF:
                break

    def write_array_header(self, n):
        if n <= 23:
            return struct.pack("B", 0x80 | n)
        elif n <= 0xFF:
            return struct.pack(">BB", 0x98, n)
        elif n <= 0xFFFF:
            return struct.pack(">BH", 0x99, n)
        elif n <= 0xFFFFFFFF:
            return struct.pack(">BI", 0x9A, n)
        elif n <= 0xFFFFFFFFFFFFFFFF:
            return struct.pack(">BQ", 0x9B, n)
        else:
            raise ValueError("Array is too large")

    def write_map_header(self, n):
        if n <= 23:
            return struct.pack("B", 0xA0 | n)
        elif n <= 0xFF:
            return struct.pack(">BB", 0xB8, n)
        elif n <= 0xFFFF:
            return struct.pack(">BH", 0xB9, n)
        elif n <= 0xFFFFFFFF:
            return struct.pack(">BI", 0xBA, n)
        elif n <= 0xFFFFFFFFFFFFFFFF:
            return struct.pack(">BQ", 0xBB, n)
        else:
            raise ValueError("Dict is too large")


class MsgpackCodecBase(LazyCodec, ABC):
    def stream_decode(self, data):
        yield from msgpack.Unpacker(BytesIO(data))

    def write_array_header(self, n):
        if n <= 0x0F:
            return struct.pack("B", 0x90 + n)
        elif n <= 0xFFFF:
            return struct.pack(">BH", 0xDC, n)
        elif n <= 0xFFFFFFFF:
            return struct.pack(">BI", 0xDD, n)
        else:
            raise ValueError("Array is too large")

    def write_map_header(self, n):
        if n <= 0x0F:
            return struct.pack("B", 0x80 + n)
        elif n <= 0xFFFF:
            return struct.pack(">BH", 0xDE, n)
        elif n <= 0xFFFFFFFF:
            return struct.pack(">BI", 0xDF, n)
        else:
            raise ValueError("Dict is too large")


class MsgpackCodec(MsgpackCodecBase):
    def __init__(self):
        self._packer = msgpack.Packer()
        self._unpacker = msgpack.Unpacker()

    def encode(self, data):
        return self._packer.pack(data)

    def decode(self, data):
        self._unpacker.feed(data)
        return self._unpacker.unpack()


if find_spec("msgspec"):
    import msgspec

    class MsgspecCodec(MsgpackCodecBase):
        def __init__(self):
            self._packer = msgspec.msgpack.Encoder()
            self._unpacker = msgspec.msgpack.Decoder()

        def encode(self, data):
            return self._packer.encode(data)

        def decode(self, data):
            return self._unpacker.decode(data)
else:
    MsgspecCodec = MsgpackCodec


if find_spec("ormsgpack"):
    import ormsgpack

    class OrmsgpackCodec(MsgpackCodecBase):
        def encode(self, data):
            return ormsgpack.packb(data)

        def decode(self, data):
            return ormsgpack.unpackb(data)
else:
    OrmsgpackCodec = MsgpackCodec


def acquire_codec(codec: type[LazyCodec] | LazyCodec | None) -> LazyCodec:
    if isinstance(codec, LazyCodec):
        return codec

    if isclass(codec) and issubclass(codec, LazyCodec):
        return codec()

    if codec is None:
        return MsgspecCodec()

    raise TypeError("Need a valid codec.")
