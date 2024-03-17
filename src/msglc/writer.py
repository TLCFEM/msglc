#  Copyright (C) 2024 Theodore Chang
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

from __future__ import annotations

from io import BytesIO, BufferedReader
from typing import Generator

from msgpack import Packer, packb  # type: ignore

from .config import config, increment_gc_counter, decrement_gc_counter, BufferWriter, max_magic_len
from .toc import TOC


class LazyWriter:
    magic: bytes = b"msglc-2024".rjust(max_magic_len, b"\0")

    @classmethod
    def magic_len(cls) -> int:
        return len(cls.magic)

    @classmethod
    def set_magic(cls, magic: bytes):
        cls.magic = magic.rjust(max_magic_len, b"\0")

    def __init__(self, buffer_or_path: str | BufferWriter, packer: Packer = None):
        self._buffer_or_path: str | BufferWriter = buffer_or_path
        self._packer = packer if packer else Packer()

        self._buffer: BufferWriter = None  # type: ignore
        self._toc_packer: TOC = None  # type: ignore
        self._header_start: int = 0
        self._file_start: int = 0
        self._no_more_writes: bool = False

    def __enter__(self):
        increment_gc_counter()

        if isinstance(self._buffer_or_path, str):
            self._buffer = open(self._buffer_or_path, "wb", buffering=config.write_buffer_size)
        elif isinstance(self._buffer_or_path, (BytesIO, BufferedReader)):
            self._buffer = self._buffer_or_path
        else:
            raise ValueError("Expecting a buffer or path.")

        self._buffer.write(self.magic)
        self._header_start = self._buffer.tell()
        self._buffer.write(b"\0" * 20)
        self._file_start = self._buffer.tell()

        self._toc_packer = TOC(packer=self._packer, buffer=self._buffer)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        decrement_gc_counter()

        if isinstance(self._buffer_or_path, str):
            self._buffer.close()

    def write(self, obj) -> None:
        """
        This function is used to write the object to the file.

        Only one write is allowed. The function raises a ValueError if it is called more than once.

        :param obj: the object to be written to the file
        :return: None
        """
        if self._no_more_writes:
            raise ValueError("No more writes allowed.")

        self._no_more_writes = True

        toc: dict = self._toc_packer.pack(obj)
        toc_start: int = self._buffer.tell() - self._file_start
        packed_toc: bytes = self._packer.pack(toc)

        self._buffer.write(packed_toc)
        self._buffer.seek(self._header_start)
        self._buffer.write(self._packer.pack(toc_start).rjust(10, b"\0"))
        self._buffer.write(self._packer.pack(len(packed_toc)).rjust(10, b"\0"))


class LazyCombiner:
    def __init__(self, buffer_or_path: str | BufferWriter):
        self._buffer_or_path: str | BufferWriter = buffer_or_path

        self._buffer: BufferWriter = None  # type: ignore

        self._toc: dict | list = None  # type: ignore
        self._header_start: int = 0
        self._file_start: int = 0

    def __enter__(self):
        if isinstance(self._buffer_or_path, str):
            self._buffer = open(self._buffer_or_path, "wb", buffering=config.write_buffer_size)
        elif isinstance(self._buffer_or_path, (BytesIO, BufferedReader)):
            self._buffer = self._buffer_or_path
        else:
            raise ValueError("Expecting a buffer or path.")

        self._buffer.write(LazyWriter.magic)
        self._header_start = self._buffer.tell()
        self._buffer.write(b"\0" * 20)
        self._file_start = self._buffer.tell()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        toc_start: int = self._buffer.tell() - self._file_start
        packed_toc: bytes = packb({"t": self._toc})

        self._buffer.write(packed_toc)
        self._buffer.seek(self._header_start)
        self._buffer.write(packb(toc_start).rjust(10, b"\0"))
        self._buffer.write(packb(len(packed_toc)).rjust(10, b"\0"))

        if isinstance(self._buffer_or_path, str):
            self._buffer.close()

    def write(self, obj: Generator, name: str | None = None) -> None:
        if self._toc is None:
            self._toc = [] if name is None else {}

        if name is not None and name in self._toc:
            raise ValueError(f"File {name} already exists.")

        start: int = self._buffer.tell() - self._file_start
        for chunk in obj:
            self._buffer.write(chunk)

        if name is None:
            assert isinstance(self._toc, list)
            self._toc.append(start)
        else:
            assert isinstance(self._toc, dict)
            self._toc[name] = start
