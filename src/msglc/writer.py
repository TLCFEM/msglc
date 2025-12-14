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

from __future__ import annotations

import os.path
from io import BufferedReader, BytesIO
from tempfile import TemporaryFile
from typing import TYPE_CHECKING

from msgpack import Packer, packb, unpackb  # type: ignore

from .config import (
    config,
    decrement_gc_counter,
    increment_gc_counter,
    max_magic_len,
)
from .toc import TOC

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Literal

    from .config import S3FS, BufferReader, BufferWriter


def _upsert(source: BufferReader, target: str, fs):
    if not fs:
        return

    with fs.open(target, "wb", block_size=config.write_buffer_size) as s3_file:
        # now transfer the local cache to s3
        source.seek(0)
        while chunk := source.read(config.write_buffer_size):
            s3_file.write(chunk)


class LazyWriter:
    magic: bytes = b"msglc-2024".rjust(max_magic_len, b"\0")

    @classmethod
    def magic_len(cls) -> int:
        return len(cls.magic)

    @classmethod
    def set_magic(cls, magic: bytes):
        cls.magic = magic.rjust(max_magic_len, b"\0")

    def __init__(
        self,
        buffer_or_path: str | BufferWriter,
        packer: Packer = None,
        *,
        s3fs: S3FS | None = None,
    ):
        """
        It is possible to provide a custom packer object to be used for packing the object.
        However, this packer must be compatible with the `msgpack` packer.

        :param buffer_or_path: target buffer or file path
        :param packer: packer object to be used for packing the object
        :param s3fs: s3fs object (s3fs.S3FileSystem) to be used for storing
        """
        self._buffer_or_path: str | BufferWriter = buffer_or_path
        self._packer = packer if packer else Packer()
        self._s3fs: S3FS | None = s3fs or config.s3fs

        self._buffer: BufferWriter | TemporaryFile = None  # type: ignore
        self._toc_packer: TOC = None  # type: ignore
        self._header_start: int = 0
        self._file_start: int = 0
        self._no_more_writes: bool = False

    def __enter__(self):
        increment_gc_counter()

        if isinstance(self._buffer_or_path, str):
            if self._s3fs:
                # we need to seek to the beginning and overwrite the header
                # however, s3 does not allow seek in write mode
                # thus use a local temp file as cache
                self._buffer = TemporaryFile()
            else:
                self._buffer = open(
                    self._buffer_or_path, "wb", buffering=config.write_buffer_size
                )
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

        if not isinstance(self._buffer_or_path, str):
            return

        _upsert(self._buffer, self._buffer_or_path, self._s3fs)

        self._buffer.close()

    def write(self, obj) -> None:
        """
        This function is used to write the object to the file.

        Only one write is allowed. The function raises a `ValueError` if it is called more than once.

        :param obj: the object to be written to the file
        :raise ValueError: if the function is called more than once
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
    def __init__(
        self,
        buffer_or_path: str | BufferWriter,
        *,
        mode: Literal["a", "w"] = "w",
        s3fs: S3FS | None = None,
    ):
        """
        The mode resembles typical mode designations and implies the same meaning.
        If the mode is 'w', the file is overwritten.
        If the mode is 'a', the file is appended.

        :param buffer_or_path: target buffer or file path
        :param mode: mode of operation, 'w' for write and 'a' for append
        :param s3fs: s3fs object (s3fs.S3FileSystem) to be used for storing
        """
        self._buffer_or_path: str | BufferWriter = buffer_or_path
        self._mode: str = mode
        self._s3fs: S3FS | None = s3fs or config.s3fs

        self._buffer: BufferWriter | TemporaryFile = None  # type: ignore

        self._toc: dict | list = None  # type: ignore
        self._header_start: int = 0
        self._file_start: int = 0

    def __enter__(self):
        if isinstance(self._buffer_or_path, str):
            if self._s3fs:
                self._buffer = TemporaryFile()
                if self._s3fs.exists(self._buffer_or_path):
                    with self._s3fs.open(self._buffer_or_path, "rb") as s3_file:
                        while chunk := s3_file.read(config.read_buffer_size):
                            self._buffer.write(chunk)
                    self._buffer.seek(0)
            else:
                mode: str = (
                    "wb"
                    if not os.path.exists(self._buffer_or_path) or self._mode == "w"
                    else "r+b"
                )
                self._buffer = open(  # type: ignore
                    self._buffer_or_path, mode, buffering=config.write_buffer_size
                )
        elif isinstance(self._buffer_or_path, (BytesIO, BufferedReader)):
            self._buffer = self._buffer_or_path
            if self._mode == "a":
                # need to read the header anyway
                self._buffer.seek(0)
        else:
            raise ValueError("Expecting a buffer or path.")

        if self._mode == "w":
            self._buffer.write(LazyWriter.magic)
            self._header_start = self._buffer.tell()
            self._buffer.write(b"\0" * 20)
            self._file_start = self._buffer.tell()
        else:
            sep_a, sep_b, sep_c = (
                LazyWriter.magic_len(),
                LazyWriter.magic_len() + 10,
                LazyWriter.magic_len() + 20,
            )

            ini_position: int = self._buffer.tell()
            header: bytes = self._buffer.read(sep_c)

            def _raise_invalid(msg: str):
                self._buffer.seek(ini_position)
                raise ValueError(msg)

            if header[:sep_a] != LazyWriter.magic:
                _raise_invalid(
                    "Invalid file format, cannot append to the current file."
                )

            toc_start: int = unpackb(header[sep_a:sep_b].lstrip(b"\0"))
            toc_size: int = unpackb(header[sep_b:sep_c].lstrip(b"\0"))

            self._buffer.seek(ini_position + sep_c + toc_start)
            self._toc = unpackb(self._buffer.read(toc_size)).get("t", None)

            if isinstance(self._toc, list):
                if any(not isinstance(i, int) for i in self._toc):
                    _raise_invalid("The given file is not a valid combined file.")
            elif isinstance(self._toc, dict):
                if any(not isinstance(i, int) for i in self._toc.values()):
                    _raise_invalid("The given file is not a valid combined file.")
            else:
                _raise_invalid("The given file is not a valid combined file.")

            self._header_start = ini_position + sep_a
            self._file_start = ini_position + sep_c
            self._buffer.seek(ini_position + sep_c + toc_start)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        toc_start: int = self._buffer.tell() - self._file_start
        packed_toc: bytes = packb({"t": self._toc})

        self._buffer.write(packed_toc)
        self._buffer.seek(self._header_start)
        self._buffer.write(packb(toc_start).rjust(10, b"\0"))
        self._buffer.write(packb(len(packed_toc)).rjust(10, b"\0"))

        if not isinstance(self._buffer_or_path, str):
            return

        _upsert(self._buffer, self._buffer_or_path, self._s3fs)

        self._buffer.close()

    def write(self, obj: Generator, name: str | None = None) -> None:
        """
        Write a number of objects to the file.

        :param obj: a generator of objects to be written to the file
        :param name: a name to be assigned to the object, only required when combining in dict mode
        """
        if self._toc is None:
            self._toc = [] if name is None else {}

        if name is None:
            if not isinstance(self._toc, list):
                raise ValueError("Need a name when combining in dict mode.")
        else:
            if not isinstance(self._toc, dict):
                raise ValueError("Cannot assign a name when combining in list mode.")
            if name in self._toc:
                raise ValueError(f"File {name} already exists.")

        start: int = self._buffer.tell() - self._file_start
        for chunk in obj:
            self._buffer.write(chunk)

        if name is None:
            self._toc.append(start)
        else:
            self._toc[name] = start
