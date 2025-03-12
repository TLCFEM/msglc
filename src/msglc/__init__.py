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

import dataclasses
import os.path
from io import BytesIO
from typing import BinaryIO, Literal

from .config import config
from .writer import LazyCombiner, LazyWriter


def dump(file: str | BytesIO, obj, **kwargs):
    """
    This function is used to write the object to the file.

    :param file: a string representing the file path
    :param obj: the object to be written to the file
    :param kwargs: additional keyword arguments to be passed to the `LazyWriter`
    :return: None
    """
    with LazyWriter(file, **kwargs) as msglc_writer:
        msglc_writer.write(obj)


@dataclasses.dataclass
class FileInfo:
    """
    Wrap the file path or in memory buffer and name into a FileInfo object.
    The name is optional and is only used when the file is combined in the dictionary (key-value) mode.
    """

    path: str | BinaryIO
    name: str | None = None


def combine(
    archive: str | BytesIO,
    files: FileInfo | list[FileInfo],
    *,
    mode: Literal["a", "w"] = "w",
    validate: bool = True,
):
    """
    This function is used to combine the multiple serialized files into a single archive.

    :param archive: a string representing the file path of the archive
    :param files: a list of FileInfo objects
    :param mode: a string representing the combination mode, 'w' for write and 'a' for append
    :param validate: switch on to validate the files before combining
    :return: None
    """
    if isinstance(files, FileInfo):
        files = [files]

    if 0 < sum(1 for file in files if file.name is not None) < len(files):
        raise ValueError("Files must either all have names or all not have names.")

    if len(all_names := {file.name for file in files}) != len(files) and (
        len(all_names) != 1 or all_names.pop() is not None
    ):
        raise ValueError("Files must have unique names.")

    def _validate(_fp):
        if isinstance(_fp, str):
            if not os.path.exists(_fp):
                raise ValueError(f"File {_fp} does not exist.")
            with open(_fp, "rb") as _file:
                if _file.read(LazyWriter.magic_len()) != LazyWriter.magic:
                    raise ValueError(f"Invalid file format: {_fp}.")
        else:
            ini_pos = _fp.tell()
            magic = _fp.read(LazyWriter.magic_len())
            _fp.seek(ini_pos)
            if magic != LazyWriter.magic:
                raise ValueError("Invalid file format.")

    if validate:
        for file in files:
            _validate(file.path)

    def _iter(path: str | BinaryIO):
        if isinstance(path, str):
            with open(path, "rb") as _file:
                while _data := _file.read(config.copy_chunk_size):
                    yield _data
        else:
            while _data := path.read(config.copy_chunk_size):
                yield _data

    with LazyCombiner(archive, mode=mode) as combiner:
        for file in files:
            combiner.write(_iter(file.path), file.name)


def append(
    archive: str | BytesIO, files: FileInfo | list[FileInfo], *, validate: bool = True
):
    """
    This function is used to append the multiple serialized files to an existing single archive.

    :param archive: a string representing the file path of the archive
    :param files: a list of FileInfo objects
    :param validate: switch on to validate the files before combining
    :return: None
    """
    combine(archive, files, mode="a", validate=validate)
