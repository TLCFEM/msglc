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
from contextlib import nullcontext
from typing import TYPE_CHECKING

from .config import config
from .writer import LazyCombiner, LazyWriter

if TYPE_CHECKING:
    from io import BytesIO
    from typing import BinaryIO, Literal

    from .config import S3FS


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


class FileInfo:
    """
    Wrap the file path or in memory buffer and name into a FileInfo object.
    The `name` is optional and is only used when the file is combined in the dictionary (key-value) mode.

    The `s3fs` can be different for each `FileInfo` object, meaning it is possible to combine files from different sources.
    It is not affected by the global `s3fs` object stored in `config`.

    :param path: a string representing the file path or an in memory buffer
    :param name: key name of the content in the combined dict
    :param s3fs: s3fs object (s3fs.S3FileSystem) to read the object from
    """

    def __init__(
        self,
        path: str | BinaryIO,
        name: str | None = None,
        *,
        s3fs: S3FS | None = None,
    ):
        self.path = path
        self.name = name
        self._s3fs: S3FS | None = s3fs

    def exists(self):
        if not isinstance(self.path, str):
            return True

        if self._s3fs:
            return self._s3fs.exists(self.path)

        return os.path.exists(self.path)

    def open(self):
        if not isinstance(self.path, str):
            return nullcontext(self.path)

        return self._s3fs.open(self.path) if self._s3fs else open(self.path, "rb")


def combine(
    archive: str | BytesIO,
    files: FileInfo | list[FileInfo],
    *,
    mode: Literal["a", "w"] = "w",
    validate: bool = True,
    s3fs: S3FS | None = None,
):
    """
    This function is used to combine the multiple serialized files into a single archive.
    If `s3fs` is given, the combined archive will be uploaded to S3.

    The files to be combined must exist in local filesystem regardless of whether `s3fs` is given.
    In other words, only local files can be combined.

    :param archive: a string representing the file path of the archive
    :param files: a list of FileInfo objects
    :param mode: a string representing the combination mode, 'w' for write and 'a' for append
    :param validate: switch on to validate the files before combining
    :param s3fs: s3fs object (s3fs.S3FileSystem) to be used for storing
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

    def _validate(_fp: FileInfo):
        if isinstance(_fp.path, str):
            if not _fp.exists():
                raise ValueError(f"File {_fp.path} does not exist.")
            with _fp.open() as _file:
                if _file.read(LazyWriter.magic_len()) != LazyWriter.magic:
                    raise ValueError(f"Invalid file format: {_fp.path}.")
        else:
            with _fp.open() as _file:
                ini_pos = _file.tell()
                magic = _file.read(LazyWriter.magic_len())
                _file.seek(ini_pos)
                if magic != LazyWriter.magic:
                    raise ValueError("Invalid file format.")

    if validate:
        for file in files:
            _validate(file)

    def _iter(_fp: FileInfo):
        with _fp.open() as _file:
            while _data := _file.read(config.copy_chunk_size):
                yield _data

    with LazyCombiner(archive, mode=mode, s3fs=s3fs) as combiner:
        for file in files:
            combiner.write(_iter(file), file.name)


def append(
    archive: str | BytesIO,
    files: FileInfo | list[FileInfo],
    *,
    validate: bool = True,
    s3fs: S3FS | None = None,
):
    """
    This function is used to append the multiple serialized files to an existing single archive.
    If `s3fs` is given, the target will be downloaded first if it exists in the bucket.
    The final archive will be uploaded to S3.

    The files to be appended must exist in local filesystem regardless of whether `s3fs` is given.

    :param archive: a string representing the file path of the archive
    :param files: a list of FileInfo objects
    :param validate: switch on to validate the files before combining
    :param s3fs: s3fs object (s3fs.S3FileSystem) to be used for storing
    :return: None
    """
    combine(archive, files, mode="a", validate=validate, s3fs=s3fs)
