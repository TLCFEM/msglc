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

from __future__ import annotations

from collections.abc import Generator
from contextlib import nullcontext
from io import IOBase
from tempfile import TemporaryDirectory, mkstemp
from typing import TYPE_CHECKING, BinaryIO
from uuid import uuid4

from fsspec.implementations.local import LocalFileSystem
from upath import UPath

from .config import config
from .msglc_rust import dump_rust_impl
from .reader import LazyReader, to_obj
from .writer import LazyCombiner, LazyWriter

if TYPE_CHECKING:
    from io import BytesIO
    from typing import Literal

    from .config import FileSystem


def dump(
    file: str | UPath | BytesIO,
    obj,
    *,
    backend: Literal["python", "rust"] = "python",
    **kwargs,
):
    """
    This function is used to write the object to the file.

    :param file: a string representing the file path
    :param obj: the object to be written to the file
    :param backend: the backend to be used for writing, only used when `file` is a `str` or `UPath`:
        'python' for the pure Python implementation;
        'rust' for the Rust implementation.
    :param kwargs: additional keyword arguments to be passed to the `LazyWriter`
    :return: None
    """
    if backend == "python" or not isinstance(file, (str, UPath)):
        with LazyWriter(file, **kwargs) as msglc_writer:
            msglc_writer.write(obj)
        return

    if isinstance(file, UPath):
        target_path = file.path
        target_fs = file.fs
    else:
        target_path = file
        target_fs = kwargs.get("fs", config.fs) or LocalFileSystem()

    if isinstance(target_fs, LocalFileSystem):
        dump_rust_impl(target_path, obj)
        return

    # rust implementation on remote file
    _, tmp_path = mkstemp()
    try:
        dump_rust_impl(tmp_path, obj)
        target_fs.put(tmp_path, target_path)
    except Exception:
        raise
    finally:
        UPath(tmp_path).unlink()


class FileInfo:
    """
    Wrap the file path or in memory buffer and name into a FileInfo object.
    The `name` is optional and is only used when the file is combined in the dictionary (key-value) mode.

    It is possible to optionally pass in an in-memory object (rather than serialized bytes stream) to be serialized.
    In this case, `obj` shall be assigned, and `path` must be `None`.

    The `fs` can be different for each `FileInfo` object, meaning it is possible to combine files from different sources.
    It is not affected by the global `fs` object stored in `config`.

    :param path: a string representing the file path or an in memory buffer
    :param name: key name of the content in the combined dict
    :param fs: `FileSystem` object to read the object from
    :param obj: the object to be written to the file, when `path` is None, `obj` will be serialized.
    """

    def __init__(
        self,
        path: str | UPath | BinaryIO | LazyReader | None,
        name: str | None = None,
        *,
        fs: FileSystem | None = None,
        obj=None,
    ):
        # `path` and `obj` can both be None, but not both assigned at the same time
        if obj is not None:
            assert path is None, "When `obj` is assigned, `path` must be None."
        self.path = path
        self.name = name
        self._fs: FileSystem = fs or LocalFileSystem()
        self._obj = obj

    def _exists(self):
        if isinstance(self.path, str):
            return self._fs.exists(self.path)
        if isinstance(self.path, UPath):
            return self.path.exists()
        return True

    def _open(self):
        if isinstance(self.path, str):
            return self._fs.open(self.path)
        if isinstance(self.path, UPath):
            return self.path.open("rb")
        if isinstance(self.path, IOBase):
            return nullcontext(self.path)
        raise RuntimeError

    def validate(self):
        if isinstance(self.path, (str, UPath)):
            if not self._exists():
                raise ValueError(f"File {self.path} does not exist.")
            with self._open() as _file:
                if not config.check_compatibility(_file.read(LazyWriter.magic_len())):
                    raise ValueError(f"Invalid file format: {self.path}.")
        elif isinstance(self.path, IOBase):
            with self._open() as _file:
                ini_pos = _file.tell()
                magic = _file.read(LazyWriter.magic_len())
                _file.seek(ini_pos)
                if not config.check_compatibility(magic):
                    raise ValueError("Invalid file format.")

    def chunking(self):
        if isinstance(self.path, LazyReader):
            yield from self.path.raw_data()
        elif self.path is None:
            with TemporaryDirectory() as _tmp_dir:
                dump(file_path := UPath(_tmp_dir) / uuid4().hex, to_obj(self._obj))
                with LazyReader(file_path) as _tmp_file:
                    yield from _tmp_file.raw_data()
        else:
            with self._open() as _file:
                while _data := _file.read(config.copy_chunk_size):
                    yield _data


def combine(
    archive: str | UPath | BytesIO,
    files: FileInfo | list[FileInfo] | Generator[FileInfo, None, None],
    *,
    mode: Literal["a", "w"] = "w",
    validate: bool = True,
    fs: FileSystem | None = None,
):
    """
    This function is used to combine the multiple serialized files into a single archive.
    If `fs` is given, the combined archive will be uploaded to the designated file system.

    The files to be combined must exist in local filesystem regardless of whether `fs` is given.
    In other words, only local files can be combined.

    If a generator is passed in as `files`, the validation will be skipped regardless of the `validate` flag.
    It is assumed that the input files are valid.

    :param archive: a string representing the file path of the archive
    :param files: a single `FileInfo` object, a list of `FileInfo` objects, or a generator that yields `FileInfo` objects
    :param mode: a string representing the combination mode, 'w' for write and 'a' for append
    :param validate: switch on to validate the files before combining, ignored if `files` is a generator.
    :param fs: `FileSystem` object to be used for storing
    :return: None
    """
    if isinstance(files, FileInfo):
        files = [files]

    assert isinstance(files, list | Generator)

    if not isinstance(files, Generator):
        if 0 < sum(1 for file in files if file.name is not None) < len(files):
            raise ValueError("Files must either all have names or all not have names.")

        if len(all_names := {file.name for file in files}) != len(files) and (
            len(all_names) != 1 or all_names.pop() is not None
        ):
            raise ValueError("Files must have unique names.")

        if validate:
            for file in files:
                file.validate()

    with LazyCombiner(archive, mode=mode, fs=fs) as combiner:
        for file in files:
            combiner.write(file.chunking(), file.name)


def append(
    archive: str | UPath | BytesIO,
    files: FileInfo | list[FileInfo] | Generator[FileInfo, None, None],
    *,
    validate: bool = True,
    fs: FileSystem | None = None,
):
    """
    This function is used to append the multiple serialized files to an existing single archive.
    If `fs` is given, the target will be downloaded first if it exists in the remote.
    The final archive will be uploaded to remote.

    The files to be appended must exist in local filesystem regardless of whether `fs` is given.

    :param archive: a string representing the file path of the archive
    :param files: a list of FileInfo objects
    :param validate: switch on to validate the files before combining
    :param fs: `FileSystem` object to be used for storing
    :return: None
    """
    combine(archive, files, mode="a", validate=validate, fs=fs)
