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

import gc
from dataclasses import dataclass
from importlib.util import find_spec
from io import BufferedReader, BytesIO
from typing import BinaryIO, Union

from msglc.utility import MockIO

BufferWriter = Union[BinaryIO, BytesIO, BufferedReader]
BufferWriterType = (BinaryIO, BytesIO, BufferedReader)

BufferReader = Union[BufferWriter, MockIO]
BufferReaderType = BufferWriterType + (MockIO,)


if find_spec("s3fs"):
    from fsspec.spec import AbstractBufferedFile
    from s3fs import S3FileSystem

    BufferReader = Union[BufferReader, AbstractBufferedFile]
    BufferReaderType = BufferReaderType + (AbstractBufferedFile,)

    S3FS = S3FileSystem
else:
    from types import NoneType

    S3FS = NoneType


@dataclass
class Config:
    small_obj_optimization_threshold: int = 2**13  # 8KB
    write_buffer_size: int = 2**23  # 8MB
    read_buffer_size: int = 2**16  # 64KB
    fast_loading: bool = True
    fast_loading_threshold: float = 0.3
    trivial_size: int = 20
    disable_gc: bool = True
    simple_repr: bool = True
    copy_chunk_size: int = 2**24  # 16MB
    numpy_encoder: bool = False
    numpy_fast_int_pack: bool = False
    s3fs: S3FS | None = None


config = Config()


max_magic_len: int = 30


def configure(
    *,
    small_obj_optimization_threshold: int | None = None,
    write_buffer_size: int | None = None,
    read_buffer_size: int | None = None,
    fast_loading: bool | None = None,
    fast_loading_threshold: int | float | None = None,
    trivial_size: int | None = None,
    disable_gc: bool | None = None,
    simple_repr: bool | None = None,
    copy_chunk_size: int | None = None,
    numpy_encoder: bool | None = None,
    numpy_fast_int_pack: bool | None = None,
    magic: bytes | None = None,
    s3fs: S3FS | None = None,
):
    """
    This function is used to configure the settings. It accepts any number of keyword arguments.
    The function updates the values of the configuration parameters if they are provided in the arguments.

    :param small_obj_optimization_threshold:
            The threshold (in bytes) for small object optimization.
            Objects smaller than this threshold are not indexed.
    :param write_buffer_size:
            The size (in bytes) for the write buffer.
    :param read_buffer_size:
            The size (in bytes) for the read buffer.
    :param fast_loading:
            Flag to enable or disable fast loading.
            If enabled, the container will be read in one go, instead of reading each child separately.
    :param fast_loading_threshold:
            The threshold (0 to 1) for fast loading.
            With the fast loading flag turned on, fast loading will be performed if the number of
            already read children over the total number of children is smaller than this threshold.
    :param trivial_size:
            The size (in bytes) considered trivial, around a dozen bytes.
            Objects smaller than this size are considered trivial.
            For a list of trivial objects, the container will be indexed in a blocked fashion.
    :param disable_gc:
            Flag to enable or disable garbage collection.
    :param simple_repr:
            Flag to enable or disable simple representation used in the __repr__ method.
            If turned on, __repr__ will not incur any disk I/O.
    :param copy_chunk_size:
            The size (in bytes) for the copy chunk.
    :param numpy_encoder:
            Flag to enable or disable the `numpy` support.
            If enabled, the `numpy` arrays will be encoded using the `dumps` method provided by `numpy`.
            The arrays are stored as binary data directly.
            If disabled, the `numpy` arrays will be converted to lists before encoding.
    :param numpy_fast_int_pack:
            If enabled, the integer numpy array will be packed assigning each element has identical size (4 or 8 bytes).
            This improves the performance of packing by avoiding the overhead of checking the size of each element.
            However, depending on the backend, for example, `messagepack` C implementation packs unsigned long long or long long.
            But its python implementation packs integer of various lengths (1, 2, 3, 5, 9 bytes).
    :param magic:
            Magic bytes (max length: 30) to set, used to identify the file format version.
    :param s3fs:
            The global `S3FileSystem` object that will be used by default so that there is no need to provide this for every function call.
            It is used to 1) read data by readers, 2) write output by writers/combiners.
            To specify where combiners read input files from, assign a specific `S3FileSystem` object to each `FileInfo`.
    """
    if (
        isinstance(small_obj_optimization_threshold, int)
        and small_obj_optimization_threshold > 0
    ):
        config.small_obj_optimization_threshold = small_obj_optimization_threshold
        if config.trivial_size > config.small_obj_optimization_threshold:
            config.trivial_size = config.small_obj_optimization_threshold

    if isinstance(write_buffer_size, int) and write_buffer_size > 0:
        config.write_buffer_size = write_buffer_size

    if isinstance(read_buffer_size, int) and read_buffer_size > 0:
        config.read_buffer_size = read_buffer_size

    if isinstance(fast_loading, bool):
        config.fast_loading = fast_loading

    if (
        isinstance(fast_loading_threshold, (int, float))
        and 0 <= fast_loading_threshold <= 1
    ):
        config.fast_loading_threshold = fast_loading_threshold

    if isinstance(trivial_size, int) and trivial_size > 0:
        config.trivial_size = trivial_size
        if config.trivial_size > config.small_obj_optimization_threshold:
            config.small_obj_optimization_threshold = config.trivial_size

    if isinstance(disable_gc, bool):
        config.disable_gc = disable_gc

    if isinstance(simple_repr, bool):
        config.simple_repr = simple_repr

    if isinstance(copy_chunk_size, int) and copy_chunk_size > 0:
        config.copy_chunk_size = copy_chunk_size

    if isinstance(numpy_encoder, bool):
        config.numpy_encoder = numpy_encoder

    if isinstance(numpy_fast_int_pack, bool):
        config.numpy_fast_int_pack = numpy_fast_int_pack

    config.s3fs = s3fs

    if isinstance(magic, bytes) and 0 < len(magic) <= max_magic_len:
        from msglc import LazyWriter

        LazyWriter.set_magic(magic)


__gc_counter: int = 0


def increment_gc_counter():
    global __gc_counter
    if config.disable_gc:
        __gc_counter += 1
        gc.disable()
    return __gc_counter


def decrement_gc_counter():
    global __gc_counter
    if config.disable_gc:
        __gc_counter -= 1
        if __gc_counter == 0:
            gc.enable()
    return __gc_counter
