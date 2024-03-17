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

import gc
from dataclasses import dataclass
from io import BytesIO, BufferedReader
from typing import Union, BinaryIO

from msglc.utility import MockIO

BufferWriter = Union[BinaryIO, BytesIO, BufferedReader]
BufferReader = Union[BufferWriter, MockIO]


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
    magic: bytes | None = None,
):
    """
    This function is used to configure the settings. It accepts any number of keyword arguments.
    The function updates the values of the configuration parameters if they are provided in the arguments.
    """
    if isinstance(small_obj_optimization_threshold, int) and small_obj_optimization_threshold > 0:
        config.small_obj_optimization_threshold = small_obj_optimization_threshold
        if config.trivial_size > config.small_obj_optimization_threshold:
            config.trivial_size = config.small_obj_optimization_threshold

    if isinstance(write_buffer_size, int) and write_buffer_size > 0:
        config.write_buffer_size = write_buffer_size

    if isinstance(read_buffer_size, int) and read_buffer_size > 0:
        config.read_buffer_size = read_buffer_size

    if isinstance(fast_loading, bool):
        config.fast_loading = fast_loading

    if isinstance(fast_loading_threshold, (int, float)) and 0 <= fast_loading_threshold <= 1:
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
