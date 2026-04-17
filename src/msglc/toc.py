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

import struct
from collections.abc import Mapping
from typing import TYPE_CHECKING

from .config import config

if TYPE_CHECKING:
    from collections.abc import Callable
    from tempfile import TemporaryFile

    from msgpack import Packer

    from .config import BufferWriter

try:
    import numpy  # type: ignore

    ndarray = numpy.ndarray  # type: ignore
except ImportError:
    ndarray = list  # type: ignore


def _group_into_batches(
    starts: list[int], ends: list[int], threshold: int
) -> list[tuple[int, int, int]]:
    groups: list[tuple[int, int, int]] = []

    group_size: int = 0
    group_start: int = starts[0]
    group_end: int = ends[0]

    for i in range(len(starts)):
        group_size += 1
        group_end = ends[i]
        if group_end >= group_start + threshold:
            groups.append((group_size, group_start, group_end))
            group_start = group_end
            group_size = 0

    if group_size:
        groups.append((group_size, group_start, group_end))

    return groups


class TOC:
    def __init__(
        self,
        *,
        packer: Packer,
        buffer: BufferWriter | TemporaryFile,
        transform: Callable = None,
    ):
        self._buffer: BufferWriter | TemporaryFile = buffer
        self._packer: Packer = packer
        self._initial_pos = self._buffer.tell()
        self._pos: int = 0
        self._in_numpy_array: bool = False

        def plain_forward(obj):
            return obj

        self._transform: Callable = transform or plain_forward

    def _write_array_header(self, n):
        if n <= 0x0F:
            self._writeb(struct.pack("B", 0x90 + n))
        elif n <= 0xFFFF:
            self._writeb(struct.pack(">BH", 0xDC, n))
        elif n <= 0xFFFFFFFF:
            self._writeb(struct.pack(">BI", 0xDD, n))
        else:
            raise ValueError("Array is too large")

    def _write_map_header(self, n):
        if n <= 0x0F:
            self._writeb(struct.pack("B", 0x80 + n))
        elif n <= 0xFFFF:
            self._writeb(struct.pack(">BH", 0xDE, n))
        elif n <= 0xFFFFFFFF:
            self._writeb(struct.pack(">BI", 0xDF, n))
        else:
            raise ValueError("Dict is too large")

    # noinspection SpellCheckingInspection
    def _writeb(self, data: bytes):
        self._buffer.write(data)
        self._pos += len(data)

    def _pack(self, obj) -> tuple:
        def _generate(_start: int) -> tuple:
            return None, [_start, self._pos], self._pos <= _start + config.trivial_size

        if not isinstance(obj, (Mapping, list, set, tuple, ndarray)):
            start_pos = self._pos
            self._writeb(self._packer.pack(obj))
            return _generate(start_pos)

        current_level_is_numpy_array: bool = False

        def _resume_flag(output):
            if current_level_is_numpy_array:
                self._in_numpy_array = False
            return output

        if isinstance(obj, tuple):
            obj = list(obj)
        elif isinstance(obj, set):
            obj = sorted(obj)
        elif ndarray is not list and isinstance(obj, ndarray):
            if config.numpy_encoder:
                start_pos = self._pos
                self._writeb(self._packer.pack(obj.dumps()))
                return _generate(start_pos)

            obj = obj.tolist()

            current_level_is_numpy_array = True
            self._in_numpy_array = True

        start_pos = self._pos

        obj_toc: dict | list
        all_small_obj: bool
        if isinstance(obj, Mapping):
            self._write_map_header(len(obj))
            obj_toc = {}
            for k, v in self._transform(obj.items()):
                self._writeb(self._packer.pack(k))
                obj_toc[k] = self._pack(v)
            all_small_obj = all(v[2] for v in obj_toc.values())
        elif isinstance(obj, list):
            self._write_array_header(len(obj))

            if (
                self._in_numpy_array
                and len(obj) > 0
                and (
                    isinstance(obj[0], float)
                    or (config.numpy_fast_int_pack and isinstance(obj[0], int))
                )
            ):
                list_start: int = self._pos

                for v in obj:
                    self._writeb(self._packer.pack(v))

                if self._pos < start_pos + config.small_obj_optimization_threshold:
                    return _resume_flag(_generate(start_pos))

                # assuming homogeneous list
                # compute the groups using a cheaper method
                total_items: int = len(obj)
                item_size: int = (self._pos - list_start) // total_items
                if item_size * total_items == self._pos - list_start:
                    group_size: int = min(
                        total_items,
                        config.small_obj_optimization_threshold // item_size + 1,
                    )
                    numpy_groups: list = []
                    current_pos: int = list_start
                    while total_items != 0:
                        current_block: int = min(group_size, total_items)
                        numpy_groups.append(
                            (
                                current_block,
                                current_pos,
                                current_pos + current_block * item_size,
                            )
                        )
                        current_pos += current_block * item_size
                        total_items -= current_block

                    assert current_pos == self._pos

                    return _resume_flag((None, numpy_groups, False))

                self._buffer.seek(start_pos)
                self._pos = start_pos - self._initial_pos

            obj_toc = [self._pack(v) for v in self._transform(obj)]
            all_small_obj = all(v[2] for v in obj_toc)
        else:
            raise ValueError(f"Expecting dict or list, got {obj.__class__}.")

        if self._pos < start_pos + config.small_obj_optimization_threshold:
            return _resume_flag(_generate(start_pos))

        if not all_small_obj:
            return _resume_flag((obj_toc, [start_pos, self._pos], False))

        if isinstance(obj, Mapping) or len(obj) == 0:
            return _resume_flag(_generate(start_pos))

        groups = _group_into_batches(
            [v[1][0] for v in obj_toc],
            [v[1][1] for v in obj_toc],
            config.small_obj_optimization_threshold,
        )

        return _resume_flag(
            (None, groups, False) if len(groups) > 1 else _generate(start_pos)
        )

    def pack(self, obj) -> dict:
        def _serialize(tree):
            t, p, _ = tree
            result = {"p": p}
            if isinstance(t, list):
                result["t"] = [_serialize(v) for v in t]
            elif isinstance(t, dict):
                result["t"] = {k: _serialize(v) for k, v in t.items()}
            return result

        return _serialize(self._pack(obj))
