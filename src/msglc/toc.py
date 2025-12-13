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

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING

from .config import config

if TYPE_CHECKING:
    from io import BytesIO
    from typing import BinaryIO

    from msgpack import Packer  # type: ignore

try:
    import numpy  # type: ignore

    ndarray = numpy.ndarray  # type: ignore
except ImportError:
    ndarray = list  # type: ignore


@dataclass()
class Node:
    t: dict | list | None
    p: dict | list
    s: bool = False


class TOC:
    def __init__(
        self, *, packer: Packer, buffer: BytesIO | BinaryIO, transform: callable = None
    ):  # type: ignore
        self._buffer: BytesIO | BinaryIO = buffer
        self._packer: Packer = packer
        self._initial_pos = self._buffer.tell()
        self._in_numpy_array: bool = False

        def plain_forward(obj):
            return obj

        self._transform: callable = transform if transform else plain_forward  # type: ignore

    @property
    def _pos(self) -> int:
        return self._buffer.tell() - self._initial_pos

    def _pack(self, obj) -> Node:
        def _pack_bin(_obj: bytes) -> None:
            self._buffer.write(_obj)

        def _pack_obj(_obj) -> None:
            self._buffer.write(self._packer.pack(_obj))

        def _generate(_start: int) -> Node:
            _end = self._pos
            return Node(None, [_start, _end], _end <= _start + config.trivial_size)

        if not isinstance(obj, (dict, list, set, tuple, ndarray)):
            start_pos = self._pos
            _pack_obj(obj)
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
                _pack_obj(obj.dumps())
                return _generate(start_pos)

            obj = obj.tolist()

            current_level_is_numpy_array = True
            self._in_numpy_array = True

        start_pos = self._pos

        obj_toc: dict | list
        all_small_obj: bool
        if isinstance(obj, dict):
            _pack_bin(self._packer.pack_map_header(len(obj)))
            obj_toc = {}
            for k, v in self._transform(obj.items()):  # type: ignore
                _pack_obj(k)
                obj_toc[k] = self._pack(v)
            all_small_obj = all(v.s for v in obj_toc.values())
        elif isinstance(obj, list):
            _pack_bin(self._packer.pack_array_header(len(obj)))

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
                    _pack_obj(v)

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

                    return _resume_flag(Node(None, numpy_groups))

                self._buffer.seek(start_pos)

            obj_toc = [self._pack(v) for v in self._transform(obj)]  # type: ignore
            all_small_obj = all(v.s for v in obj_toc)
        else:
            raise ValueError(f"Expecting dict or list, got {obj.__class__}.")

        if self._pos < start_pos + config.small_obj_optimization_threshold:
            return _resume_flag(_generate(start_pos))

        if all_small_obj:
            if isinstance(obj, dict) or len(obj) == 0:
                return _resume_flag(_generate(start_pos))

            groups: list = []
            accu_list: list = []
            accu_size: int = 0
            for v in obj_toc:
                accu_list.append(v)
                accu_size += v.p[1] - v.p[0]
                if accu_size > config.small_obj_optimization_threshold:
                    groups.append(
                        (len(accu_list), accu_list[0].p[0], accu_list[-1].p[1])
                    )
                    accu_list = []
                    accu_size = 0

            if accu_list:
                groups.append((len(accu_list), accu_list[0].p[0], accu_list[-1].p[1]))

            return _resume_flag(
                Node(None, groups) if len(groups) > 1 else _generate(start_pos)
            )

        return _resume_flag(Node(obj_toc, [start_pos, self._pos]))

    def pack(self, obj) -> dict:
        def _factory(_obj) -> dict:
            return {k: v for k, v in _obj if v and k != "s"}

        return asdict(self._pack(obj), dict_factory=_factory)
