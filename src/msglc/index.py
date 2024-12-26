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

from functools import lru_cache


@lru_cache(maxsize=2**14)
def _is_index(key: str) -> int | None:
    try:
        return int(key)
    except ValueError:
        return None


@lru_cache(maxsize=2**14)
def normalise_index(index: int, total_size: int) -> int:
    while index < -total_size:
        index += total_size
    while index >= total_size:
        index -= total_size
    return index


@lru_cache(maxsize=2**14)
def _normalise_bound(index: int, total_size: int) -> int:
    while index < 1 - total_size:
        index += total_size
    while index >= 1 + total_size:
        index -= total_size
    return index


@lru_cache(maxsize=2**14)
def _is_slice(key: str, total_size: int) -> tuple | None:
    if ":" not in key:
        return None

    parts: list = list(key.split(":"))

    if len(parts) == 2:
        start = 0 if parts[0] == "" else _is_index(parts[0])
        stop = total_size if parts[1] == "" else _is_index(parts[1])

        if start is not None and stop is not None:
            start = normalise_index(start, total_size)
            stop = _normalise_bound(stop, total_size)
            return start, stop, 1

        return None

    if len(parts) == 3:
        start = 0 if parts[0] == "" else _is_index(parts[0])
        step = 1 if parts[1] == "" else _is_index(parts[1])
        stop = total_size if parts[2] == "" else _is_index(parts[2])

        if start is not None and step is not None and stop is not None:
            start = normalise_index(start, total_size)
            stop = _normalise_bound(stop, total_size)
            return start, stop, step

        return None

    return None


@lru_cache(maxsize=2**14)
def to_index(key: str, total_size: int):
    if (int_key := _is_index(key)) is not None:
        return int_key

    if slicing := _is_slice(key, total_size):
        return slice(*slicing)

    return key
