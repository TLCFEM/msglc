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

from functools import lru_cache


@lru_cache(maxsize=2**14)
def is_index(key: int | str | tuple):
    if isinstance(key, int):
        return True

    if isinstance(key, tuple):
        return all(is_index(k) for k in key)

    if key.isdigit():
        return True

    if len(key) > 1 and key[0] == "-" and key[1:].isdigit():
        return True

    return False


@lru_cache(maxsize=2**14)
def normalise_index(index: int | str, total_size: int):
    if isinstance(index, str):
        index = int(index)

    while index < -total_size:
        index += total_size
    while index >= total_size:
        index -= total_size
    return index


@lru_cache(maxsize=2**14)
def normalise_bound(index: int | str, total_size: int):
    if isinstance(index, str):
        index = int(index)

    while index < 1 - total_size:
        index += total_size
    while index >= 1 + total_size:
        index -= total_size
    return index


@lru_cache(maxsize=2**14)
def is_slice(key: str, total_size: int):
    parts: list = list(key.split(":"))

    if len(parts) == 2:
        start, stop = parts
        if start == "":
            parts[0] = 0
        if stop == "":
            parts[1] = total_size

        if is_index(tuple(parts)):
            start, stop = normalise_index(parts[0], total_size), normalise_bound(parts[1], total_size)
            return start, stop, 1

        return None

    if len(parts) == 3:
        start, step, stop = parts
        if start == "":
            parts[0] = 0
        if stop == "":
            parts[2] = total_size

        if is_index(tuple(parts)):
            start, stop = normalise_index(parts[0], total_size), normalise_bound(parts[2], total_size)
            return start, stop, int(step)

        return None

    return None
