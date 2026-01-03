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

from time import sleep
from typing import TYPE_CHECKING

from upath import UPath

if TYPE_CHECKING:
    from io import BytesIO


class MockIO:
    def __init__(
        self,
        path: str | UPath | BytesIO,
        mode: str,
        seek_delay: float = 0,
        read_speed: int | list = 1 * 2**20,
        fs=None,
    ):
        self._path = path if isinstance(path, (str, UPath)) else None
        if isinstance(path, str):
            if fs:
                self._io = fs.open(path, mode)
            else:
                self._io = open(path, mode)  # noqa: SIM115
        elif isinstance(path, UPath):
            self._io = path.open(mode)
        else:
            self._io = path
        self._seek_delay: float = seek_delay
        self._read_speed: int | list = read_speed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _actual_speed(self, size):
        if isinstance(self._read_speed, int):
            return self._read_speed

        for min_size, speed in self._read_speed:
            if size < min_size:
                return speed

        return self._read_speed[-1][1]

    def tell(self):
        return self._io.tell()

    def seek(self, offset: int):
        sleep(self._seek_delay)
        self._io.seek(offset)

    def read(self, size: int):
        sleep(size / self._actual_speed(size))
        return self._io.read(size)

    def close(self):
        if self._path is not None:
            self._io.close()

    @property
    def closed(self):
        return self._io.closed
