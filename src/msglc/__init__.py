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

import dataclasses

from .config import configure, config
from .reader import LazyReader, to_obj
from .writer import LazyWriter, LazyCombiner


def dump(file: str, obj, **kwargs):
    with LazyWriter(file, **kwargs) as msglc_writer:
        msglc_writer.write(obj)


@dataclasses.dataclass
class FileInfo:
    name: str
    path: str


def combine(archive: str, files: list[FileInfo]):
    def _iter(path: str):
        with open(path, "rb") as _file:
            while True:
                _data = _file.read(config.copy_chunk_size)
                if not _data:
                    break
                yield _data

    with LazyCombiner(archive) as combiner:
        for file in files:
            combiner.write(file.name, _iter(file.path))
