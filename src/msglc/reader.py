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

from io import BytesIO

from msgpack import unpackb, Unpacker

from .config import config, increment_gc_counter, decrement_gc_counter
from .writer import Writer


def to_obj(v):
    return v.to_obj() if isinstance(v, LazyItem) else v


class ReaderStats:
    def __init__(self):
        self._read_counter: int = 0
        self._call_counter: int = 0

    def __iadd__(self, other):
        self._read_counter += other
        self._call_counter += 1
        return self

    def __call__(self, *args, **kwargs):
        return self._read_counter

    def bytes_per_call(self):
        return self._read_counter / self._call_counter

    def clear(self):
        self._read_counter = 0
        self._call_counter = 0


class LazyItem:
    def __init__(self, buffer: BytesIO, offset: int, *, counter: ReaderStats = None):
        self._buffer: BytesIO = buffer
        self._offset: int = offset
        self._counter: ReaderStats = counter

        self._accessed_items: int = 0

    def __eq__(self, other):
        return self.to_obj() == to_obj(other)

    def __str__(self):
        return self.to_obj().__str__()

    # noinspection SpellCheckingInspection
    def _readb(self, start: int, end: int):
        if self._buffer.closed:
            raise ValueError("File is closed.")

        size: int = end - start
        if self._counter:
            self._counter += size
        self._buffer.seek(start + self._offset)
        return self._buffer.read(size)

    def _read(self, start: int, end: int):
        return unpackb(self._readb(start, end))

    def _child(self, toc: dict):
        self._accessed_items += 1

        if (child_toc := toc.get("t", None)) is None:
            if 2 == len(toc["p"]) and isinstance(toc["p"][0], int) and isinstance(toc["p"][1], int):
                return self._read(*toc["p"])

            return LazyList(toc, self._buffer, self._offset, counter=self._counter)

        if isinstance(child_toc, list):
            return LazyList(toc, self._buffer, self._offset, counter=self._counter)

        if isinstance(child_toc, dict):
            return LazyDict(toc, self._buffer, self._offset, counter=self._counter)

        raise ValueError(f"Invalid: {toc}.")

    def to_obj(self):
        raise NotImplementedError


class LazyList(LazyItem):
    def __init__(self, toc: dict, buffer: BytesIO, offset: int, *, counter: ReaderStats = None):
        super().__init__(buffer, offset, counter=counter)
        self._toc: list = toc.get("t", [])
        self._pos: list = toc["p"]
        self._index: int = 0
        self._cache: list = [None] * len(self)
        self._full_loaded: bool = False

    def __repr__(self):
        return f"LazyList[{len(self)}]" if config.simple_repr else self.to_obj().__repr__()

    def __getitem__(self, index):
        if isinstance(index, str):
            try:
                index = int(index)
            except ValueError:
                raise TypeError(f"Invalid type: {type(index)} for index {index}.")

        if isinstance(index, slice):
            index_range = range(*index.indices(len(self)))
        elif isinstance(index, int):
            index_range = [index]
        else:
            raise TypeError(f"Invalid type: {type(index)} for index {index}.")

        for item in index_range:
            total_size = len(self)
            while item < 0:
                item += total_size
            while item >= total_size:
                item -= total_size

            if self._cache[item] is None:
                if self._toc:
                    self._cache[item] = self._child(self._toc[item])
                else:
                    num_start, num_end = 0, 0
                    for size, start, end in self._pos:
                        num_end += size
                        if num_start <= item < num_end:
                            self._cache[num_start:num_end] = list(Unpacker(BytesIO(self._readb(start, end))))
                            break
                        num_start = num_end

        return self._cache[index]

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        if self._index >= len(self):
            raise StopIteration

        item = self[self._index]
        self._index += 1
        return item

    def __len__(self):
        return self._toc.__len__() if self._toc else sum(x[0] for x in self._pos)

    def to_obj(self):
        if not self._full_loaded:
            self._full_loaded = True
            if config.fast_loading and self._accessed_items < config.fast_loading_threshold * len(self):
                if self._toc:
                    self._cache = self._read(*self._pos)
                else:
                    num_start, num_end = 0, 0
                    for size, start, end in self._pos:
                        num_end += size
                        self._cache[num_start:num_end] = list(Unpacker(BytesIO(self._readb(start, end))))
                        num_start = num_end
            else:
                self._cache = [to_obj(v) for v in self]

        return self._cache


class LazyDict(LazyItem):
    def __init__(self, toc: dict, buffer: BytesIO, offset: int, *, counter: ReaderStats = None):
        super().__init__(buffer, offset, counter=counter)
        self._toc: dict = toc["t"]
        self._pos: list = toc["p"]
        self._cache: dict = {}
        self._full_loaded: bool = False

    def __repr__(self):
        return f"LazyDict[{len(self)}]" if config.simple_repr else self.to_obj().__repr__()

    def __getitem__(self, key):
        if key not in self._cache:
            self._cache[key] = self._child(self._toc[key])

        return self._cache[key]

    def __contains__(self, item):
        return item in self._toc

    def __iter__(self):
        return self._toc.__iter__()

    def __len__(self):
        return self._toc.__len__()

    def get(self, key, default=None):
        return self[key] if key in self._toc else default

    def items(self):
        for k in self._toc:
            yield k, self[k]

    def keys(self):
        return self._toc.keys()

    def values(self):
        for k in self._toc:
            yield self[k]

    def to_obj(self):
        if not self._full_loaded:
            self._full_loaded = True
            if config.fast_loading and self._accessed_items < config.fast_loading_threshold * len(self):
                self._cache = self._read(*self._pos)
            else:
                self._cache = {k: to_obj(v) for k, v in self.items()}

        return self._cache


class Reader(LazyItem):
    def __init__(self, buffer_or_path: str | BytesIO, counter: ReaderStats = None):
        self._buffer_or_path: str | BytesIO = buffer_or_path

        if isinstance(self._buffer_or_path, str):
            buffer = open(self._buffer_or_path, "rb", buffering=config.read_buffer_size)
        elif isinstance(self._buffer_or_path, BytesIO):
            buffer = self._buffer_or_path
        else:
            raise ValueError("Expecting a buffer or path.")

        buffer.seek(0)

        sep_a, sep_b, sep_c = Writer.magic_len(), Writer.magic_len() + 10, Writer.magic_len() + 20

        header: bytes = buffer.read(sep_c)

        if header[:sep_a] != Writer.magic:
            raise ValueError("Invalid file format.")

        super().__init__(buffer, sep_c, counter=counter)

        toc_start: int = unpackb(header[sep_a:sep_b].lstrip(b"\0")) - sep_c  # trick to reuse utility function
        toc_size: int = unpackb(header[sep_b:sep_c].lstrip(b"\0"))

        self._obj = self._child(self._read(toc_start, toc_start + toc_size))

    def __enter__(self):
        increment_gc_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        decrement_gc_counter()
        if isinstance(self._buffer_or_path, str):
            self._buffer.close()

    def read(self, path: str | list = None):
        if path is None:
            path_stack = []
        elif isinstance(path, str):
            path_stack = path.split("/")
        else:
            path_stack = path

        target = self._obj
        while path_stack:
            key = path_stack.pop(0)
            if isinstance(key, str) and key.isdigit() and isinstance(target, list):
                key = int(key)
            target = target[key]
        return target

    def to_obj(self):
        return to_obj(self._obj)
