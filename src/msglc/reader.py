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

from io import BytesIO, BufferedReader

from bitarray import bitarray
from msgpack import Unpacker  # type: ignore

from .config import config, increment_gc_counter, decrement_gc_counter, BufferReader
from .utility import normalise_index, MockIO, to_index
from .writer import LazyWriter


def to_obj(v):
    return v.to_obj() if isinstance(v, LazyItem) else v


class LazyStats:
    def __init__(self):
        self._read_counter: int = 0
        self._call_counter: int = 0

    def __iadd__(self, other):
        self._read_counter += other
        self._call_counter += 1
        return self

    def __str__(self):
        return f"{self._call_counter} calls, {self._read_counter} bytes, {self.bytes_per_call()} bytes per call."

    def __call__(self, *args, **kwargs):
        return self._read_counter

    def bytes_per_call(self):
        return self._read_counter / self._call_counter

    def clear(self):
        self._read_counter = 0
        self._call_counter = 0


class LazyItem:
    def __init__(
        self,
        buffer: BufferReader,
        offset: int,
        *,
        counter: LazyStats | None = None,
        cached: bool = True,
        unpacker: Unpacker | None = None,
    ):
        self._buffer: BufferReader = buffer
        self._offset: int = offset  # start of original data
        self._counter: LazyStats | None = counter
        self._cached: bool = cached
        self._unpacker: Unpacker = unpacker if unpacker else Unpacker()

        self._accessed_items: int = 0

    def __len__(self):
        raise NotImplementedError

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

    def _unpack(self, data: bytes):
        self._unpacker.feed(data)
        return self._unpacker.unpack()

    def _read(self, start: int, end: int):
        return self._unpack(self._readb(start, end))

    def _child(self, toc: dict | int):
        self._accessed_items += 1

        params: dict = {"counter": self._counter, "cached": self._cached, "unpacker": self._unpacker}

        # {"t": {"name1": start_pos, "name2": start_pos}}
        # this is used in combined archives
        if isinstance(toc, int):
            self._buffer.seek(toc + self._offset)
            return LazyReader(self._buffer, **params)

        if (child_toc := toc.get("t", None)) is None:
            # {"p": [start_pos, end_pos]}
            # this is used in small objects
            if 2 == len(child_pos := toc["p"]) and all(isinstance(x, int) for x in child_pos):
                return self._read(*child_pos)

            # {"p": [[size1, start_pos, end_pos], [size2, start_pos, end_pos], [size3, start_pos, end_pos]]}
            # this is used in arrays of small objects
            return LazyList(toc, self._buffer, self._offset, **params)

        # {"t": [...], "p": [start_pos, end_pos]}
        # this is used in lazy lists
        if isinstance(child_toc, list):
            return LazyList(toc, self._buffer, self._offset, **params)

        # {"t": {...}, "p": [start_pos, end_pos]}
        # this is used in lazy dicts
        if isinstance(child_toc, dict):
            return LazyDict(toc, self._buffer, self._offset, **params)

        raise ValueError(f"Invalid: {toc}.")

    @property
    def _fast_loading(self):
        return config.fast_loading and self._accessed_items < config.fast_loading_threshold * len(self)

    def to_obj(self):
        raise NotImplementedError


class LazyList(LazyItem):
    def __init__(
        self,
        toc: dict,
        buffer: BufferReader,
        offset: int,
        *,
        counter: LazyStats | None = None,
        cached: bool = True,
        unpacker: Unpacker | None = None,
    ):
        super().__init__(buffer, offset, counter=counter, cached=cached, unpacker=unpacker)
        self._toc: list | None = toc.get("t", None)  # if None, it's a list of small objects
        self._pos: list = toc.get("p", None)  # noqa # if None, it comes from a combined archive
        self._index: int = 0
        self._cache: list = [None] * len(self)
        self._mask: bitarray = bitarray(len(self))
        self._mask.setall(0)  # ensure all bits are 0
        self._full_loaded: bool = False
        self._size_list: list = [0]
        if self._toc is None:
            total_size: int = 0
            for size, _, _ in self._pos:
                total_size += size
                self._size_list.append(total_size)

    def __repr__(self):
        return f"LazyList[{len(self)}]" if config.simple_repr or not self._cached else self.to_obj().__repr__()

    def _lookup_index(self, index: int) -> int:
        low: int = 0
        high: int = len(self._size_list) - 1

        while True:
            mid: int = (low + high) // 2

            if self._size_list[mid] <= index < self._size_list[mid + 1]:
                return mid

            if index < self._size_list[mid]:
                high = mid
            else:
                low = mid

    def _all(self, start: int, end: int) -> list:
        return list(Unpacker(BytesIO(self._readb(start, end))))

    def __getitem__(self, index):
        index_range: list | range
        if isinstance(index, str):
            try:
                index_range = [int(index)]
            except ValueError:
                raise TypeError(f"Invalid type: {type(index)} for index {index}.")
        elif isinstance(index, slice):
            index_range = range(*index.indices(len(self)))
        elif isinstance(index, int):
            index_range = [index]
        else:
            raise TypeError(f"Invalid type: {type(index)} for index {index}.")

        if self._cached:
            for item in index_range:
                item = normalise_index(item, len(self))

                if 0 == self._mask[item]:
                    if self._toc is not None:
                        self._mask[item] = 1
                        self._cache[item] = self._child(self._toc[item])
                    else:
                        lookup_index: int = self._lookup_index(item)
                        num_start, num_end = self._size_list[lookup_index], self._size_list[lookup_index + 1]
                        self._mask[num_start:num_end] = 1
                        self._cache[num_start:num_end] = self._all(*self._pos[lookup_index][1:])

            return self._cache[index]

        for item in index_range:
            item = normalise_index(item, len(self))

            if self._toc is not None:
                self._cache[item] = self._child(self._toc[item])
            else:
                lookup_index = self._lookup_index(item)
                num_start, num_end = self._size_list[lookup_index], self._size_list[lookup_index + 1]
                self._cache[num_start:num_end] = self._all(*self._pos[lookup_index][1:])

        result = self._cache[index]
        self._cache = [None] * len(self)
        return result

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
        return self._toc.__len__() if self._toc is not None else sum(x[0] for x in self._pos)

    def to_obj(self):
        if not self._cached:
            if self._toc is not None:
                return self._read(*self._pos)

            result: list = []
            for _, start, end in self._pos:
                result.extend(self._all(start, end))

            return result

        if not self._full_loaded:
            self._full_loaded = True
            if not self._fast_loading:
                for index in range(len(self)):
                    self._cache[index] = to_obj(self[index])
            elif self._toc is not None:
                self._cache = self._read(*self._pos)
            else:
                num_start, num_end = 0, 0
                for size, start, end in self._pos:
                    num_end += size
                    if 0 == self._mask[num_start]:
                        self._cache[num_start:num_end] = self._all(start, end)
                    num_start = num_end

            self._mask.setall(1)

        return self._cache


class LazyDict(LazyItem):
    def __init__(
        self,
        toc: dict,
        buffer: BufferReader,
        offset: int,
        *,
        counter: LazyStats | None = None,
        cached: bool = True,
        unpacker: Unpacker | None = None,
    ):
        super().__init__(buffer, offset, counter=counter, cached=cached, unpacker=unpacker)
        self._toc: dict = toc["t"]
        self._pos: list = toc.get("p", None)  # noqa # if empty, it comes from a combined archive
        self._cache: dict = {}
        self._full_loaded: bool = False

    def __repr__(self):
        return f"LazyDict[{len(self)}]" if config.simple_repr or not self._cached else self.to_obj().__repr__()

    def __getitem__(self, key):
        if not self._cached:
            return self._child(self._toc[key])

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
        if not self._cached:
            return self._read(*self._pos)

        if not self._full_loaded:
            self._full_loaded = True
            if self._fast_loading and self._pos is not None:
                self._cache = self._read(*self._pos)
            else:
                for k in self:
                    self._cache[k] = to_obj(self[k])

        return self._cache


class LazyReader(LazyItem):
    def __init__(
        self,
        buffer_or_path: str | BufferReader,
        *,
        counter: LazyStats | None = None,
        cached: bool = True,
        unpacker: Unpacker | None = None,
    ):
        self._buffer_or_path: str | BufferReader = buffer_or_path

        buffer: BufferReader
        if isinstance(self._buffer_or_path, str):
            buffer = open(self._buffer_or_path, "rb", buffering=config.read_buffer_size)
        elif isinstance(self._buffer_or_path, (BytesIO, BufferedReader, MockIO)):
            buffer = self._buffer_or_path
        else:
            raise ValueError("Expecting a buffer or path.")

        sep_a, sep_b, sep_c = LazyWriter.magic_len(), LazyWriter.magic_len() + 10, LazyWriter.magic_len() + 20

        # keep the buffer unchanged in case of failure
        original_pos: int = buffer.tell()
        header: bytes = buffer.read(sep_c)
        buffer.seek(original_pos)

        if header[:sep_a] != LazyWriter.magic:
            raise ValueError("Invalid file format.")

        super().__init__(buffer, original_pos + sep_c, counter=counter, cached=cached, unpacker=unpacker)

        toc_start: int = self._unpack(header[sep_a:sep_b].lstrip(b"\0"))
        toc_size: int = self._unpack(header[sep_b:sep_c].lstrip(b"\0"))

        self._obj = self._child(self._read(toc_start, toc_start + toc_size))

    def __repr__(self):
        file_path: str = f" ({self._buffer_or_path})" if isinstance(self._buffer_or_path, str) else ""

        return f"LazyReader{file_path}" if config.simple_repr or not self._cached else self.to_obj().__repr__()

    def __enter__(self):
        increment_gc_counter()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        decrement_gc_counter()

        if isinstance(self._buffer_or_path, str):
            self._buffer.close()

    def __getitem__(self, item):
        return self.read(item)

    def __len__(self):
        return len(self._obj)

    def __contains__(self, item):
        return item in self._obj

    def get(self, key, default=None):
        return self._obj.get(key, default)

    def keys(self):
        return self._obj.keys()

    def values(self):
        return self._obj.values()

    def items(self):
        return self._obj.items()

    def read(self, path: str | list | slice | None = None):
        """
        Reads the data from the given path.

        This method navigates through the data structure based on the provided path.
        The path can be a string or a list. If it's a string, it's split into a list
        using '/' as the separator. Each element of the list is used to navigate
        through the data structure.

        If the path is None, it returns the root object.

        :param path: the path to the data to read
        :return: The data at the given path.
        """

        path_stack: list
        if path is None:
            path_stack = []
        elif isinstance(path, str):
            path_stack = path.split("/")
        elif isinstance(path, list):
            path_stack = path
        else:
            path_stack = [path]

        target = self._obj
        for key in path_stack:
            if "" == key:
                continue
            target = target[
                to_index(key, len(target)) if isinstance(key, str) and isinstance(target, (list, LazyList)) else key
            ]
        return target

    def visit(self, path: str = ""):
        """
        Reads the data from the given path.

        This method navigates through the data structure based on the provided path.
        The path can be a string of paths separated by '/'.

        If the path is None, it returns the root object.

        :param path: the path to the data to read
        :return: The data at the given path.
        """
        target = self._obj
        for key in path.split("/"):
            if "" == key:
                continue
            target = target[to_index(key, len(target)) if isinstance(target, (list, LazyList)) else key]
        return target

    def to_obj(self):
        return to_obj(self._obj)
