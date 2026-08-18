from collections import deque
from collections.abc import Generator
from typing import Literal

import pytest

from msglc import LazyReader, dump
from msglc.codec import CBORCodec, MsgspecCodec


class Streamable:
    def __init__(self, iterable):
        self._it = iter(iterable)
        self._cache = deque()

    def __iter__(self):
        return self

    def __next__(self):
        if self._cache:
            return self._cache.popleft()
        return next(self._it)

    def _peek(self):
        try:
            peek = next(self._it)
        except StopIteration:
            pass
        else:
            self._cache.append(peek)

    def __bool__(self):
        self._peek()
        return bool(self._cache)


class StreamableListA(Streamable, list):
    pass


class StreamableDictA(Streamable, dict):
    def items(self):
        return self


class StreamableListB(list):
    def __init__(self, gen: Generator):
        super().__init__()
        self._gen = gen

    def __iter__(self):
        yield from self._gen


class StreamableDictB(dict):
    def __init__(self, gen: Generator):
        super().__init__()
        self._gen = gen

    def items(self):
        yield from self._gen


def generator(list_or_dict: Literal["list", "dict"]):
    n = 0
    while n < 10:
        if list_or_dict == "list":
            yield n
        else:
            yield str(n), n
        n += 1


@pytest.mark.parametrize("obj", [StreamableDictA, StreamableDictB])
@pytest.mark.parametrize("backend", ["rust", "python"])
@pytest.mark.parametrize("packer", [MsgspecCodec(), CBORCodec], ids=["msgspec", "cbor"])
def test_streamable_dict(tmpdir, obj, backend: Literal["rust", "python"], packer):
    with tmpdir.as_cwd():
        dump(
            f"stream_{backend}",
            obj(generator(list_or_dict="dict")),
            backend=backend,
            packer=packer,
        )
        with LazyReader(f"stream_{backend}", unpacker=packer) as reader:
            assert reader.to_obj() == dict(generator(list_or_dict="dict"))


@pytest.mark.parametrize("obj", [StreamableListA, StreamableListB])
@pytest.mark.parametrize("backend", ["rust", "python"])
@pytest.mark.parametrize("packer", [MsgspecCodec(), CBORCodec], ids=["msgspec", "cbor"])
def test_streamable_list(tmpdir, obj, backend: Literal["rust", "python"], packer):
    with tmpdir.as_cwd():
        dump(
            "stream",
            obj(generator(list_or_dict="list")),
            backend=backend,
            packer=packer,
        )
        with LazyReader("stream", unpacker=packer) as reader:
            assert reader.to_obj() == list(generator(list_or_dict="list"))
