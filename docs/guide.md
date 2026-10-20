# Guide

This page walks through the common tasks: serializing, deserializing, combining archives, streaming and extending `msglc`.

!!! note
    Snippets are runnable in order from top to bottom.
    Later examples reuse files created by earlier ones (`data.msg`, `large.msg`, `dict.msg`, `list.msg`).

## Serialization

### Write an object to a file

`dump` writes a JSON compatible object to a file.
`msgpack` is the default format.

```python
from msglc import dump

data = {"a": [1, 2, 3], "b": {"c": 4, "d": 5, "e": [0x221548313] * 10}}
dump("data.msg", data)
```

`dump` also accepts a `UPath`, an in-memory buffer, or an existing `LazyReader` (see [Repacking a reader](#repacking-a-reader)).

### Serialize into memory

`dumps` serializes to a `BytesIO` buffer and rewinds it, ready to be read.

```python
from msglc import dumps
from msglc.reader import LazyReader

buffer = dumps({"a": [1, 2, 3]})
with LazyReader(buffer) as reader:
    print(reader.to_obj())  # -> {'a': [1, 2, 3]}
```

### Backends

`dump` has two backends:

| Backend | Default | Notes |
| --- | --- | --- |
| `python` | yes | Works everywhere, supports the widest range of types. |
| `rust` | no | Faster for writing local or remote files; only a subset of Python types. |

```python
from msglc import dump

data = {"a": [1, 2, 3], "b": {"c": 4, "d": 5, "e": [0x221548313] * 10}}
dump("data.msg", data, backend="rust")
```

!!! note
    The `rust` backend only applies when the target is a `str` path or a `UPath`.
    When an in-memory buffer is passed, `dump` silently falls back to the `python` backend.

The `rust` backend does not support every type.
In particular `datetime` objects and integers outside `[-2**63, 2**64 - 1]` raise an error; use the default backend (or CBOR) for those.

### Formats and codecs

A codec defines the underlying serialization format.
The default is `msgpack` (through `msgspec` when it is installed).
To use CBOR, pass `CBORCodec`.

```python
from msglc import dump
from msglc.codec import CBORCodec

data = {"a": [1, 2, 3], "b": {"c": 4, "d": 5, "e": [0x221548313] * 10}}
dump("data.cbor", data, packer=CBORCodec)
```

Available codecs in `msglc.codec`:

| Codec | Protocol | Backed by |
| --- | --- | --- |
| `MsgspecCodec` | msgpack | `msgspec` (falls back to `MsgpackCodec` if absent) |
| `MsgpackCodec` | msgpack | `msgpack` |
| `OrmsgpackCodec` | msgpack | `ormsgpack` (falls back to `MsgpackCodec` if absent) |
| `CBORCodec` | cbor | `cbor2` |

The codec is chosen automatically when reading, based on the file header.
Passing a mismatched `unpacker` raises `ValueError`.

### Supported types

Standard JSON types (`dict`, `list`, `str`, `int`, `float`, `bool`, `None`) are always supported.
A few extra Python types are handled too:

| Python type | Encoded as |
| --- | --- |
| `tuple` | `list` |
| `set` | sorted `list` |
| `bytes` / `bytearray` / `memoryview` | binary value |
| `numpy.ndarray` | nested `list`, or a binary blob when `numpy_encoder=True` |

```python
from msglc import dump
from msglc.reader import LazyReader

dump("types.msg", {"tuple": (1, 2, 3), "set": {3, 1, 2}, "bytes": b"abc", "big": 2**63})
with LazyReader("types.msg") as reader:
    print(reader["tuple"])  # -> [1, 2, 3]
    print(reader["set"])    # -> [1, 2, 3]
    print(reader["bytes"])  # -> b'abc'
    print(reader["big"])    # -> 9223372036854775808
```

!!! warning "Integers and CBOR"
    `msgpack` cannot represent integers outside `[-2**63, 2**64 - 1]`.
    Use `CBORCodec` for arbitrary precision integers.

#### NumPy

With `numpy_encoder=False` (default), arrays are converted to nested lists.
With `numpy_encoder=True`, the array is stored as a binary blob and is decoded back to an `numpy.ndarray` on read.

```python
import numpy
from msglc import dump
from msglc.config import configure
from msglc.reader import LazyReader

arr = numpy.arange(3000, dtype=numpy.float64)

configure(numpy_encoder=True)
dump("arr.msg", {"arr": arr})
with LazyReader("arr.msg") as reader:
    got = reader["arr"]
    print(type(got).__name__, got.shape)  # -> ndarray (3000,)
configure(numpy_encoder=False)
```

!!! note
    The binary `numpy` representation is decoded back to an `ndarray` only when the containing node is indexed.
    If the whole document is small enough to be decoded as a single leaf, the value is returned as raw `bytes` and can be restored with `pickle.loads`.
    Keep arrays inside a larger container (or lower `small_obj_optimization_threshold`) to get an `ndarray` directly.
    See [Small objects](internals.md#small-objects).

## Deserialization

### Reading a value

`LazyReader` is a context manager.
It reads only the table of contents up front and defers reading the actual data until it is accessed.

```python
from msglc.reader import LazyReader

with LazyReader("data.msg") as reader:
    print(reader.read())        # -> the whole document
    print(reader["b/c"])        # -> 4, subscripting is supported
    print(reader.read("b/c"))   # -> 4, or use an explicit path
    print(reader.visit("b/c"))  # -> 4, string-only variant of `read`
```

- `read(path=None)` accepts `None`, a `/`-separated `str`, a `list` of keys, or a `slice`.
- `visit(path="")` is a lighter string-only variant of `read`.

All access has to happen before the `with` block exits, because the file handle is closed afterwards.

### Path syntax

`/` separates the nodes of the path.
Any intermediate node can be exposed as a lazy container.

```python
with LazyReader("data.msg") as reader:
    print(reader["a"])     # -> [1, 2, 3]
    print(reader["a/0"])   # -> 1
```

Lists support negative indices and slices.
Two-part slices behave like Python, but **three-part slices use the `start:step:stop` order**, not Python's `start:stop:step`.

```python
from msglc import dump

large = {"items": [{"id": i, "payload": "x" * 100} for i in range(1000)]}
dump("large.msg", large)
```

```python
with LazyReader("large.msg") as reader:   # large.msg has a 1000-element "items" list
    print(reader["items/0"])       # -> first element
    print(reader["items/-1"])      # -> last element
    print(reader["items/2:5"])     # -> elements 2, 3, 4
    print(reader["items/0:250:"])  # -> every 250th element: 0, 250, 500, 750
```

!!! warning "Slice order"
    The three-part form is `start:step:stop`.
    `"0:2:10"` means *start at 0, step by 2, stop at 10*, which is different from Python's `[0:2:10]`.

Invalid index types raise `TypeError`; out-of-range access is normalized like Python.

### Dictionary and list behavior

A lazy container mirrors the interface of `dict` or `list`.

```python
with LazyReader("data.msg") as reader:
    print(list(reader.keys()))  # -> ['a', 'b']
    print(reader.get("missing"))  # -> None
    print("b" in reader)        # -> True
    print(len(reader))          # -> 2
```

`LazyDict` supports `get`, `keys`, `values`, `items`, `in`, `len` and iteration.
`LazyList` supports `len`, iteration and subscripting.

### Lazy containers and caching

When a container is large enough to be indexed, it is returned as a `LazyDict` or `LazyList`.
Otherwise the node is small and is decoded eagerly, so you get a plain `dict` or `list`.

```python
from msglc.reader import LazyReader

with LazyReader("large.msg") as reader:
    root = reader.unwrap()
    print(type(root).__name__)                # -> LazyDict
    print(type(root["items"]).__name__)       # -> LazyList
    print(len(root["items"]))                 # -> 1000
    print(root["items"][7])                   # -> {'id': 7, 'payload': 'xxx...'}
```

By default each read node is cached so it is fetched at most once.
Pass `cached=False` to skip the cache; this keeps memory flat for one-pass reads at the cost of re-reading shared nodes.

```python
with LazyReader("data.msg", cached=False) as reader:
    print(reader.read("b/c"))  # -> 4
```

### Materializing data

`to_obj` recursively reads a value and returns plain Python objects that are safe to use outside the `with` block.
`unwrap` peels the `LazyReader` wrapper and returns the underlying lazy container or primitive without forcing a full read.

```python
from msglc.reader import LazyReader, to_obj

with LazyReader("data.msg") as reader:
    obj = reader.to_obj()          # plain dict / list / value
    node = reader.unwrap()         # LazyDict / LazyList / value
print(obj["a"])                    # safe after the context exits
```

There is also a module-level `to_obj(value)` for converting an arbitrary lazy value.

### Tracking reads

Pass a `LazyStats` counter to measure how much data is actually read.
It is useful when tuning block size.

```python
from msglc.reader import LazyReader, LazyStats

stats = LazyStats()
with LazyReader("large.msg", counter=stats) as reader:
    _ = reader["items/0"]
print(stats)  # e.g. "2 calls, 11887 bytes, 5943.5 bytes per call."
```

### Repacking a reader

A `LazyReader` can be passed straight to `dump`.
The data is copied lazily, so no full decode is performed.

```python
from msglc import dump
from msglc.reader import LazyReader

with LazyReader("data.msg") as reader:
    dump("data-copy.msg", reader)
```

### Async access

`async_read`, `async_visit` and `async_to_obj` run the blocking reads on a worker thread, which is handy for async servers.

```python
import asyncio
from msglc.reader import LazyReader, async_to_obj


async def main():
    with LazyReader("data.msg") as reader:
        print(await reader.async_read("b/c"))          # -> 4
        print((await async_to_obj(reader))["a"])       # -> [1, 2, 3]


asyncio.run(main())
```

## Combining archives

`combine` merges several serialized files into one **without decoding and re-encoding** the payload.
Each input is copied as a raw byte block and a start offset is recorded in the table of contents.

### Dictionary and list layouts

With names, the archive uses a `dict` layout; without names it uses a `list` layout.

```python
from msglc import dump, combine, FileInfo
from msglc.reader import LazyReader

dump("dict.msg", {str(v): v for v in range(1000)})
dump("list.msg", [float(v) for v in range(1000)])

# dict layout: {"dict": {...}, "list": [...]}
combine("combined.msg", [FileInfo("dict.msg", "dict"), FileInfo("list.msg", "list")])
with LazyReader("combined.msg") as reader:
    print(reader["dict/101"])     # -> 101
    print(reader["list/101"])     # -> 101.0
    print(reader["dict"]["101"])  # -> 101, also reachable without a path
```

```python
# list layout: [{...}, [...]]
combine("combined.msg", [FileInfo("dict.msg"), FileInfo("list.msg")])
with LazyReader("combined.msg") as reader:
    print(reader[0]["101"])  # -> 101
    print(reader[1]["101"])  # -> 101.0
```

!!! note "Naming rules"
    Names are optional, but they must be either present on **all** inputs or on none.
    Names must be unique.
    Violations raise `ValueError`.

### FileInfo sources

`FileInfo` describes where an input comes from.
It accepts a local path, a `UPath`, an open buffer, an existing `LazyReader`, or an in-memory object via `obj=`.

```python
from msglc import combine, FileInfo
from msglc.reader import LazyReader
from upath import UPath

combine(
    "combined.msg",
    [
        FileInfo(None, obj={"k": "v"}),     # serialize an in-memory object
        FileInfo(UPath("dict.msg")),        # a UPath
        FileInfo(LazyReader("list.msg")),   # another reader, copied lazily
    ],
)
with LazyReader("combined.msg") as reader:
    print(reader[0]["k"])       # -> v
    print(reader[1]["101"])     # -> 101
    print(reader[2]["101"])     # -> 101.0
```

Each `FileInfo` may carry its own `fs=` so inputs can come from different filesystems.

### Mixing formats

Inputs may use different formats.
The metadata of the combined archive is encoded with the codec passed to `combine`, while every payload keeps its original encoding.

```python
from msglc import dump, combine, FileInfo
from msglc.codec import CBORCodec, MsgspecCodec
from msglc.reader import LazyReader

dump("dict.cbor", {str(v): v for v in range(1000)}, backend="rust", packer=CBORCodec)
dump("list.msg", [float(v) for v in range(1000)], backend="rust")

combine("combined.msg", [FileInfo("dict.cbor"), FileInfo("list.msg")], packer=MsgspecCodec)
with LazyReader("combined.msg") as reader:
    print(reader[0]["101"])  # -> 101
    print(reader[1]["101"])  # -> 101.0
```

### Recursive combining and append

The output of `combine` is itself a valid archive and can be combined again.
`append` is shorthand for `combine(..., mode="a")`.

```python
from msglc import dump, combine, append, FileInfo
from msglc.reader import LazyReader

dump("dict.msg", {str(v): v for v in range(1000)})
dump("list.msg", [float(v) for v in range(1000)])

combine("inner.msg", [FileInfo("dict.msg", "d"), FileInfo("list.msg", "l")])
combine("outer.msg", [FileInfo("inner.msg", "inner")])
with LazyReader("outer.msg") as reader:
    print(reader["inner/d/101"])  # -> 101

combine("app.msg", [FileInfo("dict.msg")])
append("app.msg", [FileInfo("list.msg")])
with LazyReader("app.msg") as reader:
    print(reader[0]["101"], reader[1]["101"])  # -> 101 101.0
```

### Splitting a combined archive

Because combining only copies bytes, an individual child can be extracted by binary copying.
That is the exact reverse of combining and requires no decoding.

```python
from msglc import combine, FileInfo
from msglc.reader import LazyReader

combine("combined.msg", [FileInfo(None, "child_dict", obj={"a": "a", "b": "b"})])
with LazyReader("combined.msg") as combined, open("child.msg", "wb") as f:
    f.write(combined["child_dict"].raw_data(chunked=False))

with LazyReader("child.msg") as child:
    print(child.to_obj())  # -> {'a': 'a', 'b': 'b'}
```

### Raw protocol data

`protocol_raw_data` returns the payload encoded in the plain `msgpack`/`cbor` format, without any `msglc` metadata.
It can be decoded by any compliant decoder.

```python
from msgpack import unpackb
from msglc import dump
from msglc.reader import LazyReader

dump("data.msg", {"a": [1, 2, 3]})
with LazyReader("data.msg") as reader:
    print(unpackb(reader.protocol_raw_data(chunked=False)))  # -> {'a': [1, 2, 3]}
```

Both `protocol_raw_data` and `raw_data` accept `chunked=True` (default) to return a generator of chunks, or `chunked=False` to return a single `bytes` object.

## Streaming data

### Writing generated data

The writer does not require the object to exist in memory first.
It recognizes any `dict` subclass that implements `items()` and any `list` subclass that implements `__iter__`, so data can be produced on the fly.

```python
from collections.abc import Generator
from msglc import dump
from msglc.reader import LazyReader


class Stream(dict):
    def __init__(self, generator: Generator):
        super().__init__()
        self._gen = generator

    def items(self):          # only `items()` is used
        yield from self._gen


def example():
    yield "a", 1
    yield "b", 2


dump("stream.msg", Stream(example()))
with LazyReader("stream.msg") as reader:
    print(reader.to_obj())  # -> {'a': 1, 'b': 2}
```

!!! note "Custom containers"
    The Python writer recognizes container subclasses unconditionally.
    The rust writer recognizes them only when `enable_custom_container` is `True` (the default).
    See [Configuration](configuration.md).

### Bounded memory

Because data is streamed to disk, both writing and reading use memory that is independent of the file size.

```python
import tracemalloc
from pathlib import Path
from msglc import dump
from msglc.reader import LazyDict, LazyList, LazyReader


class Stream(dict):
    def __init__(self, generator):
        super().__init__()
        self._gen = generator

    def items(self):
        yield from self._gen


def nest_generator(depth):
    n = 0
    if depth > 0:
        while n < 10:
            yield str(n), Stream(nest_generator(depth - 1))
            n += 1
    else:
        while n < 10:
            yield str(n), "A" * (256 * 1024)
            n += 1


def visitor(obj):
    if isinstance(obj, LazyReader):
        yield from visitor(obj.unwrap())
    if isinstance(obj, (LazyDict, dict)):
        for v in obj.values():
            yield from visitor(v)
    elif isinstance(obj, (LazyList, list)):
        for v in obj:
            yield from visitor(v)
    else:
        yield obj


tracemalloc.start()
dump("example.msg", Stream(nest_generator(2)))
_, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
print(f"File size: {Path('example.msg').stat().st_size / 1024 / 1024:.3f} MB.")
print(f"Peak memory during serialization: {peak / 1024 / 1024:.3f} MB.")

tracemalloc.start()
with LazyReader("example.msg", cached=False) as reader:
    for _ in visitor(reader):
        ...
_, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
print(f"Peak memory during deserialization: {peak / 1024 / 1024:.3f} MB.")
# File size: 250.024 MB.
# Peak memory during serialization: ~11 MB.
# Peak memory during deserialization: ~1 MB.
```

The file is roughly 250 MB, yet neither writing nor reading needs more than a few MB of memory.

## Extending msglc

### Custom codec

Subclass `LazyCodec` (or one of the provided bases) to plug in another serialization format.
Pass it as `packer` when writing and as `unpacker` when reading.

```python
import msgpack
from msglc import dump
from msglc.codec import MsgpackCodecBase
from msglc.reader import LazyReader


class MyCodec(MsgpackCodecBase):
    def encode(self, data):
        return msgpack.packb(data)

    def decode(self, data):
        return msgpack.unpackb(data)


dump("custom.msg", {"a": [1, 2, 3]}, packer=MyCodec)
with LazyReader("custom.msg", unpacker=MyCodec) as reader:
    print(reader.to_obj())  # -> {'a': [1, 2, 3]}
```

A codec must implement `encode`, `decode`, `stream_decode`, `write_array_header` and `write_map_header`.
`MsgpackCodecBase` already provides the last three for msgpack-style framing.

### Custom table of contents

Pass `toc_cls` to `LazyWriter` (or to `dump`) to customize how the table of contents is built.
The class is initialized with `packer=` and `buffer=` keyword arguments and must expose `pack(obj) -> dict`.

```python
from msglc import dump
from msglc.reader import LazyReader
from msglc.toc import TOC


class MyTOC(TOC):
    pass


dump("custom_toc.msg", {"a": [1, 2, 3]}, toc_cls=MyTOC)
with LazyReader("custom_toc.msg") as reader:
    print(reader.to_obj())  # -> {'a': [1, 2, 3]}
```

`toc_cls` is ignored by the `rust` backend.
