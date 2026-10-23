# msglc

`msglc` serializes JSON compatible objects using [`msgpack`](https://github.com/msgpack/msgpack-python) or [`cbor`](https://github.com/agronholm/cbor2) and embeds a table of contents so that data can be loaded **lazily and partially**.

The encoded payload stays a valid `msgpack`/`cbor` blob.
The extra table of contents makes it possible to jump straight to the piece of data that is actually needed.

## When to use it

It is a good fit for `msgpack`/`cbor` data of more than a few MB whenever one of the following holds.

1. After cold storage, each retrieval only accesses part of the stored data.
2. Decoding the whole file is too expensive in memory or time.
3. Several encoded blobs need to be merged into one without decoding and re-encoding the payload.

## Installation

`msglc` is published on PyPI.

```bash
pip install msglc
```

Optional extras:

```bash
pip install "msglc[msgspec]"   # faster msgpack decoding (recommended)
pip install "msglc[numpy]"     # numpy.ndarray support
pip install "msglc[s3fs]"      # S3 support
pip install "msglc[all]"       # everything above
```

!!! note
    The `msgspec` extra is optional but recommended.
    When `msgspec` is installed it becomes the default `msgpack` codec; otherwise the pure Python `msgpack` codec is used.

## Quick start

Serialize with `dump`, then read with `LazyReader`.
Only the requested node is decoded.

```python
from msglc import dump
from msglc.reader import LazyReader

data = {"a": [1, 2, 3], "b": {"c": 4, "d": 5, "e": [0x221548313] * 10}}
dump("data.msg", data)

with LazyReader("data.msg") as reader:
    print(reader["b/c"])  # 4
    print(reader["b/d"])  # 5
    print(list(reader.keys()))  # ['a', 'b']
```

All data access must happen inside the `with` block because it is read from the file on demand.
Use [`to_obj`](api/utility.md) to fully materialize a value before leaving the block.

## Where to go next

- **[Guide](guide.md)** — serialization, deserialization, combining archives, streaming and custom codecs.
- **[Internals](internals.md)** — why it is fast, the binary layout and the table of contents design.
- **[Configuration](configuration.md)** — every tunable option and its default, plus performance advice.
- **[Storage](storage.md)** — local files, `UPath`, `fsspec` and S3.
- **[API](api/utility.md)** — the generated reference.
