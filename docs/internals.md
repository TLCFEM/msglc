# Internals

This page explains why `msglc` is fast and how the binary format is organized.

## Motivation

[MessagePack](https://msgpack.org/) and [CBOR](https://www.rfc-editor.org/rfc/rfc8949.html) are compact binary serialization formats, but they carry no structural metadata.
To reach a node deep inside a document, a decoder has to walk the whole blob.
For cold storage where only a small part of a large document is read at a time, that is wasteful.

`msglc` appends a **table of contents** (TOC) to the payload, much like a ZIP archive keeps a central directory.
On open, only the TOC is read; the requested node is then located and decoded directly.
The TOC is normally tiny compared to the payload, so the overhead is negligible.

## Binary layout

```
+--------------+----------------+--------------+-------------------+
| magic bytes  | 20-byte header | encoded data | encoded TOC       |
+--------------+----------------+--------------+-------------------+
```

1. **Magic bytes** — up to 30 bytes identifying the format.
   Default is `msglc-2024`, configurable.
2. **20-byte header** — the start position and the length of the encoded TOC, 10 bytes each.
3. **Encoded data** — the original `msgpack`/`cbor` payload.
4. **Encoded TOC** — the table of contents, itself encoded in the same format.

The TOC is placed at the end so the payload can be streamed directly to disk without seeking back.
The payload is self-contained: it can be decoded without the TOC.

```python
from msglc import dump
from msglc.reader import LazyReader

dump("data.msg", {"a": [1, 2, 3]})

with open("data.msg", "rb") as f:
    print(f.read(30).lstrip(b"\0"))  # -> b'msglc-2024'

with LazyReader("data.msg") as reader:
    # the payload alone is valid msgpack
    from msgpack import unpackb

    print(unpackb(reader.protocol_raw_data(chunked=False)))  # -> {'a': [1, 2, 3]}
```

## Table of contents

JSON has two container kinds: arrays (`list`) and objects (`dict`).
The TOC mirrors that structure.
Every node stores its position under the key `p`; containers additionally store their children under `t`.

| Node | TOC |
| --- | --- |
| primitive or small container | `{"p": [start, end]}` |
| dict with indexed children | `{"p": [start, end], "t": {"key": {...}}}` |
| list with indexed children | `{"p": [start, end], "t": [{...}, ...]}` |
| list of trivial items | `{"p": [[size, start, end], ...]}` |

The single-character keys `p` (position) and `t` (table) keep the TOC small.

You can generate the TOC for any object directly with the `TOC` class.

```python
from io import BytesIO
from msglc.codec import MsgspecCodec
from msglc.toc import TOC
from msglc.config import configure


def toc_of(data, threshold):
    configure(small_obj_optimization_threshold=threshold, trivial_size=threshold)
    buffer = BytesIO()
    return TOC(packer=MsgspecCodec(), buffer=buffer).pack(data)


# primitives are leaves
print(toc_of(2154848, 10))  # -> {'p': [0, 5]}
print(toc_of("a string", 10))  # -> {'p': [0, 9]}

# a small dict: its children are indexed
print(toc_of({"a": {"c": 4, "d": 5, "e": [1, 1, 1]}, "b": 2}, 10))
# -> {'p': [0, 19], 't': {'a': {'p': [3, 16]}, 'b': {'p': [18, 19]}}}
```

### Small objects

If every node were indexed, a shallow document would produce a TOC larger than the data itself.
Instead, a node smaller than `small_obj_optimization_threshold` is treated as a **leaf**: its `t` is dropped and the whole node is decoded on demand.
In the example above, the inner dict `"a"` is small, so it appears only as `{"p": [3, 16]}` even though it contains a list.

### Lists of trivial items

Long lists of small values (integers, floats) are grouped into blocks instead of being indexed element-wise.
A block is `(size, start, end)`.
`trivial_size` defines what counts as a trivial element.

```python
print(toc_of(list(range(500)), 20)["p"][:3])
# -> [(20, 3, 23), (20, 23, 43), (20, 43, 63)]
```

Each block holds 20 items here.
Reading one element fetches a whole block, which avoids a flood of tiny reads.

### Choosing the block size

`small_obj_optimization_threshold` is the block size.
A smaller value yields a finer TOC and smaller reads but a larger TOC; a larger value yields a coarser TOC and larger reads.
The same document at four thresholds:

```python
data = {"id": [{"x": [0] * 40}, {"y": [1] * 40}]}
for threshold in (16, 64, 256, 2048):
    print(threshold, toc_of(data, threshold))
# 16   {'p': [0, 97], 't': {'id': {'p': [4, 97], 't': [...both children indexed...]}}}
# 64   {'p': [0, 97], 't': {'id': {'p': [4, 97]}}}
# 256  {'p': [0, 97]}
# 2048 {'p': [0, 97]}
```

Each read fetches at least one block.
Set the threshold to a multiple of the storage block size — often 4 KiB for local disks, larger for remote object stores.
See [Configuration](configuration.md) for the exact option names and defaults.

## Reading model

1. The TOC is decoded on open (cheap).
2. Accessing a node decodes only that node's byte range.
3. Decoded nodes are cached so each is fetched at most once, unless `cached=False`.

### Fast loading

For a container there are two ways to materialize it:

- read the whole container in one system call, or
- read each child separately.

The first is cheaper when the whole container is needed soon; the second avoids re-reading parts that were already fetched.
`fast_loading` enables the heuristic and `fast_loading_threshold` controls the switch: if the fraction of already-read children is below the threshold, the container is read in one go.

```python
from msglc.config import configure

configure(fast_loading=True, fast_loading_threshold=0.3)
```

## Magic bytes and versioning

The leading magic bytes identify the format.
You can set your own (up to 30 bytes) to version a dataset or to prevent opening unrelated files.

```python
from msglc.config import configure

configure(magic=b"my-app-v1")

# restore the default
configure(magic=b"msglc-2024")
```

For finer control, `compatibility_check` accepts a callable `bytes -> bool` used instead of the exact magic comparison.
See [`configure`](api/utility.md#configure).
