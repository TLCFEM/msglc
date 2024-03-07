# msglc --- (de)serialize json objects with lazy/partial loading containers using msgpack

[![codecov](https://codecov.io/gh/TLCFEM/msglc/graph/badge.svg?token=JDPARZSVDR)](https://codecov.io/gh/TLCFEM/msglc)

## Quick Start

Use `dump` to serialize a json object to a file.

```python
from msglc import dump

data = {"a": [1, 2, 3], "b": {"c": 4, "d": 5, "e": [0x221548313] * 10000}}
dump("data.msg", data)
```

Use `Reader` to read a file.

```python
from msglc import Reader, to_obj

with Reader("data.msg") as reader:
    data = reader.read("b/c")
    print(data)  # 4
    b_dict = reader.read("b")
    print(b_dict.__class__)  # <class 'msglc.reader.LazyDict'>
    for k, v in b_dict.items():
        if k != "e":
            print(k, v)  # c 4, d 5
    b_json = to_obj(b_dict)  # ensure plain dict
```

Please note all data operations shall be performed inside the `with` block.

## What

`msglc` is a Python library that provides a way to serialize and deserialize json objects with lazy/partial loading
containers using `msgpack` as the serialization format.

## Why

The `msgpack` specification and the corresponding Python library `msgpack` provide a tool to serialize json objects into
binary data.
However, the encoded data has to be fully decoded to reveal what is inside.
This becomes an issue when the data is large and only a small part of it is needed.

`msglc` provides an enhanced format to embed structure information into the encoded data.
This allows lazy and partial decoding of the data of interest, which can be a significant performance improvement.

## How

### Overview

`msglc` packs tables of contents and data into a single binary blob. The detailed layout can be shown as follows.

```text
#####################################################################
# magic bytes # 20 bytes # encoded data # encoded table of contents #
#####################################################################
```

1. The magic bytes are used to identify the format of the file.
2. The 20 bytes are used to store the start position and the length of the encoded table of contents.
3. The encoded data is the original msgpack encoded data.

The table of contents is placed at the end of the file to allow direct writing of the encoded data to the file.
This makes the memory footprint small.

### Buffering

One can configure the buffer size for reading and writing.

```python
from msglc import configure

configure(write_buffer_size=2 ** 23)
configure(read_buffer_size=2 ** 16)
```

### Table of Contents

There are two types of containers in json objects: array and object.
They correspond to `list` and `dict` in Python, respectively.

The table of contents mimics the structure of the original json object.
However, only containers that exceed a certain size are included in the table of contents.
This size is configurable and can be often set to the block size of the storage system.

```python
from msglc import configure

configure(small_obj_optimization_threshold=8192)
```

The basic structure of the table of contents of any object is a `dict` with two keys: `t` (toc) and `p` (position).
The `t` field only exists when the object is a **sufficiently large container**.

If all the elements in the container are small, the `t` field will also be omitted.

For the purpose of demonstration, the size threshold is set to 2 bytes in the following examples.

```python
# an integer is not a container
data = 2154848
toc = {"p": [0, 5]}

# a string is not a container
data = "a string"
toc = {"p": [5, 14]}

# the inner lists contain small elements, so the `t` field is omitted
# the outer list is larger than 2 bytes, so the `t` field is included
data = [[1, 1], [2, 2, 2, 2, 2]]
toc = {"t": [{"p": [15, 18]}, {"p": [18, 24]}], "p": [14, 24]}

# the outer dict is larger than 2 bytes, so the `t` field is included
# the `b` field is not a container
# the `aa` field is a container, but all its elements are small, so the `t` field is omitted
data = {'a': {'aa': [2, 2, 2, 2, 2, 2, 2, 2, 2, 2]}, 'b': 2}
toc = {"t": {"a": {"t": {"aa": {"p": [31, 42]}}, "p": [27, 42]}, "b": {"p": [44, 45]}}, "p": [24, 45]}
```

Due to the presence of the size threshold, the table of contents only requires a small amount of extra space.

### Reading

The table of contents is read first. The actual data is represented by `Dict` and `List` classes, which have similar
interfaces to the original `dict` and `list` classes in Python.

As long as the table of contents contains the `t` field, no actual data is read.
Each piece of data is read only when it is accessed, and it is cached for future use.
Thus, the data is read lazily and will only be read once (unless fast loading is enabled).

### Fast Loading

There are two ways to read a container into memory:

1. Read the entire container into memory.
2. Read each element of the container into memory one by one.

The first way only requires one system call, but data may be repeatedly read if some of its children have been read
before.
The second way requires multiple system calls, but it ensures that each piece of data is read only once.
Depending on various factors, one may be faster than the other.

Fast loading is a feature that allows the entire data to be read into memory at once.
This helps to avoid issuing multiple system calls to read the data, which can be slow if the latency is high.

```python
from msglc import configure

configure(fast_loading=True)
```

One shall also configure the threshold for fast loading.

```python
from msglc import configure

configure(fast_loading_threshold=0.5)
```

The threshold is a fraction between 0 and 1. The above 0.5 means if more than half of the children of a container have
been read already, `to_obj` will use the second way to read the whole container. Otherwise, it will use the first way.
