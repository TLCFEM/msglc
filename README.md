# msglc --- (de)serialize json objects with lazy/partial loading containers using msgpack

[![codecov](https://codecov.io/gh/TLCFEM/msglc/graph/badge.svg?token=JDPARZSVDR)](https://codecov.io/gh/TLCFEM/msglc)
[![PyPI version](https://badge.fury.io/py/msglc.svg)](https://pypi.org/project/msglc/)

## What

`msglc` is a Python library that provides a way to serialize and deserialize json objects with lazy/partial loading
containers using `msgpack` as the serialization format.

It can be used in environments that use `msgpack` to store/exchange data that is larger than a few MBs if any of the
followings hold.

1. After cold storage, each retrieval only accesses part of the stored data.
2. Cannot afford to decode the whole file due to memory limitation, performance consideration, etc.
3. Want to combine encoded data into a single blob without decoding and re-encoding the same piece of data.

One may want to check the [benchmark](https://github.com/TLCFEM/msglc/wiki/Benchmark).

## Quick Start

### Serialization

Use `dump` to serialize a json object to a file.

```python
from msglc import dump

data = {"a": [1, 2, 3], "b": {"c": 4, "d": 5, "e": [0x221548313] * 10}}
dump("data.msg", data)
```

Use `combine` to combine several serialized files together.
The combined files can be further combined.

#### Combine as `dict`

```python
from msglc import dump, combine, FileInfo
from msglc.reader import LazyReader

dump("dict.msg", {str(v): v for v in range(1000)})
dump("list.msg", [float(v) for v in range(1000)])

combine("combined.msg", [FileInfo("dict.msg", "dict"), FileInfo("list.msg", "list")])
# support recursively combining files
# ...

# the combined file uses a dict layout
# { 'dict' : {'1':1,'2':2,...}, 'list' : [1.0,2.0,3.0,...] }
# so one can read it as follows, details in coming section
with LazyReader("combined.msg") as reader:
    assert reader['dict/101'] == 101  # also reader['dict'][101]
    assert reader['list/101'] == 101.0  # also reader['list'][101]
```

#### Combine as `list`

```python
from msglc import dump, combine, FileInfo
from msglc.reader import LazyReader

dump("dict.msg", {str(v): v for v in range(1000)})
dump("list.msg", [float(v) for v in range(1000)])

combine("combined.msg", [FileInfo("dict.msg"), FileInfo("list.msg")])
# support recursively combining files
# ...

# the combined file uses a list layout
# [ {'1':1,'2':2,...}, [1.0,2.0,3.0,...] ]
# so one can read it as follows, details in coming section
with LazyReader("combined.msg") as reader:
    assert reader['0/101'] == 101  # also reader[0][101]
    assert reader['1/101'] == 101.0  # also reader[1][101]
```

### Deserialization

Use `LazyReader` to read a file.

```python
from msglc.reader import LazyReader, to_obj

with LazyReader("data.msg") as reader:
    data = reader.read()  # return a LazyDict, LazyList, dict, list or primitive value
    data = reader["b/c"]  # subscriptable if the actual data is subscriptable
    # data = reader[2:]  # also support slicing if its underlying data is list compatible
    data = reader.read("b/c")  # or provide a path to visit a particular node
    print(data)  # 4
    b_dict = reader.read("b")
    print(b_dict.__class__)  # <class 'msglc.reader.LazyDict'>
    for k, v in b_dict.items():  # dict compatible
        if k != "e":
            print(k, v)  # c 4, d 5
    b_json = to_obj(b_dict)  # ensure plain dict
```

Please note all data operations shall be performed inside the `with` block.

All data is lazily loaded, use `to_obj()` function to ensure it is properly read, especially when the data goes out of
the `with` block.

If there is no need to cache the read data, pass the argument `cached=False` to the initializer.

```python
from msglc.reader import LazyReader, to_obj

with LazyReader("data.msg", cached=False) as reader:
    data = to_obj(reader.read('some/path/to/the/target'))
```

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
from msglc.config import configure

configure(write_buffer_size=2 ** 23)
configure(read_buffer_size=2 ** 16)
```

Combining multiple files into a single one requires copying data from one file to another.
Adjust `copy_chunk_size` to control memory footprint.

```python
from msglc.config import configure

configure(copy_chunk_size=2 ** 24)  # 16 MB
```

### Table of Contents

There are two types of containers in json objects: array and object.
They correspond to `list` and `dict` in Python, respectively.

The table of contents mimics the structure of the original json object.
However, only containers that exceed a certain size are included in the table of contents.
This size is configurable and can be often set to the multiple of the block size of the storage system.

```python
from msglc.config import configure

configure(small_obj_optimization_threshold=2 ** 20)
```

The above configuration assigns a threshold of 1 MB, containers larger than 1 MB will be indexed in the table of
contents.
To achieve optimal performance, one shall configure this value according to the underlying file system.

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

The table of contents is read first. The actual data is represented by `LazyDict` and `LazyList` classes, which have
similar interfaces to the original `dict` and `list` classes in Python.

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
from msglc.config import configure

configure(fast_loading=True)
```

One shall also configure the threshold for fast loading.

```python
from msglc.config import configure

configure(fast_loading_threshold=0.5)
```

The threshold is a fraction between 0 and 1. The above 0.5 means if more than half of the children of a container have
been read already, `to_obj` will use the second way to read the whole container. Otherwise, it will use the first way.

### Detection of Long List with Small Elements

Longs lists with small elements, such as integers and floats, can be further optimized by grouping elements into blocks
that are of the size of `small_obj_optimization_threshold` so that small reads can be avoided.

Set a `trivial_size` to the desired bytes to identify those long lists.
For example, the following sets it to 10 bytes, long lists of integers and floats will be grouped into blocks.
64-bit integers and doubles require 8 bytes (data) + 1 byte (type) = 9 bytes.

```python
from msglc.config import configure

configure(trivial_size=10)
```

### Disable GC

To improve performance, `gc` can be disabled during (de)serialization.
It is controlled by a global counter, as long as there is one active writer/reader, `gc` will stay disabled.

```python
from msglc.config import configure

configure(disable_gc=True)
```

### Default Values

```python
from dataclasses import dataclass


@dataclass
class Config:
    small_obj_optimization_threshold: int = 2 ** 13  # 8KB
    write_buffer_size: int = 2 ** 23  # 8MB
    read_buffer_size: int = 2 ** 16  # 64KB
    fast_loading: bool = True
    fast_loading_threshold: float = 0.3
    trivial_size: int = 20
    disable_gc: bool = True
    simple_repr: bool = True
    copy_chunk_size: int = 2 ** 24  # 16MB
```