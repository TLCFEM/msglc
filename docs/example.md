# Examples

## Serialization

### Dumping one object to a file

Use `dump` to serialize a json object to a file.

```python
from msglc import dump

data = {"a": [1, 2, 3], "b": {"c": 4, "d": 5, "e": [0x221548313] * 10}}
dump("data.msg", data)
```

### Combining several files

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

## Deserialization

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

## Streaming Data

The data fed to the writer does not need to be fully generated in advance.
It is possible to generate data on the fly.

The writer expects and recognizes `collections.abc.Mapping` objects.
It is thus possible to fake a dictionary with items generated from generators.

The following is a minimum implementation.

```python
from collections.abc import Generator, Mapping


class DictStream(Mapping):
    def __init__(self, generator: Generator, length: int):
        self._len = length
        self._gen = generator

    def __iter__(self): ...  # not used by writer but has to be implemented

    def __getitem__(self, key, /): ...  # not used by writer but has to be implemented

    def __len__(self):
        # required
        # note that the length needs to be known in advance
        # you do not want to get it from the generator as doing so consumes it
        return self._len

    def items(self):
        # required
        yield from self._gen
```

!!! warning "length requirement"
    Only two things will be invoked: `len()` and `.items()`.
    Thus, `__len__(self)` and `items(self)` must be properly implemented.
    If the length is **not** known in advance, streaming data is not feasible.

With the above, one can do the following.

```python
from msglc import dump
from msglc.reader import LazyReader


def example():
    yield "a", 1
    yield "b", 2


target = "example.msg"
dump(target, DictSteam(example(), 2))

with LazyReader(target) as reader:
    assert reader == {"a": 1, "b": 2}
```
