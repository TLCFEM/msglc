# Configuration

`msglc` is configured through a single global object.
Import `configure` and pass only the options you want to change.

```python
from msglc.config import configure

configure(small_obj_optimization_threshold=2**20)
```

The current values are always available on `config`.

```python
from msglc.config import config

print(config.small_obj_optimization_threshold)  # -> 1048576
```

!!! warning "`configure` resets `fs` and `compatibility_check`"
    Every call to `configure` assigns `fs` and `compatibility_check`.
    If you set a global `fs` and later change an unrelated option, the global `fs` is cleared unless you pass it again.
    Set `fs` last, or pass it on each call.
    The same applies to `compatibility_check`.

## Options

| Option | Default | Description |
| --- | --- | --- |
| `small_obj_optimization_threshold` | `8192` (8 KiB) | Nodes smaller than this are not indexed and are decoded as a whole. Also the block size for lists of trivial items. |
| `trivial_size` | `20` | Nodes no larger than this are considered trivial and may be grouped into blocks. Kept `<= small_obj_optimization_threshold`. |
| `fast_loading` | `True` | Read a whole container in one call instead of child by child. |
| `fast_loading_threshold` | `0.3` | Fraction of already-read children below which fast loading kicks in. |
| `write_buffer_size` | `8388608` (8 MiB) | Buffer size used while writing. |
| `read_buffer_size` | `65536` (64 KiB) | Buffer size used while reading. |
| `copy_chunk_size` | `16777216` (16 MiB) | Chunk size used when copying data while combining. |
| `disable_gc` | `False` | Disable the garbage collector while a reader/writer is active. |
| `simple_repr` | `True` | Make `repr` cheap by not reading data. |
| `numpy_encoder` | `False` | Store `numpy.ndarray` as a binary blob instead of nested lists. |
| `numpy_fast_int_pack` | `False` | Assume integer arrays have fixed-width elements when packing. |
| `enable_custom_container` | `True` | Recognize `dict`/`list` subclasses for streaming (rust backend). |
| `has_set` | `True` | Recognize `set` and pack as a sorted list (rust backend). |
| `has_tuple` | `True` | Recognize `tuple` and pack as a list (rust backend). |
| `has_numpy` | `True` | Recognize `numpy.ndarray` (rust backend). |
| `fs` | `None` | Default `fsspec` filesystem used when a path is given without `fs`. |
| `compatibility_check` | `None` | Callable `bytes -> bool` replacing the default magic check. |
| `magic` | `b"msglc-2024"` | Magic bytes, up to 30. Set through `configure`, not stored on `config`. |

### Buffer sizes

```python
from msglc.config import configure

configure(write_buffer_size=2**23)  # 8 MiB
configure(read_buffer_size=2**16)  # 64 KiB
configure(copy_chunk_size=2**24)  # 16 MiB, used when combining
```

### Block size

`small_obj_optimization_threshold` trades table-of-contents size against read size.
See [Choosing the block size](internals.md#choosing-the-block-size).

```python
from msglc.config import configure

configure(small_obj_optimization_threshold=2**20)  # 1 MiB
```

`trivial_size` identifies elements small enough to be grouped into blocks.
64-bit integers and doubles need about 9 bytes, so a value around 10–20 is typical.

```python
from msglc.config import configure

configure(trivial_size=10)
```

`trivial_size` and `small_obj_optimization_threshold` are kept consistent: setting one above the other raises the other to match.

### Fast loading

```python
from msglc.config import configure

configure(fast_loading=True)
configure(fast_loading_threshold=0.5)
```

### Garbage collection

Disabling `gc` during (de)serialization can improve throughput.
A global counter keeps it disabled only while at least one reader or writer is active.

```python
from msglc.config import configure

configure(disable_gc=True)
```

### Representation

With `simple_repr=True`, `repr` never reads data.
Set it to `False` to see the decoded value in `repr`.

```python
from msglc import dump
from msglc.config import configure
from msglc.reader import LazyReader

dump("data.msg", {"a": [1, 2, 3], "b": {"c": 4}})

configure(simple_repr=False)
with LazyReader("data.msg") as reader:
    print(repr(reader))  # -> {'a': [1, 2, 3], 'b': {'c': 4}}
configure(simple_repr=True)
with LazyReader("data.msg") as reader:
    print(repr(reader))  # -> LazyReader (data.msg)
```

## Performance

The `python` and `rust` backends detect extra types differently:

- The `python` backend always recognizes `tuple`, `set`, `numpy.ndarray` and container subclasses.
- The `rust` backend only recognizes them when the corresponding option is enabled, so those checks can be turned off.

If the data contains only plain JSON types and you write with the `rust` backend, disable the checks to reduce per-value overhead.

```python
from msglc.config import configure

# plain list / dict only, no streaming containers
configure(enable_custom_container=False)

# no set / tuple / numpy values
configure(has_set=False, has_tuple=False, has_numpy=False)
```

```python
from msglc import dump
from msglc.config import configure
from msglc.reader import LazyReader

configure(
    enable_custom_container=False,
    has_set=False,
    has_tuple=False,
    has_numpy=False,
)

dump("data.msg", {"a": [1, 2, 3], "b": {"c": 4}}, backend="rust")
with LazyReader("data.msg") as reader:
    print(reader.to_obj())  # -> {'a': [1, 2, 3], 'b': {'c': 4}}
```

!!! note
    Only disable a check if you are sure the corresponding type is absent.
    With the rust backend an unrecognized extra type raises an error instead of being converted.
