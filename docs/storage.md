# Storage

`msglc` reads from and writes to any object exposing `seek`, `read` and `write`, and integrates with [`fsspec`](https://filesystem-spec.readthedocs.io/) so remote filesystems work transparently.

## Local files and buffers

```python
from io import BytesIO
from msglc import dump, LazyWriter
from msglc.reader import LazyReader

# a path
dump("data.msg", {"a": 1})

# an in-memory buffer
buffer = BytesIO()
with LazyWriter(buffer) as writer:
    writer.write({"a": 1})
buffer.seek(0)
with LazyReader(buffer) as reader:
    print(reader.to_obj())  # -> {'a': 1}

# an open binary file object
with open("data.msg", "rb") as f:
    with LazyReader(f) as reader:
        print(reader.to_obj())  # -> {'a': 1}
```

When a `str` path or `UPath` is passed, the reader owns and closes the file.
When an object is passed, the caller keeps ownership.

## UPath

[`UPath`](https://github.com/fsspec/universal_pathlib) gives a single path type for many filesystems.

```python
from upath import UPath
from msglc import dump
from msglc.reader import LazyReader

path = UPath("memory://data/example.msg")
dump(path, {"a": 1})
with LazyReader(path) as reader:
    print(reader.to_obj())  # -> {'a': 1}
```

## fsspec filesystems

Pass any `fsspec` filesystem with `fs=`.
It works with `dump`, `LazyWriter`, `LazyReader`, `combine` and `append`.

```python
import fsspec
from msglc import combine, FileInfo
from msglc.reader import LazyReader
from msglc.writer import LazyWriter

fs = fsspec.filesystem("memory")

with LazyWriter("bucket/a.msg", fs=fs) as writer:
    writer.write({"a": 1})
with LazyWriter("bucket/b.msg", fs=fs) as writer:
    writer.write([1, 2, 3])

with LazyReader("bucket/a.msg", fs=fs) as reader:
    print(reader.to_obj())  # -> {'a': 1}

combine(
    "bucket/combined.msg",
    [FileInfo("bucket/a.msg", fs=fs), FileInfo("bucket/b.msg", fs=fs)],
    fs=fs,
)
with LazyReader("bucket/combined.msg", fs=fs) as reader:
    print(reader["0/a"], reader["1/1"])  # -> 1 2
```

Set a default filesystem once so `fs=` can be omitted.

!!! warning
    `configure` resets `fs` on every call.
    Set it last, or pass it again.
    See [Configuration](configuration.md).

```python
import fsspec
from msglc import dump
from msglc.config import configure
from msglc.reader import LazyReader

configure(fs=fsspec.filesystem("memory"))
dump("data.msg", {"a": 1})
with LazyReader("data.msg") as reader:
    print(reader.to_obj())  # -> {'a': 1}
configure(fs=None)
```

Inputs of `combine` may live on different filesystems: give each `FileInfo` its own `fs=`.

## S3

Install [`s3fs`](https://s3fs.readthedocs.io/).

```bash
pip install "msglc[s3fs]"
```

### Reading

Either open an `S3File` yourself and pass it in, or pass the path together with the filesystem.

```python
from s3fs import S3FileSystem
from msglc.reader import LazyReader

s3 = S3FileSystem()

# option 1: an S3File object
with s3.open("my-bucket/my-file", "rb") as f:
    with LazyReader(f) as reader:
        ...

# option 2: a path plus the filesystem
with LazyReader("my-bucket/my-file", fs=s3) as reader:
    ...
```

### Writing, combining and appending

For writers only option 2 works: pass a path plus `fs=`.
Passing an `S3File` is not supported, because S3 object writes are sequential while the TOC has to be written back near the start of the file.

```python
from s3fs import S3FileSystem
from msglc import FileInfo, combine
from msglc.writer import LazyWriter

s3 = S3FileSystem()

with LazyWriter("my-bucket/my-file", fs=s3) as writer:
    writer.write({"a": 1})

combine("my-bucket/combined", [FileInfo("my-bucket/child", fs=s3)], fs=s3)
```

!!! note "How remote writes work"
    Because the TOC is finalized only after all data is written, `msglc` writes to a local temporary file and uploads the complete blob at the end.

!!! warning "Local disk requirement"
    The local temporary file holds the whole blob, so the machine needs enough local disk space to accommodate it.

### Local demo with RustFS

The following starts a `RustFS` server locally for testing.

```bash
docker run -d --rm -p 9000:9000 rustfs/rustfs:latest
```

Then write and read using the default credentials.

```python
import s3fs
from msglc.reader import LazyReader
from msglc.writer import LazyWriter

fs = s3fs.S3FileSystem(
    key="msglcadmin",
    secret="msglcadmin",
    endpoint_url="http://localhost:9000",
)

bucket = "test-bucket"
fs.mkdir(bucket)
object_name = f"{bucket}/example"

with LazyWriter(object_name, fs=fs) as writer:
    writer.write({"hello": "world"})

with LazyReader(object_name, fs=fs) as reader:
    print(reader.to_obj())  # -> {'hello': 'world'}
```

### Other filesystems via pyarrow

Any `fsspec`-compatible filesystem works, including ones wrapped by [`pyarrow`](https://arrow.apache.org/docs/python/filesystems.html).

```python
from fsspec.implementations.arrow import ArrowFSWrapper
from pyarrow.fs import S3FileSystem

from msglc.reader import LazyReader
from msglc.writer import LazyWriter

fs = ArrowFSWrapper(
    S3FileSystem(
        access_key="msglcadmin",
        secret_key="msglcadmin",
        endpoint_override="localhost:9000",
        scheme="http",
        allow_bucket_creation=True,
        allow_bucket_deletion=True,
    )
)

bucket = "test-bucket"
fs.mkdir(bucket)
object_name = f"{bucket}/example"

with LazyWriter(object_name, fs=fs) as writer:
    writer.write({"hello": "world"})

with LazyReader(object_name, fs=fs) as reader:
    print(reader.to_obj())  # -> {'hello': 'world'}
```

## Simulating slow storage

`MockIO` wraps a buffer and adds configurable seek latency and read throughput.
It is useful for exercising the lazy-reading logic under realistic I/O conditions.

```python
from msglc import dump
from msglc.reader import LazyReader
from msglc.utility import MockIO

dump("data.msg", {"a": [1, 2, 3]})

with MockIO("data.msg", "rb", seek_delay=0.0, read_speed=1 << 20) as buffer:
    with LazyReader(buffer) as reader:
        print(reader.to_obj())  # -> {'a': [1, 2, 3]}
```

`read_speed` is bytes per second, or a list of `(min_size, speed)` pairs for size-dependent behavior.
`seek_delay` is the delay in seconds applied to each `seek`.
