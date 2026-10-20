# msglc · load `msgpack`/`cbor` lazily, not all at once

[![codecov](https://codecov.io/gh/TLCFEM/msglc/graph/badge.svg?token=JDPARZSVDR)](https://codecov.io/gh/TLCFEM/msglc)
[![PyPI](https://badge.fury.io/py/msglc.svg)](https://pypi.org/project/msglc/)

> **Your data stays a valid `msgpack`/`cbor` blob.**
> We just tack on a table of contents so you can **jump straight to the piece you need** — instead of decoding the whole
> file.

---

## 💥 The problem

`msgpack` and `cbor` are compact and fast — until your file hits a few hundred MB and you only need **one value buried
inside**.

Vanilla decoding forces you to read and decode **everything**, every time.
That is wasteful after cold storage, it blows up memory, and it makes small reads feel huge.

## ✨ The fix

`msglc` appends a tiny **table of contents** to the payload, like a ZIP central directory.
On open, only that index is read.
The value you actually want is located and decoded **directly** — nothing more.

- ⚡ **Lazy & partial** — pull one node from a multi-GB file.
- 🧩 **Still valid `msgpack`/`cbor`** — drop the index and any compliant decoder reads it.
- 🦀 **Rust backend** — serialize at near-vanilla speed, or stream custom containers.
- 🧬 **Merge archives** — combine files into one blob *without decoding and re-encoding*.
- 🌍 **Every filesystem** — local, `UPath`, `fsspec`, and S3.

## 🚀 Quick start

```python
from msglc import dump
from msglc.reader import LazyReader

dump("data.msg", {"a": [1, 2, 3], "b": {"c": 4, "d": 5, "e": [0x221548313] * 10}})

with LazyReader("data.msg") as reader:
    print(reader["b/c"])  # 4  ← only this node is decoded
    print(reader.read("b/d"))  # 5
    print(list(reader.keys()))  # ['a', 'b']
```

## 📦 Install

```bash
pip install msglc
```

Snap on some extras for extra speed / features:

```bash
pip install "msglc[msgspec]"   # faster msgpack decoding
pip install "msglc[numpy]"     # numpy.ndarray support
pip install "msglc[s3fs]"      # S3 support
pip install "msglc[all]"       # everything above
```

## 🗺️ Full documentation

Everything the README leaves out lives in the docs — cleanly split by concern:

| Page                                       | What you'll find                                                     |
|--------------------------------------------|----------------------------------------------------------------------|
| **[Guide](docs/guide.md)**                 | Serialization, deserialization, combining, streaming & custom codecs |
| **[Internals](docs/internals.md)**         | Why it's fast — the binary layout & TOC design                       |
| **[Configuration](docs/configuration.md)** | Every tunable option, its default & performance tips                 |
| **[Storage](docs/storage.md)**             | Local files, `UPath`, `fsspec` & S3                                  |
| **[API reference](docs/api/utility.md)**   | The generated reference                                              |

> ✨ *In one line*: **`msglc` = `msgpack`/`cbor` + a table of contents.**

## 🧑‍💻 Development

A working **Rust** toolchain is required.
Install it via the [official guide](https://rust-lang.org/tools/install/), then:

```bash
pip install -e ".[all]"
```

Not all optional dependencies are universally available — some are unavailable on platforms other than `CPython`.
