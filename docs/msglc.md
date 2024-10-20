# msglc

`msglc` is a Python library that provides a way to serialize and deserialize json objects with lazy/partial loading
containers using [`msgpack`](https://github.com/msgpack/msgpack-python) as the serialization format.

It can be used in environments that use `msgpack` to store/exchange data that is larger than a few MBs if any of the
followings hold.

1. After cold storage, each retrieval only accesses part of the stored data.
2. Cannot afford to decode the whole file due to memory limitation, performance consideration, etc.
3. Want to combine encoded data into a single blob without decoding and re-encoding the same piece of data.


## Installation

`msglc` is a pure Python library and can be installed using `pip`.

```bash
pip install msglc
```

The only dependencies are `msgpack` and `bitarray`.

### `msgspec`

[`msgspec`](https://jcristharif.com/msgspec/) is an alternative library that provides better decoding performance compared to `msgpack`.
It is recommended to use `msgspec`.

```bash
pip install msgspec[msgspec]
```

### `numpy`

`numpy` arrays can be serialized and deserialized, to use this feature, install `numpy`.

```bash
pip install msglc[numpy]
```
