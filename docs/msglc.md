# msglc

`msglc` is a Python library that provides a way to serialize and deserialize json objects with lazy/partial loading
containers using `msgpack` as the serialization format.

It can be used in environments that use `msgpack` to store/exchange data that is larger than a few MBs if any of the
followings hold.

1. After cold storage, each retrieval only accesses part of the stored data.
2. Cannot afford to decode the whole file due to memory limitation, performance consideration, etc.
3. Want to combine encoded data into a single blob without decoding and re-encoding the same piece of data.
