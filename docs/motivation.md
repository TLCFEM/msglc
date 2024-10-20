# Motivation

[MessagePack](https://msgpack.org/index.html) is a binary serialization format.
It can be used for data exchange and storage.

When it is used for storing large tree-like data, it has a significant shortcoming.
That is, whenever one wants to read some data from cold storage, even if only a small amount of data is required, the whole binary blob needs to be de-serialized.

This is due to the fact that `MessagePack` itself does not generate/store any structural metadata regarding the data to be serialized.
As a result, there is no way to know when the desired segment is located in the binary blob.
Thus, the whole thing needs to be decoded.
(To be precise, it is actually linear complexity, the worse case is to decode the whole thing.)


## Structural Information

Similar to the [design](https://en.wikipedia.org/wiki/ZIP_(file_format)) of the zip format, it is possible to append a table of contents to the binary blob.
The table of contents contains the structural information of the serialized data, serves as a lookup table, and is encoded as well.

Whenever read operations are required, the table of contents is read first.
Ideally, the table of contents is much smaller than the original binary blob.
Thus, the overhead of reading the table of contents is negligible.
After the table of contents is read, the desired segment can be located and decoded directly.
This would significantly reduce the amount of data that needs to be decoded if only a small amount of data is required.

### Primitive Types and Small Containers

For primitive types, the table of contents stores the start and end positions of the serialized data.

For example, the table of contents of the integer `a=10251585` would look like as follows.

```py
{
    "p": [start, end]
}
```

The `p` stands for position, and only a single character is used to save space.

This applies to small containers.

### Dictionary/Map

Apart from the `p` field that stores the start and end positions of the whole dictionary, to allow nested lookups, the table of contents also contains the tables of contents of the children under the `t` field.

For example, the table of contents of the dictionary `d={"a": 1, "b": 2}` would look like as follows.

```py
{
    "p": [start, end],
    "t": {
        "a": {"p": [start, end]},
        "b": {"p": [start, end]}
    }
}
```

The `t` stands for table, and only a single character is used to save space.
The `t` field is a dictionary.

### List/Array

Similar to the dictionary, the table of contents of a list also contains the tables of contents of the children.

For example, the table of contents of the list `l=[1, 2, 3]` would look like as follows.

```py
{
    "p": [start, end],
    "t": [
        {"p": [start, end]},
        {"p": [start, end]},
        {"p": [start, end]}
    ]
}
```

The `t` field is a list.

### List of Small Objects

For lists of small objects, storing the table of contents of each child is not efficient.
An alternative format of table of contents is used.

For example, the table of contents of the list `l=[x for x in range(100000)]` would look like as follows.

```py
{
    "p": [[size, start, end], [size, start, end], ...]
}
```

The small objects are grouped together, and the `size` field is used to indicate the number of small objects in the group.
The size of the group can be adjusted to achieve optimal performance.

### Nested Structures

All above formats contain a `p` field.

To support recursively packing serialized data, the following format is also used.

```py
# for packing a dict of packed data
{
    "t": {
        "a": start,
        "b": start,
        ...
    }
}

# for packing a list of packed data
{
    "t": [
        start,
        start,
        ...
    ]
}
```

Since the packed data is already serialized, it would contain the table of contents.
It is only necessary to store the start position of the packed data.


### Small Objects

If the data to be serialized is mainly structure, generating the corresponding table of contents would potentially result in a table of contents that is larger than the original data.

It is possible to define a threshold, such that for any given node in the tree, if the size of the node is smaller than the threshold, the node is considered a small object.
For small objects, the `t` field is omitted.

The following is an example.

```py
# data
{
    "id": [
        {
            "BlYFs": {
                "KNzFKfIR2": [True, False],
                "DZFf0InHcO": {"t32qEJJPII": 820701623, "RuUbcdXGT": 0.07535274189499452},
            },
            "SWCWj": {
                "T5Jm7j1p99": {"yEsYr8Ww": "1lgCDlDR", "1041dt7DYk": "XQUFG"},
                "ZJejJRP": {"SCIVA7Lb": 0.5045895502672991, "p5I3XN3": True},
            },
        },
        {
            "vRpNA5": {
                "0HNVOgUVHs": {"EsvObl4Q3": -1008950541, "SacDVqMG": -764697401},
                "XLK694": {"UdRKNQBrku": "64jiA4nTf", "dTPdzp7Cd": "bC6R6Q"},
            },
            "3uyABlBlY": {"7umSPsl7": {"gFa9yuPyQ": 0.24175848344688433, "UYa6UiMDZ7": True}, "zuP2wLok": "G9k2y"},
        },
    ]
}

# table of contents (full)
{
    "t": {
        "id": {
            "t": [
                {
                    "t": {
                        "BlYFs": {
                            "t": {
                                "KNzFKfIR2": {"p": [23, 26]},
                                "DZFf0InHcO": {
                                    "t": {"t32qEJJPII": {"p": [49, 54]}, "RuUbcdXGT": {"p": [64, 73]}},
                                    "p": [37, 73],
                                },
                            },
                            "p": [12, 73],
                        },
                        "SWCWj": {
                            "t": {
                                "T5Jm7j1p99": {
                                    "t": {"yEsYr8Ww": {"p": [101, 110]}, "1041dt7DYk": {"p": [121, 127]}},
                                    "p": [91, 127],
                                },
                                "ZJejJRP": {
                                    "t": {"SCIVA7Lb": {"p": [145, 154]}, "p5I3XN3": {"p": [162, 163]}},
                                    "p": [135, 163],
                                },
                            },
                            "p": [79, 163],
                        },
                    },
                    "p": [5, 163],
                },
                {
                    "t": {
                        "vRpNA5": {
                            "t": {
                                "0HNVOgUVHs": {
                                    "t": {"EsvObl4Q3": {"p": [194, 199]}, "SacDVqMG": {"p": [208, 213]}},
                                    "p": [183, 213],
                                },
                                "XLK694": {
                                    "t": {"UdRKNQBrku": {"p": [232, 242]}, "dTPdzp7Cd": {"p": [252, 259]}},
                                    "p": [220, 259],
                                },
                            },
                            "p": [171, 259],
                        },
                        "3uyABlBlY": {
                            "t": {
                                "7umSPsl7": {
                                    "t": {"gFa9yuPyQ": {"p": [290, 299]}, "UYa6UiMDZ7": {"p": [310, 311]}},
                                    "p": [279, 311],
                                },
                                "zuP2wLok": {"p": [320, 326]},
                            },
                            "p": [269, 326],
                        },
                    },
                    "p": [163, 326],
                },
            ],
            "p": [4, 326],
        }
    },
    "p": [0, 326],
}

# table of contents (minimum block size 10 bytes)
{
    "t": {
        "id": {
            "t": [
                {
                    "t": {
                        "BlYFs": {"t": {"KNzFKfIR2": {"p": [23, 26]}, "DZFf0InHcO": {"p": [37, 73]}}, "p": [12, 73]},
                        "SWCWj": {"t": {"T5Jm7j1p99": {"p": [91, 127]}, "ZJejJRP": {"p": [135, 163]}}, "p": [79, 163]},
                    },
                    "p": [5, 163],
                },
                {
                    "t": {
                        "vRpNA5": {
                            "t": {"0HNVOgUVHs": {"p": [183, 213]}, "XLK694": {"p": [220, 259]}},
                            "p": [171, 259],
                        },
                        "3uyABlBlY": {
                            "t": {"7umSPsl7": {"p": [279, 311]}, "zuP2wLok": {"p": [320, 326]}},
                            "p": [269, 326],
                        },
                    },
                    "p": [163, 326],
                },
            ],
            "p": [4, 326],
        }
    },
    "p": [0, 326],
}

# table of contents (minimum block size 100 bytes)
{
    "t": {
        "id": {
            "t": [
                {"t": {"BlYFs": {"p": [12, 73]}, "SWCWj": {"p": [79, 163]}}, "p": [5, 163]},
                {"t": {"vRpNA5": {"p": [171, 259]}, "3uyABlBlY": {"p": [269, 326]}}, "p": [163, 326]},
            ],
            "p": [4, 326],
        }
    },
    "p": [0, 326],
}

# table of contents (minimum block size 1000 bytes)
{"p": [0, 326]}
```

By controlling the block size, one can determine which format shall be used for a specific node.
A small block size would result in a more detailed table of contents, its size would be relatively larger.
But each actual read operation would be smaller.
A large block size would result in a more compact table of contents, its size would be relatively smaller.
But each actual read operation would be larger.

It is guaranteed that each read operation would fetch at least one block of data.
Depending on the actual operating system and the underlying storage hardware, the optimal block size may vary.
One shall experiment with different block sizes to find the optimal one.

## Internal Layout

For a single serialized file, the following layout is used.

```
#####################################################################
# magic bytes # 20 bytes # encoded data # encoded table of contents #
#####################################################################
```

It contains four parts.

1. The magic bytes are used to identify the format of the file.
2. The 20 bytes are used to store the start position and the length of the encoded table of contents.
3. The encoded data.
4. The encoded table of contents is the table of contents encoded in msgpack.

The table of contents is placed at the end of the file to allow direct writing of the encoded data to the file.
This makes the memory footprint small.

The encoded data is self-contained.
It can be read and decoded without the table of contents.

The same layout can be recursively used.
