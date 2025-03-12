# Benchmark

The embedded structure allows fast read without loading the whole archive, which is the main advantage of this package.
In the following, we benchmark the random read performance and compare with the `HDF5` format.

## Data Generation

A square matrix of size 5000 with random floating-point numbers is used.
The matrix is dumped onto the disk with different configurations.

1. For `msglc`, `small_obj_optimization_threshold` varies from 4KB to 4MB, `numpy_encoder` is switched off so the matrix is stored as plain json instead binary blob.
2. For `h5py`, the chunk size is computed so that each block has a size similar to `small_obj_optimization_threshold`. Compression is optionally switched on.

The following code snippets show the relevant functions.

```py
def generate_msg(mat: np.ndarray, block: int):
    configure(small_obj_optimization_threshold=2**block, numpy_encoder=False)  # 16KB
    dump(f"data-{block}.msg", mat)

def generate_h5(mat: np.ndarray, block: int, **kwargs):
    with h5py.File(h5_name(block, **kwargs), "w") as f:
        if block > 0:
            chunk_size = int(sqrt(2**block / 128))
            kwargs["chunks"] = (chunk_size, chunk_size)
        f.create_dataset("data", data=mat, **kwargs)
```

The write time of `msglc` is in general constant, because the packer needs to traverse the whole json object.
Depending on different configurations, `h5py` requires different amounts of time to dump the matrix.

![write time](./write_time.pdf)

`msglc` shall be used for data that is written to disk for cold storage and does not require frequent changes.
When compression is on, `h5py` needs to traverse the object just like `msglc`, thus requires a similar amount of time.

## Read Test

We mainly test the random read.
To this end, we repeatedly read random locations in the matrix and measure the time required.

```py
@timeit
def read_msg(file: str):
    with LazyReader(file, unpacker=MsgspecUnpacker, cached=False) as reader:
        for _ in range(repeat):
            reader[random.randint(0, 4999)][random.randint(0, 4999)]


@timeit
def read_h5(file: str):
    with h5py.File(file, "r") as f:
        dataset = f["data"]
        for _ in range(repeat):
            dataset[random.randint(0, 4999)][random.randint(0, 4999)]`
```

![read 1k random elements](./read_time_log_1k.pdf)

![read 10k random elements](./read_time_log_10k.pdf)