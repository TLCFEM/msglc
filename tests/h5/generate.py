import os
from math import sqrt

import h5py
import matplotlib.pyplot as plt
import numpy as np
from timer import get_color, timeit

from msglc import dump
from msglc.config import configure


@timeit
def generate_msg(mat: np.ndarray, block: int):
    configure(small_obj_optimization_threshold=2**block, numpy_encoder=False)  # 16KB
    dump(f"data-{block}.msg", mat)


def h5_name(block: int, **kwargs):
    file_name = "data"
    if kwargs:
        file_name += "-compressed"
    elif block > 0:
        file_name += "-chunked"

    if block > 0:
        file_name += f"-{block}"

    return f"{file_name}.h5"


@timeit
def generate_h5(mat: np.ndarray, block: int, **kwargs):
    with h5py.File(h5_name(block, **kwargs), "w") as f:
        if block > 0:
            chunk_size = int(sqrt(2**block / 128))
            kwargs["chunks"] = (chunk_size, chunk_size)
        f.create_dataset("data", data=mat, **kwargs)


def plot_write_time(write_time: dict):
    x = []
    y = []
    color = []
    for k, v in sorted(write_time.items()):
        x.append(k)
        y.append(v)
        color.append(get_color(k))

    plt.figure(figsize=(10, 10))
    plt.bar(x, y, color=color)
    plt.ylabel("time (s)")
    plt.xlabel("format")
    plt.xticks(rotation=-90)
    plt.tight_layout()
    plt.savefig("write_time.pdf")


def plot_file_size(file_size: dict):
    x = []
    y = []
    color = []
    for k in sorted(file_size.keys()):
        x.append(k)
        y.append(os.path.getsize(k) / 2**20)
        color.append(get_color(k))

    plt.figure(figsize=(10, 10))
    plt.bar(x, y, color=color)
    plt.ylabel("size (MB)")
    plt.xlabel("format")
    plt.xticks(rotation=-90)
    plt.tight_layout()
    plt.savefig("file_size.pdf")


def plot_memory_usage(write_memory: dict):
    x = []
    y = []
    color = []
    for k, v in sorted(write_memory.items()):
        x.append(k)
        y.append(v)
        color.append(get_color(k))

    plt.figure(figsize=(10, 10))
    plt.bar(x, y, color=color)
    plt.ylabel("write memory usage")
    plt.xlabel("format")
    plt.xticks(rotation=-90)
    plt.tight_layout()
    plt.savefig("write_memory.pdf")


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))

    collect = {}

    mat = np.random.rand(5000, 5000)

    collect[h5_name(-1)] = generate_h5(mat, -1)

    for i in range(12, 23):
        collect[h5_name(i)] = generate_h5(mat, i)
        collect[h5_name(i, compression="gzip", compression_opts=9)] = generate_h5(
            mat, i, compression="gzip", compression_opts=9
        )
        collect[f"data-{i}.msg"] = generate_msg(mat, i)

    plot_write_time({k: v[0] for k, v in collect.items()})
    plot_file_size(collect)
    # plot_memory_usage({k: v[1] for k, v in collect.items()})
