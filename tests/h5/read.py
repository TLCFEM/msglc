import os
import random

import h5py
import matplotlib.pyplot as plt
from timer import get_color, timeit

from msglc.reader import LazyReader
from msglc.unpacker import MsgspecUnpacker

repeat = 1000


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
            dataset[random.randint(0, 4999)][random.randint(0, 4999)]


def plot_read_time(time: dict, logscale=False):
    x = []
    y = []
    color = []
    for k, v in sorted(time.items()):
        x.append(k)
        y.append(v)
        color.append(get_color(k))

    plt.figure(figsize=(10, 10))
    plt.bar(x, y, color=color)
    plt.ylabel("time")
    plt.xlabel("format")
    plt.xticks(rotation=-90)
    if logscale:
        plt.yscale("log")
    plt.tight_layout()
    plt.savefig(f"read_time{'_log' if logscale else ''}.pdf")


def plot_memory_usage(memory: dict):
    x = []
    y = []
    color = []
    for k, v in sorted(memory.items()):
        x.append(k)
        y.append(v)
        color.append(get_color(k))

    plt.figure(figsize=(10, 10))
    plt.bar(x, y, color=color)
    plt.ylabel("memory usage")
    plt.xlabel("format")
    plt.xticks(rotation=-90)
    plt.tight_layout()
    plt.savefig("read_memory_usage.pdf")


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))

    collect = {}
    for file in os.listdir():
        if "data" not in file:
            continue
        if "msg" in file:
            collect[file] = read_msg(file)
        elif "h5" in file:
            collect[file] = read_h5(file)

    time_dict = {k: v[0] for k, v in collect.items()}
    memory_dict = {k: v[1] for k, v in collect.items()}
    plot_read_time(time_dict)
    plot_read_time(time_dict, logscale=True)
    # plot_memory_usage(memory_dict)
