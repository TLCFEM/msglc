#  Copyright (C) 2024 Theodore Chang
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import annotations

import functools
import os
import random
import string
from multiprocessing import Pool

import msgpack  # type: ignore

from msglc import dump
from msglc.config import configure
from msglc.reader import LazyDict, LazyList, LazyStats, LazyReader


def generate_random_json(depth=10, width=4, simple=False):
    seed = random.random()

    def generate_token():
        return "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 10)))

    if depth == 0 or (simple and seed < 0.1):
        return random.choice(
            [
                random.randint(-(2**30), 2**30),
                random.random(),
                random.choice([True, False]),
                generate_token(),
            ]
        )

    if seed < 0.7:
        return {generate_token(): generate_random_json(depth - 1, width, True) for _ in range(width)}

    if seed < 0.95 or not simple:
        return [generate_random_json(depth - 1, width, True) for _ in range(width)]

    return [random.randint(2**10, 2**30)] * random.randint(2**10, 2**14)


def find_all_paths(json_obj, path=None, path_list=None):
    if path_list is None:
        path_list = []

    if isinstance(json_obj, (dict, LazyDict)):
        for key, value in json_obj.items():
            new_path = [key] if not path else path + [key]
            find_all_paths(value, new_path, path_list)
    elif isinstance(json_obj, (list, LazyList)):
        for index, value in enumerate(json_obj):
            new_path = [index] if not path else path + [index]
            find_all_paths(value, new_path, path_list)

    if path:
        path_list.append(path)

    return path_list


def goto_path(json_obj, path):
    target = json_obj
    for i in path:
        if isinstance(i, str) and i.isdigit() and isinstance(target, list):
            i = int(i)
        target = target[i]
    return target


def configure_and_dump(archive, block):
    configure(small_obj_optimization_threshold=2**block)
    dump(f"archive_{block}.msg", archive)


def generate(*, depth=6, width=11, threshold=23):
    archive = {"id": generate_random_json(depth, width)}
    path = find_all_paths(archive)
    indices = list(range(0, min(1_000_000, len(path))))
    random.shuffle(indices)

    with open("path.txt", "w") as f:
        for i in indices:
            f.write("/".join(map(str, path[i])) + "\n")

    with open("archive_msgpack.msg", "wb") as f:
        msgpack.dump(archive, f)

    _dump = functools.partial(configure_and_dump, archive)

    step = range(13, threshold + 1)

    with Pool(min(os.cpu_count(), len(step))) as p:
        p.map(_dump, step)


def compare(mode, size: int = 13, total: int = 5):
    accumulator: int = 0

    with open("path.txt", "r") as f:
        if mode > 0:
            counter = LazyStats()
            with LazyReader(f"archive_{size}.msg", counter=counter) as reader:
                while p := f.readline():
                    accumulator += 1
                    if accumulator == 10**total:
                        break
                    _ = reader.visit(p.strip())
            counter.clear()
        else:
            with open("archive_msgpack.msg", "rb") as fa:
                archive = msgpack.load(fa)

            while p := f.readline():
                accumulator += 1
                if accumulator == 10**total:
                    break
                _ = goto_path(archive, p.strip().split("/"))


if __name__ == "__main__":
    generate()
    # compare(2)
    # compare(-2)
