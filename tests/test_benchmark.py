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

import random
import string

from msglc import dump
from msglc.config import config
from msglc.reader import LazyDict, LazyList, ReaderStats, Reader, to_obj


def generate_random_json(depth=10, width=4, simple=False):
    seed = random.random()

    if depth == 0 or (simple and seed < 0.4):
        return random.choice(
            [
                random.randint(-(2**30), 2**30),
                random.random(),
                random.choice([True, False]),
                "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 10))),
            ]
        )

    if seed < 0.7:
        return {
            "".join(random.choices(string.ascii_lowercase, k=random.randint(5, 10))): generate_random_json(
                depth - 1, width, True
            )
            for _ in range(width)
        }

    return [generate_random_json(depth - 1, width, True) for _ in range(width)]


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


def test_random_benchmark(monkeypatch, tmpdir):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 8192)

    archive = {f"id": generate_random_json(6, 10)}
    path = find_all_paths(archive)
    random.shuffle(path)

    with tmpdir.as_cwd():
        dump("archive.msg", archive)

        counter = ReaderStats()

        with Reader("archive.msg", counter=counter) as reader:
            for i in path[: min(1000, len(path))]:
                _ = to_obj(reader.read(i))
                counter()

        counter.bytes_per_call()

        counter.clear()
