#  Copyright (C) 2024-2026 Theodore Chang
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

import msgpack
import pytest
from generate import (
    compare,
    find_all_paths,
    generate,
    goto_path,
)

from msglc import config, dump
from msglc.reader import LazyReader, LazyStats
from msglc.unpacker import MsgpackUnpacker, MsgspecUnpacker


def test_random_benchmark(monkeypatch, tmpdir, random_medium_data):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 8192)

    path = find_all_paths(random_medium_data)
    random.shuffle(path)

    with tmpdir.as_cwd():
        dump("archive.msg", random_medium_data)

        counter = LazyStats()

        with LazyReader("archive.msg", counter=counter) as reader:
            for i in path[: min(1000, len(path))]:
                assert goto_path(random_medium_data, i) == reader.read(i)
                counter()
            assert random_medium_data == reader

        counter.bytes_per_call()

        counter.clear()


def pack(array):
    dump(
        "large_array.msg",
        {
            "run": {
                "program_name": "VASP",
                "method": {"basis_sets": "plane waves"},
                "system": [
                    {
                        "atom_labels": ["H", "H"],
                    },
                    {"atom_labels": ["H", "H"], "symmetry": {"space_group": 4}},
                ],
            },
            "repo_entry": {"chemical_formula": "H2"},
            "large_list": array,
        },
    )


def test_pack_large_array(tmpdir, benchmark):
    def pack_large_array(_tmpdir):
        with _tmpdir.as_cwd():
            pack([float(x) for x in range(20000)])

    benchmark(pack_large_array, tmpdir)


@pytest.mark.parametrize("encoder", [True, False])
def test_numpy_array(monkeypatch, tmpdir, encoder):
    monkeypatch.setattr(config, "numpy_encoder", encoder)

    try:
        with tmpdir.as_cwd():
            import numpy

            numpy_array = numpy.random.random((10, 11, 12000))
            pack(numpy_array)

            with LazyReader("large_array.msg") as reader:
                for _ in range(100000):
                    x = random.randint(0, numpy_array.shape[0] - 1)
                    y = random.randint(0, numpy_array.shape[1] - 1)
                    z = random.randint(0, numpy_array.shape[2] - 1)
                    assert reader["large_list"][x][y][z] == numpy_array[x][y][z]
    except ImportError:
        pass


def test_compare_to_plain(tmpdir):
    with tmpdir.as_cwd():
        generate(depth=4, width=6)
        compare(2)
        compare(-2)


@pytest.fixture(scope="module")
def prepare(tmpdir_factory):
    tmp_prepare = tmpdir_factory.mktemp("prepare")
    with tmp_prepare.as_cwd():
        generate(depth=4, width=4, threshold=25)
    for file in tmp_prepare.listdir():
        print(f"{file.basename}: {file.size()} bytes")
    return tmp_prepare


@pytest.mark.parametrize("size", [x for x in range(13, 25)])
@pytest.mark.parametrize("total", [0, 1, 2, 3, 4])
@pytest.mark.parametrize(
    "unpacker", [MsgpackUnpacker(), MsgspecUnpacker()], ids=["vanilla", "msgspec"]
)
def test_matrix(prepare, benchmark, size, total, unpacker):
    with prepare.as_cwd():
        benchmark(compare, 1, size, total, unpacker)


def test_serialize_large_json(tmpdir, benchmark, repo_data):
    def serialize_large_json():
        dump("repo_data.msg", repo_data)

    with tmpdir.as_cwd():
        benchmark(serialize_large_json)


def test_random_huge_json(tmpdir, benchmark, random_huge_data):
    with tmpdir.as_cwd():
        benchmark(dump, "data.msg", random_huge_data)


def test_random_huge_json_reference(tmpdir, benchmark, random_huge_data):
    with tmpdir.as_cwd():

        def msgpack_dump():
            with open("data.msgpack", "wb") as f:
                msgpack.dump(random_huge_data, f)

        benchmark(msgpack_dump)


if __name__ == "__main__":
    pass
