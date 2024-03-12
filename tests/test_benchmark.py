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

from msglc import dump, config
from msglc.generate import generate_random_json, find_all_paths, goto_path, generate, compare
from msglc.reader import LazyStats, LazyReader


def test_random_benchmark(monkeypatch, tmpdir):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 8192)

    archive = {"id": generate_random_json(5, 10)}
    path = find_all_paths(archive)
    random.shuffle(path)

    with tmpdir.as_cwd():
        dump("archive.msg", archive)

        counter = LazyStats()

        with LazyReader("archive.msg", counter=counter) as reader:
            for i in path[: min(1000, len(path))]:
                assert goto_path(archive, i) == reader.read(i)
                counter()
            assert archive == reader

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


def test_numpy_array(tmpdir):
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
        generate(depth=4, width=10)
        compare(2)
        compare(-2)


if __name__ == "__main__":
    pass
