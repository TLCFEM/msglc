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
from io import BytesIO
from itertools import cycle

import pytest

from msglc import LazyWriter, FileInfo, combine
from msglc.config import config, increment_gc_counter, decrement_gc_counter, configure
from msglc.reader import LazyStats, LazyReader
from msglc.utility import MockIO


@pytest.fixture(scope="function")
def json_base():
    return {
        "title": "example glossary",
        "GlossDiv": {
            "title": "S",
            "GlossList": {
                "GlossEntry": {
                    "ID": "SGML",
                    "SortAs": "SGML",
                    "GlossTerm": "Standard Generalized Markup Language",
                    "Acronym": "SGML",
                    "Abbrev": "ISO 8879:1986",
                    "GlossDef": {
                        "para": "A meta-markup language, used to create markup languages such as DocBook.",
                        "GlossSeeAlso": ["GML", "XML"],
                    },
                    "GlossSee": "markup",
                }
            },
        },
        "empty_list": [],
        "none_list": [None],
    }


@pytest.fixture(scope="function")
def json_before(json_base):
    return {
        "glossary": json_base,
        "some_tuple": (1, 2, 3),
        "some_set": {1, 2, 3},
    }


@pytest.fixture(scope="function")
def json_after(json_base):
    return {
        "glossary": json_base,
        "some_tuple": [1, 2, 3],
        "some_set": [1, 2, 3],
    }


@pytest.mark.parametrize("target", ["test.msg", BytesIO()])
@pytest.mark.parametrize("size", [0, 8192])
@pytest.mark.parametrize("cached", [True, False])
def test_msglc(monkeypatch, tmpdir, json_before, json_after, target, size, cached):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", size)

    with tmpdir.as_cwd():
        if isinstance(target, BytesIO):
            target.seek(0)

        with LazyWriter(target) as writer:
            writer.write(json_before)
            with pytest.raises(ValueError):
                writer.write(json_before)

        stats = LazyStats()

        if isinstance(target, BytesIO):
            target.seek(0)

        with MockIO(target, "rb", 0, 500 * 2**20) as buffer:
            with LazyReader(buffer, counter=stats, cached=cached) as reader:
                assert reader.read("glossary/GlossDiv/GlossList/GlossEntry/GlossDef/GlossSeeAlso/1") == "XML"
                assert reader.read("glossary/empty_list") == []
                assert reader.read("glossary/none_list/0") is None
                assert reader.read() == json_after
                assert reader == json_after

                dict_container = reader.read("glossary/GlossDiv")
                assert len(dict_container) == 2
                assert dict_container.get("invalid_key") is None
                assert "invalid_key" not in dict_container
                assert set(dict_container.keys()) == {"title", "GlossList"}
                for x, _ in dict_container.items():
                    assert x in ["title", "GlossList"]

                list_container = reader.read("glossary/GlossDiv/GlossList/GlossEntry/GlossDef/GlossSeeAlso")
                assert len(list_container) == 2
                for x in list_container:
                    assert x in ["GML", "XML"]
                assert set(list_container) == {"GML", "XML"}

        str(stats)


@pytest.mark.parametrize("threshold", [256, 8192])
@pytest.mark.parametrize("cached", [True, False])
@pytest.mark.parametrize("trivial", [4, 10])
def test_list_exception(monkeypatch, tmpdir, cached, threshold, trivial):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", threshold)
    monkeypatch.setattr(config, "trivial_size", trivial)

    total_size: int = 200
    with tmpdir.as_cwd():
        with LazyWriter("test.msg") as writer:
            writer.write([float(x) for x in range(total_size)])

        with LazyReader("test.msg", cached=cached) as reader:
            with pytest.raises(TypeError):
                print(reader["invalid_index"])
            with pytest.raises(TypeError):
                print(reader[(1, 2)])
            with pytest.raises(TypeError):
                print(reader["a:2:3:4"])
            with pytest.raises(TypeError):
                print(reader["a:2"])
            with pytest.raises(TypeError):
                print(reader["a:2:"])

            assert reader[f"{-2-total_size}:"] == [198.0, 199.0]
            assert reader[f"{2*total_size-2}:"] == [198.0, 199.0]
            assert reader[f":{total_size+2}"] == [0.0, 1.0]
            assert reader[f":{-2*total_size+2}"] == [0.0, 1.0]
            assert reader[:2] == [0.0, 1.0]

            for _ in range(2 * total_size):
                x = random.randint(0, total_size - 1)
                assert reader.read(f"{x}") == float(x)

            assert [float(x) for x in range(total_size)] == reader


@pytest.mark.parametrize("threshold", [256, 8192])
@pytest.mark.parametrize("cached", [True, False])
def test_dict_exception(monkeypatch, tmpdir, cached, threshold):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", threshold)

    total_size: int = 200
    with tmpdir.as_cwd():
        with LazyWriter("test.msg") as writer:
            writer.write({str(x): float(x) for x in range(total_size)})

        with LazyReader("test.msg", cached=cached) as reader:
            assert str(2 * total_size) not in reader
            assert reader.get(str(2 * total_size)) is None
            assert len(reader.keys()) == total_size
            assert len(reader.values()) == total_size
            assert len(reader.items()) == total_size


@pytest.mark.parametrize("target", ["combined.msg", BytesIO()])
def test_combine_archives(tmpdir, json_after, target):
    with tmpdir.as_cwd():
        with LazyWriter("test_list.msg") as writer:
            writer.write([x for x in range(30)])
        with LazyWriter("test_dict.msg") as writer:
            writer.write(json_after)

        combine("combined_a.msg", [FileInfo("test_list.msg", "first_inner"), FileInfo("test_dict.msg", "second_inner")])

        if isinstance(target, BytesIO):
            target.seek(0)

        combine(target, [FileInfo("combined_a.msg", "first_outer"), FileInfo("combined_a.msg", "second_outer")])

        if isinstance(target, BytesIO):
            target.seek(0)

        with LazyReader(target) as reader:
            assert reader.read("first_outer//second_inner/glossary/title") == "example glossary"
            assert reader.read("second_outer/first_inner/2") == 2
            assert reader.read("second_outer/first_inner/-1") == 29
            assert reader.read("second_outer/first_inner/0:2") == [0, 1]
            assert reader.read("second_outer/first_inner/:2") == [0, 1]
            assert reader.read("second_outer/first_inner/28:") == [28, 29]
            assert reader.read("second_outer/first_inner/24:2:30") == [24, 26, 28]
            assert reader.read("second_outer/first_inner/:2:5") == [0, 2, 4]
            assert reader.read("second_outer/first_inner/24:2:") == [24, 26, 28]
            assert reader.visit("first_outer//second_inner/glossary/title") == "example glossary"
            assert reader.visit("second_outer/first_inner/2") == 2
            assert reader.visit("second_outer/first_inner/-1") == 29
            assert reader.visit("second_outer/first_inner/0:2") == [0, 1]
            assert reader.visit("second_outer/first_inner/:2") == [0, 1]
            assert reader.visit("second_outer/first_inner/28:") == [28, 29]
            assert reader.visit("second_outer/first_inner/24:2:30") == [24, 26, 28]
            assert reader.visit("second_outer/first_inner/:2:5") == [0, 2, 4]
            assert reader.visit("second_outer/first_inner/24:2:") == [24, 26, 28]

        if isinstance(target, BytesIO):
            target = BytesIO()

        combine(target, [FileInfo("combined_a.msg"), FileInfo("combined_a.msg")])

        if isinstance(target, BytesIO):
            target.seek(0)

        with LazyReader(target) as reader:
            assert reader.read("0/second_inner/glossary/title") == "example glossary"
            assert reader.read("1/first_inner/2") == 2
            assert reader.read("1/first_inner/-1") == 29
            assert reader.read("1/first_inner/0:2") == [0, 1]
            assert reader.read("1/first_inner/:2") == [0, 1]
            assert reader.read("1/first_inner/28:") == [28, 29]
            assert reader.read("1/first_inner/24:2:30") == [24, 26, 28]
            assert reader.read("1/first_inner/:2:5") == [0, 2, 4]
            assert reader.read("1/first_inner/24:2:") == [24, 26, 28]
            assert reader.visit("0/second_inner/glossary/title") == "example glossary"
            assert reader.visit("1/first_inner/2") == 2
            assert reader.visit("1/first_inner/-1") == 29
            assert reader.visit("1/first_inner/0:2") == [0, 1]
            assert reader.visit("1/first_inner/:2") == [0, 1]
            assert reader.visit("1/first_inner/28:") == [28, 29]
            assert reader.visit("1/first_inner/24:2:30") == [24, 26, 28]
            assert reader.visit("1/first_inner/:2:5") == [0, 2, 4]
            assert reader.visit("1/first_inner/24:2:") == [24, 26, 28]
            with LazyReader("test_list.msg") as inner_reader:
                assert reader[0]["first_inner"] == inner_reader
            with LazyReader("test_dict.msg") as inner_reader:
                assert reader[1]["second_inner"] == inner_reader

        with pytest.raises(ValueError):
            combine(target, [FileInfo("combined_a.msg", "some_name"), FileInfo("combined_a.msg")])
        with pytest.raises(ValueError):
            combine(target, [FileInfo("combined_a.msg", "some_name"), FileInfo("combined_a.msg", "some_name")])
        with pytest.raises(ValueError):
            combine(target, [FileInfo("combined_aa.msg"), FileInfo("combined_a.msg")])


def test_recursive_combine(tmpdir):
    alternate = cycle(["combined.msg", "core.msg", "core.msg", "combined.msg"])

    def token():
        return random.choice(["a", "b", "c", None])

    core = [x for x in range(10)]

    with tmpdir.as_cwd():
        with LazyWriter("core.msg") as writer:
            writer.write(core)

        path: list = []

        for _ in range(10):
            segment = token()
            target = next(alternate)
            combine(target, [FileInfo(next(alternate), segment)])
            if segment is None:
                segment = 0
            path.append(segment)

        with LazyReader(target) as reader:
            assert reader.read(list(reversed(path))) == core


def test_configure_with_valid_values():
    configure(
        small_obj_optimization_threshold=2**14,
        write_buffer_size=2**24,
        read_buffer_size=2**17,
        fast_loading=False,
        fast_loading_threshold=0.5,
        trivial_size=30,
        disable_gc=True,
        simple_repr=True,
        copy_chunk_size=2**24,
        magic=b"new_version_coming",
    )
    assert config.small_obj_optimization_threshold == 2**14
    assert config.write_buffer_size == 2**24
    assert config.read_buffer_size == 2**17
    assert config.fast_loading is False
    assert config.fast_loading_threshold == 0.5
    assert config.trivial_size == 30
    assert LazyWriter.magic.strip(b"\0") == b"new_version_coming"


def test_gc_counter_increment():
    initial_counter = increment_gc_counter()
    assert increment_gc_counter() == initial_counter + 1


def test_gc_counter_decrement():
    initial_counter = increment_gc_counter()
    assert decrement_gc_counter() == initial_counter - 1
