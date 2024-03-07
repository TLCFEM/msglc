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

import pytest

from msglc import LazyReader, LazyWriter, FileInfo, combine
from msglc.config import config, increment_gc_counter, decrement_gc_counter, configure
from msglc.reader import LazyStats


@pytest.fixture(scope="function")
def json_example():
    return {
        "glossary": {
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
        }
    }


@pytest.mark.parametrize("size", [0, 8192])
def test_msglc(monkeypatch, tmpdir, json_example, size):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", size)

    with tmpdir.as_cwd():
        with LazyWriter("test.msg") as writer:
            writer.write(json_example)

        stats = LazyStats()

        with LazyReader("test.msg", stats) as reader:
            assert reader.read("glossary/GlossDiv/GlossList/GlossEntry/GlossDef/GlossSeeAlso/1") == "XML"
            assert reader == json_example

        stats.bytes_per_call()


def test_large_list_with_small_elements(monkeypatch, tmpdir):
    monkeypatch.setattr(config, "small_obj_optimization_threshold", 128)

    total_size: int = 2**12
    with tmpdir.as_cwd():
        with LazyWriter("test.msg") as writer:
            writer.write([x for x in range(total_size)])

        stats = LazyStats()

        with LazyReader("test.msg", stats) as reader:
            for _ in range(2**10):
                x = random.randint(0, total_size - 1)
                assert reader.read(f"{x}") == x

        stats.bytes_per_call()


def test_combine_archives(tmpdir, json_example):
    with tmpdir.as_cwd():
        with LazyWriter("test_list.msg") as writer:
            writer.write([x for x in range(30)])
        with LazyWriter("test_dict.msg") as writer:
            writer.write(json_example)

        combine("combined_a.msg", [FileInfo("first_inner", "test_list.msg"), FileInfo("second_inner", "test_dict.msg")])
        combine("combined.msg", [FileInfo("first_outer", "combined_a.msg"), FileInfo("second_outer", "combined_a.msg")])

        with LazyReader("combined.msg") as reader:
            assert reader.read("first_outer/second_inner/glossary/title") == "example glossary"
            assert reader.read("second_outer/first_inner/2") == 2
            assert reader.read("second_outer/first_inner/-1") == 29
            assert reader.read("second_outer/first_inner/0:2") == [0, 1]
            assert reader.read("second_outer/first_inner/:2") == [0, 1]
            assert reader.read("second_outer/first_inner/28:") == [28, 29]
            assert reader.read("second_outer/first_inner/24:2:30") == [24, 26, 28]


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
    )
    assert config.small_obj_optimization_threshold == 2**14
    assert config.write_buffer_size == 2**24
    assert config.read_buffer_size == 2**17
    assert config.fast_loading is False
    assert config.fast_loading_threshold == 0.5
    assert config.trivial_size == 30


def test_gc_counter_increment():
    initial_counter = increment_gc_counter()
    assert increment_gc_counter() == initial_counter + 1


def test_gc_counter_decrement():
    initial_counter = increment_gc_counter()
    assert decrement_gc_counter() == initial_counter - 1
