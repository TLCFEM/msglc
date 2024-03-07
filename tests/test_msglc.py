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
import pytest

from msglc import Reader, Writer
from msglc.config import config, increment_gc_counter, decrement_gc_counter, configure
from msglc.reader import ReaderStats


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
        with Writer("test.msg") as writer:
            writer.write(json_example)

        stats = ReaderStats()

        with Reader("test.msg", stats) as reader:
            assert reader.read("glossary/GlossDiv/GlossList/GlossEntry/GlossDef/GlossSeeAlso/1") == "XML"
            assert reader.to_obj() == json_example

        stats.bytes_per_call()


def test_configure_with_valid_values():
    configure(
        small_obj_optimization_threshold=2**14,
        write_buffer_size=2**24,
        read_buffer_size=2**17,
        fast_loading=False,
        fast_loading_threshold=0.5,
        trivial_size=30,
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
