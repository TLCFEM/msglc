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

import gc


class Config:
    def __init__(self):
        self.small_obj_optimization_threshold: int = 2**13  # 8KB
        self.write_buffer_size: int = 2**23  # 8MB
        self.read_buffer_size: int = 2**16  # 64KB
        self.fast_loading: bool = True
        self.fast_loading_threshold: float = 0.3
        self.trivial_size: int = 20
        self.disable_gc: bool = True
        self.simple_repr: bool = True


config = Config()


def configure(**kwargs):
    """
    This function is used to configure the settings. It accepts any number of keyword arguments.
    The function updates the values of the configuration parameters if they are provided in the arguments.
    """
    if small_obj_optimization_threshold := kwargs.get("small_obj_optimization_threshold", None):
        if isinstance(small_obj_optimization_threshold, int) and small_obj_optimization_threshold > 0:
            config.small_obj_optimization_threshold = small_obj_optimization_threshold
            if config.trivial_size > config.small_obj_optimization_threshold:
                config.trivial_size = config.small_obj_optimization_threshold

    if write_buffer_size := kwargs.get("write_buffer_size", None):
        if isinstance(write_buffer_size, int) and write_buffer_size > 0:
            config.write_buffer_size = write_buffer_size

    if read_buffer_size := kwargs.get("read_buffer_size", None):
        if isinstance(read_buffer_size, int) and read_buffer_size > 0:
            config.read_buffer_size = read_buffer_size

    if (fast_loading := kwargs.get("fast_loading", None)) is not None:
        if isinstance(fast_loading, bool):
            config.fast_loading = fast_loading

    if fast_loading_threshold := kwargs.get("fast_loading_threshold", None):
        if isinstance(fast_loading_threshold, (int, float)) and 0 <= fast_loading_threshold <= 1:
            config.fast_loading_threshold = fast_loading_threshold

    if trivial_size := kwargs.get("trivial_size", None):
        if isinstance(trivial_size, int) and trivial_size > 0:
            config.trivial_size = trivial_size
            if config.trivial_size > config.small_obj_optimization_threshold:
                config.small_obj_optimization_threshold = config.trivial_size

    if (disable_gc := kwargs.get("disable_gc", None)) is not None:
        if isinstance(disable_gc, bool):
            config.disable_gc = disable_gc

    if (simple_repr := kwargs.get("simple_repr", None)) is not None:
        if isinstance(simple_repr, bool):
            config.simple_repr = simple_repr


__gc_counter: int = 0


def increment_gc_counter():
    global __gc_counter
    if config.disable_gc:
        __gc_counter += 1
        gc.disable()
    return __gc_counter


def decrement_gc_counter():
    global __gc_counter
    if config.disable_gc:
        __gc_counter -= 1
        if __gc_counter == 0:
            gc.enable()
    return __gc_counter
