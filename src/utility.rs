//  Copyright (C) 2026 Theodore Chang
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use pyo3::{Py, PyAny, PyErr};

pub const HEADER_FIELD_LEN: usize = 10;
pub const HEADER_TOTAL_LEN: usize = 2 * HEADER_FIELD_LEN;

pub fn to_py(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

pub enum LazyTOC {
    Leaf {
        pos: [u64; 2],
    },
    Blocked {
        blocks: Vec<(u64, u64, u64)>,
    },
    Normal {
        pos: [u64; 2],
        container: LazyContainer,
    },
}

pub enum LazyContainer {
    Map(Vec<(Py<PyAny>, LazyTOC)>),
    Array(Vec<LazyTOC>),
}

impl LazyTOC {
    pub fn is_trivial(&self, threshold: u64) -> bool {
        matches!(self, LazyTOC::Leaf { pos } if (pos[1] - pos[0]) <= threshold)
    }
}

pub fn build_tree(
    start_pos: u64,
    end_pos: u64,
    all_trivial: bool,
    children: LazyContainer,
    small_obj_threshold: u64,
) -> LazyTOC {
    if end_pos <= small_obj_threshold + start_pos {
        return LazyTOC::Leaf {
            pos: [start_pos, end_pos],
        };
    }

    if !all_trivial {
        return LazyTOC::Normal {
            pos: [start_pos, end_pos],
            container: children,
        };
    }

    if let LazyContainer::Array(ref items) = children {
        let mut blocks = Vec::new();
        let mut count = 0u64;
        let mut size = 0u64;
        let mut block_start = 0u64;

        for (index, item) in items.iter().enumerate() {
            if let LazyTOC::Leaf { pos } = item {
                if count == 0 {
                    block_start = pos[0];
                }
                count += 1;
                size += pos[1] - pos[0];
                if size > small_obj_threshold || index + 1 == items.len() {
                    blocks.push((count, block_start, pos[1]));
                    count = 0;
                    size = 0;
                }
            }
        }

        if blocks.len() > 1 {
            return LazyTOC::Blocked { blocks };
        }
    }

    LazyTOC::Leaf {
        pos: [start_pos, end_pos],
    }
}
