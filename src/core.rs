//! Core types and encoding utilities for the msglc native extension.
//!
//! This module defines the Table of Contents (TOC) tree structure and provides
//! msgpack serialization helpers used by both the in-memory and streaming writers.

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};
use std::io::Write;

/// Length in bytes of each fixed-width header field.
const HEADER_FIELD_LEN: usize = 10;

// ---------------------------------------------------------------------------
// TOC tree
// ---------------------------------------------------------------------------

/// A node in the Table of Contents tree.
///
/// The TOC mirrors the structure of the serialized msgpack data and allows
/// lazy, random-access reads without deserializing the entire payload.
pub enum TocNode {
    /// A terminal value whose serialized bytes span `pos[0]..pos[1]`.
    Leaf { pos: [usize; 2] },

    /// A flat array whose leaf elements have been grouped into contiguous
    /// blocks. Each entry is `(element_count, start_offset, end_offset)`.
    Blocked { blocks: Vec<(usize, usize, usize)> },

    /// A container (map or array) with its own byte range and child entries.
    Branch {
        pos: [usize; 2],
        children: TocChildren,
    },
}

/// The children of a [`TocNode::Branch`].
pub enum TocChildren {
    Map(Vec<(Py<PyAny>, TocNode)>),
    Array(Vec<TocNode>),
}

impl TocNode {
    /// Returns `true` if this node is a [`Leaf`](TocNode::Leaf) whose byte
    /// span is at most `threshold` bytes.
    pub fn is_trivial(&self, threshold: usize) -> bool {
        matches!(self, TocNode::Leaf { pos } if (pos[1] - pos[0]) <= threshold)
    }

    /// Serializes this TOC tree into msgpack bytes appended to `out`.
    pub fn encode_msgpack(
        &self,
        py: Python<'_>,
        packer: &Py<PyAny>,
        out: &mut Vec<u8>,
    ) -> PyResult<()> {
        match self {
            TocNode::Leaf { pos } => {
                rmp::encode::write_map_len(out, 1).map_err(to_py_err)?;
                rmp::encode::write_str(out, "p").map_err(to_py_err)?;
                write_position(out, pos)?;
            }
            TocNode::Blocked { blocks } => {
                rmp::encode::write_map_len(out, 1).map_err(to_py_err)?;
                rmp::encode::write_str(out, "p").map_err(to_py_err)?;
                rmp::encode::write_array_len(out, blocks.len() as u32).map_err(to_py_err)?;
                for &(count, start, end) in blocks {
                    rmp::encode::write_array_len(out, 3).map_err(to_py_err)?;
                    rmp::encode::write_uint(out, count as u64).map_err(to_py_err)?;
                    rmp::encode::write_uint(out, start as u64).map_err(to_py_err)?;
                    rmp::encode::write_uint(out, end as u64).map_err(to_py_err)?;
                }
            }
            TocNode::Branch { pos, children } => {
                rmp::encode::write_map_len(out, 2).map_err(to_py_err)?;
                rmp::encode::write_str(out, "p").map_err(to_py_err)?;
                write_position(out, pos)?;
                rmp::encode::write_str(out, "t").map_err(to_py_err)?;
                match children {
                    TocChildren::Map(entries) => {
                        rmp::encode::write_map_len(out, entries.len() as u32).map_err(to_py_err)?;
                        for (key, child) in entries {
                            write_native_or_python_packed(py, packer, key.bind(py), out)?;
                            child.encode_msgpack(py, packer, out)?;
                        }
                    }
                    TocChildren::Array(items) => {
                        rmp::encode::write_array_len(out, items.len() as u32).map_err(to_py_err)?;
                        for child in items {
                            child.encode_msgpack(py, packer, out)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Converts any [`Display`](std::fmt::Display) error into a Python `RuntimeError`.
pub fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

/// Writes packed msgpack bytes via Python-level `Packer` directly into `out`.
fn write_with_python_packer<W: Write>(
    py: Python<'_>,
    packer: &Py<PyAny>,
    obj: &Bound<'_, PyAny>,
    out: &mut W,
) -> PyResult<()> {
    let packed = packer
        .bind(py)
        .call_method1("pack", (obj,))?
        .cast_into::<PyBytes>()?;
    out.write_all(packed.as_bytes()).map_err(to_py_err)?;
    Ok(())
}

/// Writes msgpack bytes for common scalar types directly, with Python packer fallback.
fn write_native_or_python_packed<W: Write>(
    py: Python<'_>,
    packer: &Py<PyAny>,
    obj: &Bound<'_, PyAny>,
    out: &mut W,
) -> PyResult<()> {
    if obj.is_none() {
        rmp::encode::write_nil(out).map_err(to_py_err)?;
        return Ok(());
    }

    if obj.is_instance_of::<pyo3::types::PyBool>() {
        let value: bool = obj.extract()?;
        rmp::encode::write_bool(out, value).map_err(to_py_err)?;
        return Ok(());
    }

    if obj.is_instance_of::<pyo3::types::PyInt>() {
        if let Ok(value) = obj.extract::<i64>() {
            rmp::encode::write_sint(out, value).map_err(to_py_err)?;
            return Ok(());
        }
        if let Ok(value) = obj.extract::<u64>() {
            rmp::encode::write_uint(out, value).map_err(to_py_err)?;
            return Ok(());
        }
    }

    if let Ok(s) = obj.cast::<pyo3::types::PyString>() {
        rmp::encode::write_str(out, s.to_str()?).map_err(to_py_err)?;
        return Ok(());
    }

    write_with_python_packer(py, packer, obj, out)
}

/// Writes a `[start, end]` position pair as a 2-element msgpack array.
fn write_position<W: Write>(out: &mut W, pos: &[usize; 2]) -> PyResult<()> {
    rmp::encode::write_array_len(out, 2).map_err(to_py_err)?;
    rmp::encode::write_uint(out, pos[0] as u64).map_err(to_py_err)?;
    rmp::encode::write_uint(out, pos[1] as u64).map_err(to_py_err)?;
    Ok(())
}

/// Encodes a `usize` into a fixed-width header field, zero-padded on the left.
pub fn encode_header_value(value: usize) -> PyResult<[u8; HEADER_FIELD_LEN]> {
    let mut encoded = Vec::new();
    rmp::encode::write_uint(&mut encoded, value as u64).map_err(to_py_err)?;

    if encoded.len() > HEADER_FIELD_LEN {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Header value overflow.",
        ));
    }

    let mut out = [0u8; HEADER_FIELD_LEN];
    let start = HEADER_FIELD_LEN - encoded.len();
    out[start..].copy_from_slice(&encoded);
    Ok(out)
}

// ---------------------------------------------------------------------------
// Container node construction
// ---------------------------------------------------------------------------

/// Decides whether a packed container should be stored as a [`Leaf`](TocNode::Leaf),
/// [`Blocked`](TocNode::Blocked), or [`Branch`](TocNode::Branch) node based on its
/// size and whether all children are trivial.
pub fn build_container_node(
    start_pos: usize,
    end_pos: usize,
    all_trivial: bool,
    children: TocChildren,
    small_obj_threshold: usize,
) -> TocNode {
    let size = end_pos - start_pos;
    if size <= small_obj_threshold {
        return TocNode::Leaf {
            pos: [start_pos, end_pos],
        };
    }

    if all_trivial {
        if let TocChildren::Array(ref kids) = children {
            if let Some(blocked) = try_build_blocked_node(kids, small_obj_threshold) {
                return blocked;
            }
        }
        return TocNode::Leaf {
            pos: [start_pos, end_pos],
        };
    }

    TocNode::Branch {
        pos: [start_pos, end_pos],
        children,
    }
}

/// Attempts to partition trivial leaf children into contiguous blocks.
///
/// Returns `Some(TocNode::Blocked { .. })` when two or more blocks are formed,
/// allowing the reader to skip over groups of small elements efficiently.
fn try_build_blocked_node(kids: &[TocNode], threshold: usize) -> Option<TocNode> {
    if kids.is_empty() {
        return None;
    }

    let mut blocks = Vec::new();
    let mut count = 0usize;
    let mut size = 0usize;
    let mut block_start = 0usize;

    for (i, kid) in kids.iter().enumerate() {
        if let TocNode::Leaf { pos } = kid {
            if count == 0 {
                block_start = pos[0];
            }
            count += 1;
            size += pos[1] - pos[0];
            if size > threshold || i == kids.len() - 1 {
                blocks.push((count, block_start, pos[1]));
                count = 0;
                size = 0;
            }
        }
    }

    if blocks.len() > 1 {
        Some(TocNode::Blocked { blocks })
    } else {
        None
    }
}
