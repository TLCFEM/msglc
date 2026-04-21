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

use crate::utility::{
    build_tree, to_py, LazyContainer, LazyTOC, HEADER_FIELD_LEN, HEADER_TOTAL_LEN,
};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PySet, PyTuple};
use std::fs::File;
use std::io::{BufWriter, Seek, Write};

// Compared to the standard `msgpack` specification, we do not support `ext` types and timestamps.
// They are not part of json specification anyway.
// Both can be converted to bytes in advance.
// One shall note that raw bytes are not supported by json specification as well.
fn write_primitive<W: Write>(obj: &Bound<'_, PyAny>, out: &mut W) -> PyResult<()> {
    let unsupported = Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Unsupported type.",
    ));

    if obj.is_none() {
        rmp::encode::write_nil(out).map_err(to_py)?;
    } else if let Ok(value) = obj.cast::<pyo3::types::PyBool>() {
        rmp::encode::write_bool(out, value.is_true()).map_err(to_py)?;
    } else if let Ok(value) = obj.cast::<pyo3::types::PyFloat>() {
        rmp::encode::write_f64(out, value.value()).map_err(to_py)?;
    } else if obj.is_instance_of::<pyo3::types::PyInt>() {
        if let Ok(value) = obj.extract::<i64>() {
            rmp::encode::write_sint(out, value).map_err(to_py)?;
        } else if let Ok(value) = obj.extract::<u64>() {
            rmp::encode::write_uint(out, value).map_err(to_py)?;
        } else {
            return unsupported;
        }
    } else if let Ok(value) = obj.cast::<PyBytes>() {
        rmp::encode::write_bin(out, value.as_bytes()).map_err(to_py)?;
    } else if let Ok(value) = obj.cast::<pyo3::types::PyByteArray>() {
        let bytes = value.to_vec();
        rmp::encode::write_bin(out, &bytes).map_err(to_py)?;
    } else if let Ok(_) = obj.cast::<pyo3::types::PyMemoryView>() {
        let value = obj.call_method0("tobytes")?.cast_into::<PyBytes>()?;
        rmp::encode::write_bin(out, value.as_bytes()).map_err(to_py)?;
    } else if let Ok(value) = obj.cast::<pyo3::types::PyString>() {
        rmp::encode::write_str(out, value.to_str()?).map_err(to_py)?;
    } else {
        return unsupported;
    }

    Ok(())
}

impl LazyTOC {
    fn encode_msgpack<W: Write>(&self, py: Python<'_>, out: &mut W) -> PyResult<()> {
        match self {
            LazyTOC::Leaf { pos } => {
                rmp::encode::write_map_len(out, 1).map_err(to_py)?;
                rmp::encode::write_str(out, "p").map_err(to_py)?;
                rmp::encode::write_array_len(out, 2).map_err(to_py)?;
                rmp::encode::write_uint(out, pos[0]).map_err(to_py)?;
                rmp::encode::write_uint(out, pos[1]).map_err(to_py)?;
            }
            LazyTOC::Blocked { blocks } => {
                rmp::encode::write_map_len(out, 1).map_err(to_py)?;
                rmp::encode::write_str(out, "p").map_err(to_py)?;
                rmp::encode::write_array_len(out, u32::try_from(blocks.len())?).map_err(to_py)?;
                for &(count, start, end) in blocks {
                    rmp::encode::write_array_len(out, 3).map_err(to_py)?;
                    rmp::encode::write_uint(out, count).map_err(to_py)?;
                    rmp::encode::write_uint(out, start).map_err(to_py)?;
                    rmp::encode::write_uint(out, end).map_err(to_py)?;
                }
            }
            LazyTOC::Normal { pos, container } => {
                rmp::encode::write_map_len(out, 2).map_err(to_py)?;
                rmp::encode::write_str(out, "p").map_err(to_py)?;
                rmp::encode::write_array_len(out, 2).map_err(to_py)?;
                rmp::encode::write_uint(out, pos[0]).map_err(to_py)?;
                rmp::encode::write_uint(out, pos[1]).map_err(to_py)?;
                rmp::encode::write_str(out, "t").map_err(to_py)?;
                match container {
                    LazyContainer::Map(items) => {
                        rmp::encode::write_map_len(out, u32::try_from(items.len())?)
                            .map_err(to_py)?;
                        for (key, child) in items {
                            write_primitive(key.bind(py), out)?;
                            child.encode_msgpack(py, out)?;
                        }
                    }
                    LazyContainer::Array(items) => {
                        rmp::encode::write_array_len(out, u32::try_from(items.len())?)
                            .map_err(to_py)?;
                        for child in items {
                            child.encode_msgpack(py, out)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

struct LazyBuffer<W: Write + Seek> {
    buffer: W,
    current_pos: u64, // do not use `stream_position` to reduce syscalls
    initial_pos: u64, // always len(magic) + 20
}

impl<W: Write + Seek> LazyBuffer<W> {
    fn new(buffer: W, initial_pos: u64) -> std::io::Result<Self> {
        Ok(Self {
            buffer,
            current_pos: 0,
            initial_pos,
        })
    }
}

impl<W: Write + Seek> Write for LazyBuffer<W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let size = self.buffer.write(data)?;
        self.current_pos += size as u64;
        Ok(size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.flush()
    }
}

impl<W: Write + Seek> Seek for LazyBuffer<W> {
    fn seek(&mut self, in_pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let pos = self.buffer.seek(in_pos)?;
        self.current_pos = pos;
        Ok(pos)
    }
}

struct LazyWriter<'py> {
    py: Python<'py>,
    buffer: LazyBuffer<BufWriter<File>>,
    ndarray_type: Option<Py<PyAny>>,
    sorted_fn: Py<PyAny>,
    trivial_size: u64,
    small_obj_threshold: u64,
    numpy_encoder: bool,
}

impl<'py> LazyWriter<'py> {
    fn new(py: Python<'py>, path: &str, magic_len: usize) -> PyResult<Self> {
        let config = py.import("msglc.config")?.getattr("config")?;

        Ok(Self {
            py,
            buffer: LazyBuffer::new(
                BufWriter::with_capacity(
                    config.getattr("write_buffer_size")?.extract()?,
                    File::create(path).map_err(to_py)?,
                ),
                (magic_len + HEADER_TOTAL_LEN) as u64,
            )
            .map_err(to_py)?,
            ndarray_type: py
                .import("numpy")
                .ok()
                .and_then(|m| m.getattr("ndarray").ok())
                .map(Bound::unbind),
            sorted_fn: py.import("builtins")?.getattr("sorted")?.unbind(),
            trivial_size: config.getattr("trivial_size")?.extract()?,
            small_obj_threshold: config
                .getattr("small_obj_optimization_threshold")?
                .extract()?,
            numpy_encoder: config.getattr("numpy_encoder")?.extract()?,
        })
    }

    // this method shall only be called when packing the data
    fn offset(&self) -> PyResult<u64> {
        if self.buffer.current_pos < self.buffer.initial_pos {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Buffer position is before the initial position.",
            ));
        }

        Ok(self.buffer.current_pos - self.buffer.initial_pos)
    }

    fn try_pack_numpy(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Option<LazyTOC>> {
        let Some(ndarray_type) = &self.ndarray_type else {
            return Ok(None);
        };

        if !obj.is_instance(ndarray_type.bind(self.py))? {
            return Ok(None);
        }

        if !self.numpy_encoder {
            let value = obj.call_method0("tolist")?;
            return self.pack(value.as_any()).map(Some);
        }

        let start_pos = self.offset()?;
        let bytes = obj.call_method0("dumps")?.cast_into::<PyBytes>()?;
        let value = bytes.as_bytes();
        rmp::encode::write_bin_len(&mut self.buffer, value.len() as u32).map_err(to_py)?;
        self.buffer.write_all(value).map_err(to_py)?;

        Ok(Some(LazyTOC::Leaf {
            pos: [start_pos, self.offset()?],
        }))
    }

    fn pack_map(&mut self, value: &Bound<'py, PyDict>) -> PyResult<LazyTOC> {
        let start_pos = self.offset()?;
        let mut all_trivial = true;
        let mut items = Vec::with_capacity(value.len());

        rmp::encode::write_map_len(&mut self.buffer, u32::try_from(value.len())?).map_err(to_py)?;

        for (k, v) in value.iter() {
            write_primitive(&k, &mut self.buffer)?;
            let node = self.pack(&v)?;
            if all_trivial && !node.is_trivial(self.trivial_size) {
                all_trivial = false;
            }
            items.push((k.unbind(), node));
        }

        Ok(build_tree(
            start_pos,
            self.offset()?,
            all_trivial,
            LazyContainer::Map(items),
            self.small_obj_threshold,
        ))
    }

    fn pack_array(
        &mut self,
        iter: impl Iterator<Item = Bound<'py, PyAny>>,
        len: usize,
    ) -> PyResult<LazyTOC> {
        let start_pos = self.offset()?;
        let mut all_trivial = true;
        let mut items = Vec::with_capacity(len);

        rmp::encode::write_array_len(&mut self.buffer, u32::try_from(len)?).map_err(to_py)?;

        for item in iter {
            let node = self.pack(&item)?;
            if all_trivial && !node.is_trivial(self.trivial_size) {
                all_trivial = false;
            }
            items.push(node);
        }

        Ok(build_tree(
            start_pos,
            self.offset()?,
            all_trivial,
            LazyContainer::Array(items),
            self.small_obj_threshold,
        ))
    }

    fn pack(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<LazyTOC> {
        if let Ok(value) = obj.cast::<PyDict>() {
            return self.pack_map(value);
        }
        if let Ok(value) = obj.cast::<PyList>() {
            return self.pack_array(value.iter(), value.len());
        }
        if let Ok(value) = obj.cast::<PyTuple>() {
            return self.pack_array(value.iter(), value.len());
        }
        if obj.cast::<PySet>().is_ok() {
            let value = self
                .sorted_fn
                .bind(self.py)
                .call1((obj,))?
                .cast_into::<PyList>()?;
            return self.pack_array(value.iter(), value.len());
        }
        if let Some(node) = self.try_pack_numpy(obj)? {
            return Ok(node);
        }

        let start_pos = self.offset()?;

        write_primitive(obj, &mut self.buffer)?;

        Ok(LazyTOC::Leaf {
            pos: [start_pos, self.offset()?],
        })
    }
}

fn encode_header(value: u64) -> PyResult<[u8; HEADER_FIELD_LEN]> {
    let mut encoded = Vec::new();
    rmp::encode::write_uint(&mut encoded, value).map_err(to_py)?;
    let mut out = [0u8; HEADER_FIELD_LEN];
    let start = HEADER_FIELD_LEN - encoded.len();
    out[start..].copy_from_slice(&encoded);
    Ok(out)
}

#[pyfunction]
pub fn dump_rust_impl_msgpack(py: Python<'_>, path: String, obj: Bound<'_, PyAny>) -> PyResult<()> {
    let magic: Vec<u8> = py
        .import("msglc.writer")?
        .getattr("LazyWriter")?
        .getattr("magic")?
        .extract()?;
    let header_pos = std::io::SeekFrom::Start(magic.len() as u64);

    let mut writer = LazyWriter::new(py, &path, magic.len())?;
    writer.buffer.write_all(&magic).map_err(to_py)?;
    writer
        .buffer
        .write_all(&[0u8; HEADER_TOTAL_LEN])
        .map_err(to_py)?;

    let toc = writer.pack(&obj)?;
    let toc_start_pos = writer.offset()?;
    toc.encode_msgpack(py, &mut writer.buffer)?;
    let toc_end_pos = writer.offset()?;

    writer.buffer.seek(header_pos).map_err(to_py)?;
    let toc_start = encode_header(toc_start_pos)?;
    let toc_size = encode_header(toc_end_pos - toc_start_pos)?;
    writer.buffer.write_all(&toc_start).map_err(to_py)?;
    writer.buffer.write_all(&toc_size).map_err(to_py)?;
    writer.buffer.flush().map_err(to_py)?;

    Ok(())
}
