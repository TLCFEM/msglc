use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PySet, PyTuple};
use pyo3::{exceptions::PyOverflowError, ffi};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::os::raw::c_int;

const HEADER_FIELD_LEN: usize = 10;
const HEADER_TOTAL_LEN: usize = 20;
const STREAM_WRITE_BUFFER_BYTES: usize = 8 * 1024 * 1024;
const STREAM_SCRATCH_FLUSH_BYTES: usize = 256 * 1024;

pub enum TocNode {
    Leaf {
        pos: [usize; 2],
    },
    Blocked {
        blocks: Vec<(usize, usize, usize)>,
    },
    Branch {
        pos: [usize; 2],
        children: TocChildren,
    },
}

pub enum TocChildren {
    Map(Vec<(Py<PyAny>, TocNode)>),
    Array(Vec<TocNode>),
}

impl TocNode {
    pub fn is_trivial(&self, threshold: usize) -> bool {
        matches!(self, TocNode::Leaf { pos } if (pos[1] - pos[0]) <= threshold)
    }

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

pub fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
}

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

fn write_position<W: Write>(out: &mut W, pos: &[usize; 2]) -> PyResult<()> {
    rmp::encode::write_array_len(out, 2).map_err(to_py_err)?;
    rmp::encode::write_uint(out, pos[0] as u64).map_err(to_py_err)?;
    rmp::encode::write_uint(out, pos[1] as u64).map_err(to_py_err)?;
    Ok(())
}

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

struct CountingWriter<W: Write + Seek> {
    inner: W,
    pos: u64,
}

impl<W: Write + Seek> CountingWriter<W> {
    fn new(mut inner: W) -> std::io::Result<Self> {
        let pos = inner.stream_position()?;
        Ok(Self { inner, pos })
    }
}

impl<W: Write + Seek> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.pos += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write + Seek> Seek for CountingWriter<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = self.inner.seek(pos)?;
        self.pos = new_pos;
        Ok(new_pos)
    }
}

struct StreamTocBuilder<'py, W: Write + Seek> {
    py: Python<'py>,
    python_packer: Py<PyAny>,
    numpy_ndarray_type: Option<Py<PyAny>>,
    writer: CountingWriter<W>,
    data_start: u64,
    scratch: Vec<u8>,
    trivial_size: usize,
    small_obj_threshold: usize,
    numpy_encoder: bool,
}

impl<'py, W: Write + Seek> StreamTocBuilder<'py, W> {
    fn new(
        py: Python<'py>,
        writer: W,
        data_start: u64,
        trivial_size: usize,
        small_obj_threshold: usize,
        numpy_encoder: bool,
    ) -> PyResult<Self> {
        let python_packer = py.import("msgpack")?.getattr("Packer")?.call0()?.unbind();
        let numpy_ndarray_type = py
            .import("numpy")
            .ok()
            .and_then(|m| m.getattr("ndarray").ok())
            .map(Bound::unbind);

        Ok(Self {
            py,
            python_packer,
            numpy_ndarray_type,
            writer: CountingWriter::new(writer).map_err(to_py_err)?,
            data_start,
            scratch: Vec::with_capacity(STREAM_SCRATCH_FLUSH_BYTES),
            trivial_size,
            small_obj_threshold,
            numpy_encoder,
        })
    }

    fn rel_pos(&self) -> usize {
        (self.writer.pos + self.scratch.len() as u64 - self.data_start) as usize
    }

    fn flush_scratch(&mut self) -> PyResult<()> {
        if !self.scratch.is_empty() {
            self.writer.write_all(&self.scratch).map_err(to_py_err)?;
            self.scratch.clear();
        }
        Ok(())
    }

    fn write_to_scratch<F, E>(&mut self, encode: F) -> PyResult<()>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), E>,
        E: std::fmt::Display,
    {
        encode(&mut self.scratch).map_err(to_py_err)?;
        if self.scratch.len() >= STREAM_SCRATCH_FLUSH_BYTES {
            self.flush_scratch()?;
        }
        Ok(())
    }

    fn write_bytes_to_scratch(&mut self, bytes: &[u8]) -> PyResult<()> {
        self.scratch.extend_from_slice(bytes);
        if self.scratch.len() >= STREAM_SCRATCH_FLUSH_BYTES {
            self.flush_scratch()?;
        }
        Ok(())
    }

    fn append_with_python_packer(&mut self, obj: &Bound<'_, PyAny>) -> PyResult<()> {
        let packed = self
            .python_packer
            .bind(self.py)
            .call_method1("pack", (obj,))?
            .cast_into::<PyBytes>()?;
        self.write_bytes_to_scratch(packed.as_bytes())
    }

    fn try_append_fast_int(&mut self, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
        let mut overflow: c_int = 0;
        let signed = unsafe { ffi::PyLong_AsLongLongAndOverflow(obj.as_ptr(), &mut overflow) };

        if overflow == 0 {
            if unsafe { !ffi::PyErr_Occurred().is_null() } {
                return Err(PyErr::fetch(self.py));
            }
            self.write_to_scratch(|b| rmp::encode::write_sint(b, signed).map(|_| ()))?;
            return Ok(true);
        }

        if overflow > 0 {
            let unsigned = unsafe { ffi::PyLong_AsUnsignedLongLong(obj.as_ptr()) };
            if unsafe { !ffi::PyErr_Occurred().is_null() } {
                let err = PyErr::fetch(self.py);
                if err.is_instance_of::<PyOverflowError>(self.py) {
                    return Ok(false);
                }
                return Err(err);
            }
            self.write_to_scratch(|b| rmp::encode::write_uint(b, unsigned).map(|_| ()))?;
            return Ok(true);
        }

        Ok(false)
    }

    fn try_append_native(&mut self, obj: &Bound<'_, PyAny>) -> PyResult<bool> {
        if obj.is_none() {
            self.write_to_scratch(rmp::encode::write_nil)?;
            return Ok(true);
        }
        if obj.is_instance_of::<pyo3::types::PyBool>() {
            let value: bool = obj.extract()?;
            self.write_to_scratch(|b| rmp::encode::write_bool(b, value))?;
            return Ok(true);
        }
        if obj.is_instance_of::<pyo3::types::PyInt>() {
            return self.try_append_fast_int(obj);
        }
        if let Ok(s) = obj.cast::<pyo3::types::PyString>() {
            let value = s.to_str()?;
            self.write_to_scratch(|b| rmp::encode::write_str(b, value))?;
            return Ok(true);
        }
        if let Ok(b) = obj.cast::<PyBytes>() {
            self.flush_scratch()?;
            rmp::encode::write_bin(&mut self.writer, b.as_bytes()).map_err(to_py_err)?;
            return Ok(true);
        }
        if obj.is_instance_of::<pyo3::types::PyByteArray>()
            || obj.is_instance_of::<pyo3::types::PyMemoryView>()
        {
            let bytes = obj.call_method0("tobytes")?.cast_into::<PyBytes>()?;
            self.flush_scratch()?;
            rmp::encode::write_bin(&mut self.writer, bytes.as_bytes()).map_err(to_py_err)?;
            return Ok(true);
        }
        if obj.is_instance_of::<pyo3::types::PyFloat>() {
            let value: f64 = obj.extract()?;
            self.write_to_scratch(|b| rmp::encode::write_f64(b, value))?;
            return Ok(true);
        }

        Ok(false)
    }

    fn append_packed(&mut self, obj: &Bound<'_, PyAny>) -> PyResult<()> {
        if self.try_append_native(obj)? {
            return Ok(());
        }
        self.append_with_python_packer(obj)
    }

    fn try_pack_numpy(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<Option<TocNode>> {
        let Some(ndarray_type) = &self.numpy_ndarray_type else {
            return Ok(None);
        };
        if !obj.is_instance(ndarray_type.bind(self.py))? {
            return Ok(None);
        }

        if self.numpy_encoder {
            let start = self.rel_pos();
            let dumped = obj.call_method0("dumps")?.cast_into::<PyBytes>()?;
            self.flush_scratch()?;
            rmp::encode::write_bin_len(&mut self.writer, dumped.as_bytes().len() as u32)
                .map_err(to_py_err)?;
            self.writer
                .write_all(dumped.as_bytes())
                .map_err(to_py_err)?;
            return Ok(Some(TocNode::Leaf {
                pos: [start, self.rel_pos()],
            }));
        }

        let as_list = obj.call_method0("tolist")?;
        self.pack(as_list.as_any()).map(Some)
    }

    fn pack_dict(&mut self, start_pos: usize, dict: &Bound<'py, PyDict>) -> PyResult<TocNode> {
        self.write_to_scratch(|b| rmp::encode::write_map_len(b, dict.len() as u32).map(|_| ()))?;
        let mut all_trivial = true;
        let mut entries = Vec::with_capacity(dict.len());

        for (k, v) in dict.iter() {
            self.append_packed(&k)?;
            let node = self.pack(&v)?;
            if !node.is_trivial(self.trivial_size) {
                all_trivial = false;
            }
            entries.push((k.unbind(), node));
        }

        let end_pos = self.rel_pos();
        Ok(build_container_node(
            start_pos,
            end_pos,
            all_trivial,
            TocChildren::Map(entries),
            self.small_obj_threshold,
        ))
    }

    fn pack_sequence(
        &mut self,
        start_pos: usize,
        iter: impl Iterator<Item = Bound<'py, PyAny>>,
        len: usize,
    ) -> PyResult<TocNode> {
        self.write_to_scratch(|b| rmp::encode::write_array_len(b, len as u32).map(|_| ()))?;
        let mut all_trivial = true;
        let mut items = Vec::with_capacity(len);

        for item in iter {
            let node = self.pack(&item)?;
            if !node.is_trivial(self.trivial_size) {
                all_trivial = false;
            }
            items.push(node);
        }

        let end_pos = self.rel_pos();
        Ok(build_container_node(
            start_pos,
            end_pos,
            all_trivial,
            TocChildren::Array(items),
            self.small_obj_threshold,
        ))
    }

    fn pack(&mut self, obj: &Bound<'py, PyAny>) -> PyResult<TocNode> {
        let start_pos = self.rel_pos();

        if let Ok(dict) = obj.cast::<PyDict>() {
            return self.pack_dict(start_pos, dict);
        }
        if let Ok(list) = obj.cast::<PyList>() {
            return self.pack_sequence(start_pos, list.iter(), list.len());
        }
        if let Ok(tuple) = obj.cast::<PyTuple>() {
            return self.pack_sequence(start_pos, tuple.iter(), tuple.len());
        }
        if obj.cast::<PySet>().is_ok() {
            let sorted = self
                .py
                .import("builtins")?
                .getattr("sorted")?
                .call1((obj,))?
                .cast_into::<PyList>()?;
            return self.pack_sequence(start_pos, sorted.iter(), sorted.len());
        }
        if let Some(node) = self.try_pack_numpy(obj)? {
            return Ok(node);
        }

        self.append_packed(obj)?;
        Ok(TocNode::Leaf {
            pos: [start_pos, self.rel_pos()],
        })
    }
}

#[pyfunction]
fn dump_rust_impl(py: Python<'_>, path: String, obj: Bound<'_, PyAny>) -> PyResult<()> {
    let magic: Vec<u8> = py
        .import("msglc.writer")?
        .getattr("LazyWriter")?
        .getattr("magic")?
        .extract()?;
    let config = py.import("msglc.config")?.getattr("config")?;
    let trivial_size = config.getattr("trivial_size")?.extract()?;
    let small_obj_threshold = config
        .getattr("small_obj_optimization_threshold")?
        .extract()?;
    let numpy_encoder = config.getattr("numpy_encoder")?.extract()?;

    let file = File::create(&path).map_err(to_py_err)?;
    let mut buffer = BufWriter::with_capacity(STREAM_WRITE_BUFFER_BYTES, file);

    buffer.write_all(&magic).map_err(to_py_err)?;
    let header_start = buffer.stream_position().map_err(to_py_err)?;
    buffer
        .write_all(&[0u8; HEADER_TOTAL_LEN])
        .map_err(to_py_err)?;
    let data_start = buffer.stream_position().map_err(to_py_err)?;

    let mut builder = StreamTocBuilder::new(
        py,
        buffer,
        data_start,
        trivial_size,
        small_obj_threshold,
        numpy_encoder,
    )?;

    let toc = builder.pack(&obj)?;
    builder.flush_scratch()?;

    let mut toc_bytes = Vec::with_capacity(1024 * 1024);
    toc.encode_msgpack(py, &builder.python_packer, &mut toc_bytes)?;

    let toc_start = builder.rel_pos();
    builder.writer.write_all(&toc_bytes).map_err(to_py_err)?;

    let toc_start_header = encode_header_value(toc_start)?;
    let toc_len_header = encode_header_value(toc_bytes.len())?;

    builder
        .writer
        .seek(SeekFrom::Start(header_start))
        .map_err(to_py_err)?;
    builder
        .writer
        .write_all(&toc_start_header)
        .map_err(to_py_err)?;
    builder
        .writer
        .write_all(&toc_len_header)
        .map_err(to_py_err)?;
    builder.writer.flush().map_err(to_py_err)?;
    Ok(())
}

#[pymodule]
fn msglc_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(dump_rust_impl, m)?)?;
    Ok(())
}
