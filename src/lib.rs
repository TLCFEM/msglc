//! Python extension module for high-performance msgpack serialization with
//! lazy Table of Contents (TOC) generation.
//!
//! Provides [`NativeWriter`], exposed to Python as `_msglc.NativeWriter`,
//! which streams packed msgpack data directly to a file.

mod core;

use crate::core::{build_container_node, encode_header_value, to_py_err, TocChildren, TocNode};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PySet, PyTuple};
use pyo3::{exceptions::PyOverflowError, ffi};
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::os::raw::c_int;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Total byte length of the file header (two fixed-width fields).
const HEADER_TOTAL_LEN: usize = 20;

/// Buffer size for the [`BufWriter`] wrapping the output file.
const STREAM_WRITE_BUFFER_BYTES: usize = 8 * 1024 * 1024;

/// Threshold at which the scratch buffer is flushed to the underlying writer.
const STREAM_SCRATCH_FLUSH_BYTES: usize = 256 * 1024;

// ---------------------------------------------------------------------------
// Encoding configuration (loaded from Python at runtime)
// ---------------------------------------------------------------------------

/// Runtime configuration sourced from `msglc.config.config` and
/// `msglc.writer.LazyWriter.magic`.
struct EncodingConfig {
    trivial_size: usize,
    small_obj_threshold: usize,
    numpy_encoder: bool,
    magic: Vec<u8>,
}

fn load_encoding_config(py: Python<'_>) -> PyResult<EncodingConfig> {
    let config = py.import("msglc.config")?.getattr("config")?;
    let magic: Vec<u8> = py
        .import("msglc.writer")?
        .getattr("LazyWriter")?
        .getattr("magic")?
        .extract()?;

    Ok(EncodingConfig {
        trivial_size: config.getattr("trivial_size")?.extract()?,
        small_obj_threshold: config
            .getattr("small_obj_optimization_threshold")?
            .extract()?,
        numpy_encoder: config.getattr("numpy_encoder")?.extract()?,
        magic,
    })
}

// ---------------------------------------------------------------------------
// Counting writer (tracks stream position without extra syscalls)
// ---------------------------------------------------------------------------

/// A [`Write`] + [`Seek`] wrapper that tracks the current stream position
/// in-process, avoiding repeated `stream_position()` calls.
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

// ---------------------------------------------------------------------------
// Streaming TOC builder
// ---------------------------------------------------------------------------

/// Packs a Python object graph directly to a seekable writer (file) while
/// building the TOC.  Uses a small scratch buffer to batch tiny writes.
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

    // -- Position / flush --------------------------------------------------

    /// Returns the current write position relative to `data_start`.
    fn rel_pos(&self) -> usize {
        (self.writer.pos + self.scratch.len() as u64 - self.data_start) as usize
    }

    /// Drains the scratch buffer into the underlying writer.
    fn flush_scratch(&mut self) -> PyResult<()> {
        if !self.scratch.is_empty() {
            self.writer.write_all(&self.scratch).map_err(to_py_err)?;
            self.scratch.clear();
        }
        Ok(())
    }

    /// Appends data to the scratch buffer via `encode`, flushing if the
    /// buffer exceeds [`STREAM_SCRATCH_FLUSH_BYTES`].
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

    /// Appends raw bytes to the scratch buffer, flushing if needed.
    fn write_bytes_to_scratch(&mut self, bytes: &[u8]) -> PyResult<()> {
        self.scratch.extend_from_slice(bytes);
        if self.scratch.len() >= STREAM_SCRATCH_FLUSH_BYTES {
            self.flush_scratch()?;
        }
        Ok(())
    }

    /// Provides mutable access to the underlying writer (for header fixups).
    fn writer_mut(&mut self) -> &mut CountingWriter<W> {
        &mut self.writer
    }

    // -- Primitive encoding ------------------------------------------------

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

    // -- NumPy handling ----------------------------------------------------

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

    // -- Recursive packing -------------------------------------------------

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

// ---------------------------------------------------------------------------
// Python-exposed writer
// ---------------------------------------------------------------------------

/// Native (Rust) writer exposed to Python for high-performance msgpack
/// serialization with TOC generation.
#[pyclass]
struct NativeWriter;

fn dump_to_file_streaming_impl(py: Python<'_>, obj: Bound<'_, PyAny>, path: String) -> PyResult<()> {
    let cfg = load_encoding_config(py)?;
    let file = File::create(&path).map_err(to_py_err)?;
    let mut file = BufWriter::with_capacity(STREAM_WRITE_BUFFER_BYTES, file);

    // Write magic bytes and reserve space for the header.
    file.write_all(&cfg.magic).map_err(to_py_err)?;
    let header_start = file.stream_position().map_err(to_py_err)?;
    file.write_all(&[0u8; HEADER_TOTAL_LEN]).map_err(to_py_err)?;
    let data_start = file.stream_position().map_err(to_py_err)?;

    let mut builder = StreamTocBuilder::new(
        py,
        file,
        data_start,
        cfg.trivial_size,
        cfg.small_obj_threshold,
        cfg.numpy_encoder,
    )?;

    let toc = builder.pack(&obj)?;
    builder.flush_scratch()?;

    // Serialize the TOC and append it after the data.
    let mut toc_bytes = Vec::with_capacity(1024 * 1024);
    toc.encode_msgpack(py, &builder.python_packer, &mut toc_bytes)?;

    let toc_start = builder.rel_pos();
    builder
        .writer_mut()
        .write_all(&toc_bytes)
        .map_err(to_py_err)?;

    // Seek back and fill in the header with TOC position and length.
    let toc_start_header = encode_header_value(toc_start)?;
    let toc_len_header = encode_header_value(toc_bytes.len())?;

    builder
        .writer_mut()
        .seek(SeekFrom::Start(header_start))
        .map_err(to_py_err)?;
    builder
        .writer_mut()
        .write_all(&toc_start_header)
        .map_err(to_py_err)?;
    builder
        .writer_mut()
        .write_all(&toc_len_header)
        .map_err(to_py_err)?;
    builder.writer_mut().flush().map_err(to_py_err)?;
    Ok(())
}

#[pymethods]
impl NativeWriter {
    #[new]
    fn new() -> Self {
        Self
    }

    /// Streams `obj` directly to `path` as a complete `.msglc` file, including
    /// magic header, TOC, and data payload.
    fn dump_to_file_streaming(
        &self,
        py: Python<'_>,
        obj: Bound<'_, PyAny>,
        path: String,
    ) -> PyResult<()> {
        dump_to_file_streaming_impl(py, obj, path)
    }
}

#[pyfunction]
fn dump_native_impl(py: Python<'_>, path: String, obj: Bound<'_, PyAny>) -> PyResult<()> {
    dump_to_file_streaming_impl(py, obj, path)
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

#[pymodule]
fn _msglc(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeWriter>()?;
    m.add_function(wrap_pyfunction!(dump_native_impl, m)?)?;
    Ok(())
}
