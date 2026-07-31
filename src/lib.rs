#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

use std::io::{self, Read, Write};

use lz4_flex::block::{compress_prepend_size_with_dict, decompress_size_prepended_with_dict};
use lz4_flex::frame::{FrameDecoder, FrameEncoder, FrameInfo};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use napi::bindgen_prelude::{spawn_blocking, Buffer, BufferSlice, ReadableStream, Uint8Array};
use napi::ScopedTask;
use napi::{
  bindgen_prelude::AsyncTask,
  tokio_stream::{wrappers::ReceiverStream, StreamExt},
  Either, Env, Error, Result, Status,
};

#[cfg(all(
  not(target_family = "wasm"),
  not(target_env = "ohos"),
  not(target_env = "musl")
))]
#[global_allocator]
static GLOBAL: mimalloc_safe::MiMalloc = mimalloc_safe::MiMalloc;

const FRAME_PREALLOC_THRESHOLD: usize = 4 * 1024 * 1024 + 64;
const DECODE_STREAM_CHUNK: usize = 64 * 1024;

// Minimum accumulated bytes in a streaming output buffer before a chunk is
// detached and pushed to JS. Matches the LZ4 default block size so a fully
// populated block crosses the threshold immediately while sub-block output
// stays Rust-side and is reused by the next chunk (zlib-style buffering).
const STREAM_FLUSH_THRESHOLD: usize = 64 * 1024;

/// Bounded capacity for the mpsc channels between the async forward loop
/// (Tokio async worker pool) and the long-lived `spawn_blocking` worker in
/// the Web ReadableStream compress / decompress path. Bounds queued
/// input/output for backpressure and memory growth while pipelining a few
/// chunks ahead of the JS consumer.
const STREAM_CHANNEL_CAPACITY: usize = 8;

fn frame_output_buffer(compressed_len: usize) -> Vec<u8> {
  if compressed_len > FRAME_PREALLOC_THRESHOLD {
    Vec::with_capacity(compressed_len)
  } else {
    Vec::new()
  }
}

struct Enc {
  data: Either<String, Uint8Array>,
}

#[napi]
impl<'a> ScopedTask<'a> for Enc {
  type Output = Vec<u8>;
  type JsValue = BufferSlice<'a>;

  fn compute(&mut self) -> Result<Self::Output> {
    let data: &[u8] = match self.data {
      Either::A(ref b) => b.as_bytes(),
      Either::B(ref s) => s,
    };
    Ok(compress_prepend_size(data))
  }

  fn resolve(&mut self, env: &'a Env, output: Self::Output) -> Result<Self::JsValue> {
    BufferSlice::from_data(env, output)
  }
}

struct Dec {
  data: Either<String, Uint8Array>,
}

#[napi]
impl<'a> ScopedTask<'a> for Dec {
  type Output = Vec<u8>;
  type JsValue = BufferSlice<'a>;

  fn compute(&mut self) -> Result<Self::Output> {
    let data: &[u8] = match self.data {
      Either::A(ref b) => b.as_bytes(),
      Either::B(ref s) => s,
    };
    decompress_size_prepended(data).map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  fn resolve(&mut self, env: &'a Env, output: Self::Output) -> Result<Self::JsValue> {
    BufferSlice::from_data(env, output)
  }
}

struct EncDict {
  data: Either<String, Uint8Array>,
  dict: Either<String, Uint8Array>,
}

#[napi]
impl<'a> ScopedTask<'a> for EncDict {
  type Output = Vec<u8>;
  type JsValue = BufferSlice<'a>;

  fn compute(&mut self) -> Result<Self::Output> {
    let data: &[u8] = match self.data {
      Either::A(ref b) => b.as_bytes(),
      Either::B(ref s) => s,
    };

    let dict: &[u8] = match self.dict {
      Either::A(ref b) => b.as_bytes(),
      Either::B(ref s) => s,
    };

    Ok(compress_prepend_size_with_dict(data, dict))
  }

  fn resolve(&mut self, env: &'a Env, output: Self::Output) -> Result<Self::JsValue> {
    BufferSlice::from_data(env, output)
  }
}

struct DecDict {
  data: Either<String, Uint8Array>,
  dict: Either<String, Uint8Array>,
}

#[napi]
impl<'a> ScopedTask<'a> for DecDict {
  type Output = Vec<u8>;
  type JsValue = BufferSlice<'a>;

  fn compute(&mut self) -> Result<Self::Output> {
    let data: &[u8] = match self.data {
      Either::A(ref b) => b.as_bytes(),
      Either::B(ref s) => s,
    };

    let dict: &[u8] = match self.dict {
      Either::A(ref b) => b.as_bytes(),
      Either::B(ref s) => s,
    };

    decompress_size_prepended_with_dict(data, dict)
      .map_err(|e| Error::new(Status::GenericFailure, format!("{e}")))
  }

  fn resolve(&mut self, env: &'a Env, output: Self::Output) -> Result<Self::JsValue> {
    BufferSlice::from_data(env, output)
  }
}

struct FrameDec {
  data: Either<String, Uint8Array>,
}

#[napi]
impl<'a> ScopedTask<'a> for FrameDec {
  type Output = Vec<u8>;
  type JsValue = BufferSlice<'a>;

  fn compute(&mut self) -> Result<Self::Output> {
    let data: &[u8] = match self.data {
      Either::A(ref b) => b.as_bytes(),
      Either::B(ref s) => s,
    };

    let mut buf = frame_output_buffer(data.len());

    let mut decoder = FrameDecoder::new(data);
    decoder.read_to_end(&mut buf)?;

    Ok(buf)
  }

  fn resolve(&mut self, env: &'a Env, output: Self::Output) -> Result<Self::JsValue> {
    BufferSlice::from_data(env, output)
  }
}

struct FrameEnc {
  data: Either<String, Uint8Array>,
  options: Option<FrameInfoOptions>,
}

#[napi]
impl<'a> ScopedTask<'a> for FrameEnc {
  type Output = Vec<u8>;
  type JsValue = BufferSlice<'a>;

  fn compute(&mut self) -> Result<Self::Output> {
    let data: &[u8] = match self.data {
      Either::A(ref b) => b.as_bytes(),
      Either::B(ref s) => s,
    };

    let mut buffer = vec![];

    let info = match self.options.take() {
      Some(opts) => frame_info_from_options(opts)?,
      None => FrameInfo::default(),
    };
    let mut encoder = FrameEncoder::with_frame_info(info, &mut buffer);

    encoder.write_all(data)?;

    encoder
      .finish()
      .map_err(|e| Error::new(napi::Status::Unknown, e.to_string()))?;

    Ok(buffer)
  }

  fn resolve(&mut self, env: &'a Env, output: Self::Output) -> Result<Self::JsValue> {
    BufferSlice::from_data(env, output)
  }
}

#[napi]
fn compress(
  data: Either<String, Uint8Array>,
  dict: Option<Either<String, Uint8Array>>,
) -> Result<Either<AsyncTask<Enc>, AsyncTask<EncDict>>> {
  if let Option::Some(v) = dict {
    let encoder = EncDict { data, dict: v };
    return Ok(Either::B(AsyncTask::new(encoder)));
  }
  let encoder = Enc { data };
  Ok(Either::A(AsyncTask::new(encoder)))
}

#[napi]
fn uncompress(
  data: Either<String, Uint8Array>,
  dict: Option<Either<String, Uint8Array>>,
) -> Result<Either<AsyncTask<Dec>, AsyncTask<DecDict>>> {
  if let Option::Some(v) = dict {
    let decoder = DecDict { data, dict: v };
    return Ok(Either::B(AsyncTask::new(decoder)));
  }
  let decoder = Dec { data };
  Ok(Either::A(AsyncTask::new(decoder)))
}

#[napi]
fn uncompress_sync<'a>(
  env: Env,
  data: Either<String, &'a [u8]>,
  dict: Option<Either<String, &'a [u8]>>,
) -> Result<BufferSlice<'a>> {
  if let Option::Some(v) = dict {
    return decompress_size_prepended_with_dict(
      match data {
        Either::A(ref s) => s.as_bytes(),
        Either::B(b) => b,
      },
      match v {
        Either::A(ref s) => s.as_bytes(),
        Either::B(b) => b,
      },
    )
    .map_err(|e| Error::new(napi::Status::GenericFailure, format!("{e}")))
    .and_then(|s| BufferSlice::copy_from(&env, s));
  }
  decompress_size_prepended(match data {
    Either::A(ref s) => s.as_bytes(),
    Either::B(b) => b,
  })
  .map_err(|e| Error::new(napi::Status::GenericFailure, format!("{e}")))
  .and_then(|d| BufferSlice::copy_from(&env, d))
}

#[napi]
fn compress_sync(
  data: Either<String, Buffer>,
  dict: Option<Either<String, Buffer>>,
) -> Result<Buffer> {
  if let Option::Some(v) = dict {
    return Ok(
      compress_prepend_size_with_dict(
        match data {
          Either::A(ref s) => s.as_bytes(),
          Either::B(ref b) => b,
        },
        match v {
          Either::A(ref s) => s.as_bytes(),
          Either::B(ref b) => b,
        },
      )
      .into(),
    );
  }
  Ok(
    compress_prepend_size(match data {
      Either::A(ref s) => s.as_bytes(),
      Either::B(ref b) => b,
    })
    .into(),
  )
}

#[napi]
fn compress_frame(
  data: Either<String, Uint8Array>,
  options: Option<FrameInfoOptions>,
) -> Result<AsyncTask<FrameEnc>> {
  let encoder = FrameEnc { data, options };
  Ok(AsyncTask::new(encoder))
}

#[napi]
fn decompress_frame(data: Either<String, Uint8Array>) -> Result<AsyncTask<FrameDec>> {
  let decoder = FrameDec { data };
  Ok(AsyncTask::new(decoder))
}

#[napi]
fn compress_frame_sync(
  data: Either<String, Buffer>,
  options: Option<FrameInfoOptions>,
) -> Result<Buffer> {
  let data_bytes: &[u8] = match data {
    Either::A(ref s) => s.as_bytes(),
    Either::B(ref b) => b,
  };

  let mut buffer = vec![];
  let info = match options {
    Some(opts) => frame_info_from_options(opts)?,
    None => FrameInfo::default(),
  };
  let mut encoder = FrameEncoder::with_frame_info(info, &mut buffer);
  encoder
    .write_all(data_bytes)
    .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))?;
  encoder
    .finish()
    .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))?;

  Ok(buffer.into())
}

#[napi]
fn decompress_frame_sync(data: Either<String, Buffer>) -> Result<Buffer> {
  let data_bytes: &[u8] = match data {
    Either::A(ref s) => s.as_bytes(),
    Either::B(ref b) => b,
  };

  let mut decoder = FrameDecoder::new(data_bytes);
  let mut buf = frame_output_buffer(data_bytes.len());
  decoder
    .read_to_end(&mut buf)
    .map_err(|e| Error::new(napi::Status::GenericFailure, e.to_string()))?;

  Ok(buf.into())
}

// ───────────────────────────────────────────────────────────────────
// Frame options plumbing
// ───────────────────────────────────────────────────────────────────

#[napi(object)]
pub struct FrameInfoOptions {
  pub block_size: Option<BlockSize>,
  pub block_mode: Option<BlockMode>,
  pub block_checksums: Option<bool>,
  pub content_checksum: Option<bool>,
  pub content_size: Option<f64>,
}

#[napi]
pub enum BlockSize {
  Auto,
  Max64KB,
  Max256KB,
  Max1MB,
  Max4MB,
  Max8MB,
}

#[napi]
pub enum BlockMode {
  Independent,
  Linked,
}

impl From<BlockSize> for lz4_flex::frame::BlockSize {
  fn from(value: BlockSize) -> Self {
    match value {
      BlockSize::Auto => lz4_flex::frame::BlockSize::Auto,
      BlockSize::Max64KB => lz4_flex::frame::BlockSize::Max64KB,
      BlockSize::Max256KB => lz4_flex::frame::BlockSize::Max256KB,
      BlockSize::Max1MB => lz4_flex::frame::BlockSize::Max1MB,
      BlockSize::Max4MB => lz4_flex::frame::BlockSize::Max4MB,
      BlockSize::Max8MB => lz4_flex::frame::BlockSize::Max8MB,
    }
  }
}

impl From<BlockMode> for lz4_flex::frame::BlockMode {
  fn from(value: BlockMode) -> Self {
    match value {
      BlockMode::Independent => lz4_flex::frame::BlockMode::Independent,
      BlockMode::Linked => lz4_flex::frame::BlockMode::Linked,
    }
  }
}

fn frame_err_to_napi(e: impl std::fmt::Display) -> Error {
  Error::new(Status::GenericFailure, format!("{e}"))
}

fn validate_content_size(value: f64) -> Result<u64> {
  if !value.is_finite() {
    return Err(Error::new(
      Status::InvalidArg,
      "contentSize must be a finite number",
    ));
  }
  if value < 0.0 {
    return Err(Error::new(
      Status::InvalidArg,
      "contentSize must be non-negative",
    ));
  }
  if value.fract() != 0.0 {
    return Err(Error::new(
      Status::InvalidArg,
      "contentSize must be an integer",
    ));
  }
  if value >= 2f64.powi(64) {
    return Err(Error::new(
      Status::InvalidArg,
      "contentSize exceeds the maximum supported size",
    ));
  }
  Ok(value as u64)
}

fn frame_info_from_options(options: FrameInfoOptions) -> Result<FrameInfo> {
  let mut info = FrameInfo::default();
  if let Some(block_size) = options.block_size {
    info.block_size = block_size.into();
  }
  if let Some(block_mode) = options.block_mode {
    info.block_mode = block_mode.into();
  }
  if let Some(block_checksums) = options.block_checksums {
    info.block_checksums = block_checksums;
  }
  if let Some(content_checksum) = options.content_checksum {
    info.content_checksum = content_checksum;
  }
  if let Some(content_size) = options.content_size {
    info.content_size = Some(validate_content_size(content_size)?);
  }
  Ok(info)
}

// ───────────────────────────────────────────────────────────────────
// Streaming worker adapters — io::Write sink (compress) and io::Read
// source (decompress) for the long-lived `spawn_blocking` workers that
// back the Web ReadableStream streaming path.
// ───────────────────────────────────────────────────────────────────

/// `io::Write` sink that accumulates compressed output into an owned
/// `Vec<u8>` that is **reused across chunks** until a flush threshold is
/// reached. Mirrors Node core zlib's buffering strategy: small outputs
/// accumulate in the persistent buffer (grows once to its steady capacity,
/// then no further allocation); once `written() >= STREAM_FLUSH_THRESHOLD`
/// the accumulated bytes are detached via `drain()` and handed to JS as an
/// owned `Buffer`, amortizing allocation across many chunks.
///
/// Soundness: the buffer is only ever returned to JS via `drain()` (which
/// moves the allocation out and replaces it with an empty `Vec`). Between
/// flushes JS holds no reference to the backing memory, so the worker
/// writing into it via `io::Write` cannot alias a live JS view.
struct OutputStreamAdapter {
  buf: Vec<u8>,
}

impl OutputStreamAdapter {
  fn new() -> Self {
    Self {
      buf: Vec::with_capacity(STREAM_FLUSH_THRESHOLD),
    }
  }

  /// Number of bytes accumulated and not yet drained.
  fn written(&self) -> usize {
    self.buf.len()
  }

  /// Move the accumulated output out, leaving an empty `Vec` so the next
  /// chunk grows a fresh allocation. The caller transfers ownership to JS.
  fn drain(&mut self) -> Vec<u8> {
    let mut new_buf = Vec::with_capacity(STREAM_FLUSH_THRESHOLD);
    std::mem::swap(&mut new_buf, &mut self.buf);
    new_buf
  }
}

impl Write for OutputStreamAdapter {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.buf.extend_from_slice(buf);
    Ok(buf.len())
  }

  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}

/// `io::Read` over a bounded `Receiver<napi::Result<Uint8Array>>` that
/// **blocks** until the next input chunk arrives, returning `Ok(0)` only
/// when the sender (the async forward loop) is dropped — i.e. the input
/// stream has ended. Reading from the channel freezes `FrameDecoder::read`
/// on the blocking worker until input is available, exactly the pull
/// semantics `FrameDecoder` expects.
///
/// A reader error (`Err`) is captured in `reader_err` and surfaced as an
/// `io::Error`; after `read` returns, the worker recovers the original
/// `napi::Error` from the field via `take_reader_error` (the codec owns the
/// reader, so `FrameDecoder::get_mut` exposes it) and forwards it verbatim
/// to the output stream instead of a lossy string.
struct BlockingRead {
  rx: napi::tokio::sync::mpsc::Receiver<napi::Result<Uint8Array>>,
  leftover: Option<Uint8Array>,
  pos: usize,
  reader_err: Option<napi::Error<Status>>,
}

impl BlockingRead {
  fn new(rx: napi::tokio::sync::mpsc::Receiver<napi::Result<Uint8Array>>) -> Self {
    Self {
      rx,
      leftover: None,
      pos: 0,
      reader_err: None,
    }
  }

  /// Take the last reader error captured by `read`, if any. Leaves `None`.
  fn take_reader_error(&mut self) -> Option<napi::Error<Status>> {
    self.reader_err.take()
  }
}

impl Read for BlockingRead {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    if buf.is_empty() {
      return Ok(0);
    }
    if let Some(chunk) = self.leftover.as_ref() {
      let avail = chunk.len() - self.pos;
      let n = std::cmp::min(buf.len(), avail);
      buf[..n].copy_from_slice(&chunk.as_ref()[self.pos..self.pos + n]);
      self.pos += n;
      if self.pos >= chunk.len() {
        self.leftover.take();
        self.pos = 0;
      }
      return Ok(n);
    }
    loop {
      match self.rx.blocking_recv() {
        Some(Ok(chunk)) => {
          // Skip zero-length chunks: returning `Ok(0)` would be interpreted
          // by `FrameDecoder` as end-of-stream, silently truncating the
          // decompressed output. Web ReadableStream producers may legally
          // enqueue empty Uint8Arrays; pass them straight through.
          if chunk.is_empty() {
            continue;
          }
          let n = std::cmp::min(buf.len(), chunk.len());
          buf[..n].copy_from_slice(&chunk.as_ref()[..n]);
          if n < chunk.len() {
            self.leftover = Some(chunk);
            self.pos = n;
          }
          return Ok(n);
        }
        Some(Err(e)) => {
          self.reader_err = Some(e);
          return Err(io::Error::other("lz4 stream reader error"));
        }
        None => return Ok(0),
      }
    }
  }
}

/// Convert an `io::Error` from the encoder `io::Write` sink into a napi `Error`.
fn frame_io_err(err: io::Error) -> Error {
  Error::new(Status::GenericFailure, format!("lz4 compress failed: {err}"))
}

/// Convert an `lz4_flex::frame::Error` from `try_finish` into a napi `Error`.
fn frame_lz4_err(err: lz4_flex::frame::Error) -> Error {
  Error::new(Status::GenericFailure, format!("lz4 compress failed: {err}"))
}

// ───────────────────────────────────────────────────────────────────
// Web ReadableStream streaming codec (napi built-in Tokio runtime).
//
// Single native call per stream: JS invokes `compressStream(input)` /
// `decompressStream(input)` once; the entire stream is consumed inside one
// Tokio task hosted on the napi built-in runtime via `Env::spawn_future`.
// Thread-pool split (tokio recommended pattern):
//   - `reader.next().await` (JS ReadableStream pull via tsfn) and
//     `tx.send().await` (output push) run on the Tokio async worker pool.
//   - CPU-bound `FrameEncoder` / `FrameDecoder` work runs on the separate
//     Tokio blocking pool via `spawn_blocking`, so it never starves the
//     async workers.
//
// The codec owns a long-lived `spawn_blocking` worker for the whole stream
// because `FrameEncoder::with_frame_info(info, &mut sink)` and
// `FrameDecoder::new(reader)` borrow their sink / reader, so the stateful
// codec cannot be moved per-chunk across a `spawn_blocking` boundary while
// persisting (self-referential borrow). A bounded `chunk` channel feeds
// input from the async forward loop into the blocking worker. When the JS
// consumer drops the output stream, `tx` becomes closed; the blocking
// worker's next `blocking_send` fails, it drops `chunk_rx`, and the forward
// loop's next `chunk_tx.send().await` fails, releasing the input reader.
// Cancellation latency is bounded by the next drain attempt — acceptable
// for this surface.
// ───────────────────────────────────────────────────────────────────

/// Compress a Web `ReadableStream<Uint8Array>` into a new byte
/// `ReadableStream` of LZ4-frame-compressed chunks.
#[napi]
pub fn compress_stream<'a>(
  env: &'a Env,
  input: ReadableStream<'a, Uint8Array>,
  options: Option<FrameInfoOptions>,
) -> Result<ReadableStream<'a, BufferSlice<'a>>> {
  let info = match options {
    Some(opts) => frame_info_from_options(opts)?,
    None => FrameInfo::default(),
  };
  let mut reader = input.read()?;
  let (tx, rx) =
    napi::tokio::sync::mpsc::channel::<Result<Vec<u8>>>(STREAM_CHANNEL_CAPACITY);

  env.spawn_future(async move {
    // Input channel feeding the blocking worker. Carries `Result` so reader
    // errors propagate into the worker and out through `tx`.
    let (chunk_tx, mut chunk_rx) =
      napi::tokio::sync::mpsc::channel::<napi::Result<Uint8Array>>(STREAM_CHANNEL_CAPACITY);

    // One long-lived blocking task owns the encoder + sink for the whole
    // stream. `tx` is moved in so drain outputs land directly on the output
    // stream.
    let blocking_handle = spawn_blocking(move || {
      let mut sink = OutputStreamAdapter::new();
      let mut encoder = FrameEncoder::with_frame_info(info, &mut sink);
      let mut status: Result<()> = Ok(());

      loop {
        match chunk_rx.blocking_recv() {
          Some(Ok(chunk)) => {
            if let Err(e) = encoder.write_all(chunk.as_ref()).map_err(frame_io_err) {
              status = Err(e);
              break;
            }
            if encoder.get_mut().written() >= STREAM_FLUSH_THRESHOLD {
              let drained = encoder.get_mut().drain();
              if !drained.is_empty()
                && tx.blocking_send(Ok(drained)).is_err()
              {
                break; // output stream dropped by JS consumer
              }
            }
          }
          Some(Err(e)) => {
            status = Err(e);
            break;
          }
          None => break,
        }
      }

      if status.is_ok() {
        status = encoder.try_finish().map_err(frame_lz4_err);
      }
      if let Err(error) = status {
        let _ = tx.blocking_send(Err(error));
      } else {
        let drained = encoder.get_mut().drain();
        if !drained.is_empty() {
          let _ = tx.blocking_send(Ok(drained));
        }
      }
      // `tx` drops here, signalling the output ReceiverStream to emit `None`.
    });

    // Async forward loop on the Tokio async worker pool: pull from the JS
    // reader, push into the blocking worker's input channel. Breaks on
    // reader end, reader error, or the blocking worker stopping the chunk
    // channel (output cancellation propagating back).
    loop {
      match reader.next().await {
        Some(Ok(chunk)) => {
          if chunk_tx.send(Ok(chunk)).await.is_err() {
            break; // blocking worker gone (output cancelled)
          }
        }
        Some(Err(e)) => {
          let _ = chunk_tx.send(Err(e)).await;
          break;
        }
        None => break,
      }
    }
    drop(chunk_tx);

    // Await the blocking worker so its final drain / error lands on `tx`
    // before the spawned task completes. `tx` is moved into the closure, so
    // a worker panic drops `tx` during unwind: the output stream ends
    // abruptly with a potentially truncated frame, indistinguishable from
    // normal EOF to JS, and the JoinError is swallowed. The worker body has
    // no panicking indexing or unwraps — only `Vec` growth in the sink can
    // OOM-panic, which the `STREAM_FLUSH_THRESHOLD` drain keeps bounded.
    let _ = blocking_handle.await;
    Ok(())
  })?;

  ReadableStream::create_with_stream_bytes(env, ReceiverStream::new(rx))
}

/// Decompress a Web `ReadableStream<Uint8Array>` of LZ4-frame-compressed
/// bytes into a new byte `ReadableStream` of decoded chunks. Mirrors
/// `compress_stream`: the blocking worker owns a `FrameDecoder` over a
/// `BlockingRead` channel adapter, and the async forward loop pulls from
/// the JS reader.
#[napi]
pub fn decompress_stream<'a>(
  env: &'a Env,
  input: ReadableStream<'a, Uint8Array>,
) -> Result<ReadableStream<'a, BufferSlice<'a>>> {
  let mut reader = input.read()?;
  let (tx, rx) =
    napi::tokio::sync::mpsc::channel::<Result<Vec<u8>>>(STREAM_CHANNEL_CAPACITY);

  env.spawn_future(async move {
    let (chunk_tx, chunk_rx) =
      napi::tokio::sync::mpsc::channel::<napi::Result<Uint8Array>>(STREAM_CHANNEL_CAPACITY);

    let blocking_handle = spawn_blocking(move || {
      // Long-lived decode worker. `FrameDecoder` owns the `BlockingRead`
      // reader; the worker reaches back into it via `decoder.get_mut()` to
      // recover the original `napi::Error` captured during `read` (since
      // `io::Read` can only carry an `io::Error`).
      let mut decoder = FrameDecoder::new(BlockingRead::new(chunk_rx));
      let mut buf = vec![0u8; DECODE_STREAM_CHUNK];
      let mut status: Result<()> = Ok(());

      loop {
        match decoder.read(&mut buf) {
          Ok(0) => break,
          Ok(n) => {
            if tx.blocking_send(Ok(buf[..n].to_vec())).is_err() {
              break; // output stream dropped by JS consumer
            }
          }
          Err(e) => {
            // Prefer a reader error captured by `BlockingRead`; otherwise map
            // the decoder's io::Error via `frame_err_to_napi`.
            let err = decoder
              .get_mut()
              .take_reader_error()
              .unwrap_or_else(|| frame_err_to_napi(e));
            status = Err(err);
            break;
          }
        }
      }

      if let Err(error) = status {
        let _ = tx.blocking_send(Err(error));
      }
      // `tx` drops here, signalling the output ReceiverStream to emit `None`.
    });

    loop {
      match reader.next().await {
        Some(Ok(chunk)) => {
          if chunk_tx.send(Ok(chunk)).await.is_err() {
            break; // blocking worker gone (output cancelled)
          }
        }
        Some(Err(e)) => {
          let _ = chunk_tx.send(Err(e)).await;
          break;
        }
        None => break,
      }
    }
    drop(chunk_tx);

    let _ = blocking_handle.await;
    Ok(())
  })?;

  ReadableStream::create_with_stream_bytes(env, ReceiverStream::new(rx))
}
