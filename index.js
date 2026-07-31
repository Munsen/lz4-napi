'use strict'

const { Transform } = require('node:stream')

const native = require('./native.js')

const compress = native.compress
const compressSync = native.compressSync
const uncompress = native.uncompress
const uncompressSync = native.uncompressSync

function pickFrameOptions(options) {
  if (!options) return null
  const has =
    options.blockSize != null ||
    options.blockMode != null ||
    options.blockChecksums != null ||
    options.contentChecksum != null ||
    options.contentSize != null
  if (!has) return null
  return {
    blockSize: options.blockSize,
    blockMode: options.blockMode,
    blockChecksums: options.blockChecksums,
    contentChecksum: options.contentChecksum,
    contentSize: options.contentSize,
  }
}

function compressFrame(data, options, callback) {
  if (typeof options === 'function') {
    callback = options
    options = undefined
  }
  const p = native.compressFrame(data, pickFrameOptions(options))
  if (typeof callback !== 'function') return p
  p.then(
    (out) => callback(null, out),
    (err) => callback(err),
  )
  return undefined
}

function decompressFrame(data, callback) {
  if (typeof data === 'function') {
    callback = data
    data = undefined
  }
  const p = native.decompressFrame(data)
  if (typeof callback !== 'function') return p
  p.then(
    (out) => callback(null, out),
    (err) => callback(err),
  )
  return undefined
}

const compressFrameSync = native.compressFrameSync
const decompressFrameSync = native.decompressFrameSync

const compressStream = native.compressStream
const decompressStream = native.decompressStream

class Lz4TransformBridge extends Transform {
  #pendingTransformCb
  #webInput
  #inputController
  #reader
  #pumping = false
  #wantPump = false
  #egressPaused = false
  #ended = false
  #destroyed = false
  #flushCallback

  constructor(transformOptions, makeStream, frameOptions) {
    rejectUnsupportedTransformOptions(transformOptions)
    super({ ...transformOptions, decodeStrings: true })
    this.#pendingTransformCb = null
    this.#flushCallback = null
    const self = this
    this.#webInput = new ReadableStream(
      {
        start(c) {
          self.#inputController = c
        },
        pull() {
          self.#releasePendingTransformCb()
        },
      },

      new CountQueuingStrategy({ highWaterMark: 1 }),
    )
    this.#reader = makeStream(this.#webInput, frameOptions).getReader()
  }

  #releasePendingTransformCb(err) {
    if (this.#pendingTransformCb) {
      const cb = this.#pendingTransformCb
      this.#pendingTransformCb = null
      cb(err)
    }
  }

  _transform(chunk, _encoding, callback) {
    if (this.#destroyed) return callback()
    if (this.#ended) {
      return callback()
    }
    try {
      const view = new Uint8Array(chunk.length)
      view.set(chunk)
      this.#inputController.enqueue(view)
    } catch (err) {
      return callback(err)
    }

    const desired = this.#inputController.desiredSize
    if (desired != null && desired < 0) {
      this.#pendingTransformCb = callback
    } else {
      callback()
    }
    this.#start()
  }

  _flush(callback) {
    if (this.#destroyed) return callback()
    this.#releasePendingTransformCb()
    if (this.#ended) return callback()
    try {
      this.#inputController.close()
    } catch (err) {
      return callback(err)
    }
    this.#flushCallback = callback
    this.#demand()
  }

  _read() {
    this.#demand()
  }

  #start() {
    if (this.#pumping || this.#ended || this.#destroyed || this.#egressPaused) return
    this.#pumping = true
    this.#runPump()
  }

  #demand() {
    if (this.#ended || this.#destroyed) return
    this.#egressPaused = false
    if (this.#pumping) {
      this.#wantPump = true
      return
    }
    this.#pumping = true
    this.#runPump()
  }

  async #runPump() {
    try {
      while (!this.#ended && !this.#destroyed) {
        const { done, value } = await this.#reader.read()
        if (this.#destroyed) break
        if (done) {
          this.#ended = true
          this.push(null)
          this.#releasePendingTransformCb()
          if (this.#flushCallback) {
            const cb = this.#flushCallback
            this.#flushCallback = null
            cb()
          }
          break
        }
        const buf = Buffer.isBuffer(value) ? value : Buffer.from(value)
        if (!this.push(buf)) {
          this.#egressPaused = true
          break
        }
      }
    } catch (err) {
      if (!this.#destroyed) {
        if (this.#flushCallback) {
          const cb = this.#flushCallback
          this.#flushCallback = null
          cb(err)
        } else {
          this.destroy(err)
        }
      }
    } finally {
      this.#pumping = false
      if (this.#wantPump && !this.#ended && !this.#destroyed) {
        this.#wantPump = false
        this.#demand()
      } else {
        this.#wantPump = false
      }
    }
  }

  _destroy(err, callback) {
    this.#destroyed = true
    this.#ended = true
    this.#flushCallback = null
    this.#releasePendingTransformCb(err)
    try {
      this.#inputController.error(err || new Error('destroyed'))
    } catch {}
    if (this.#reader) {
      try {
        this.#reader.cancel('destroyed').catch(() => {})
      } catch {}
    }
    callback(err)
  }
}

function rejectUnsupportedTransformOptions(options) {
  if (!options) return
  if (
    options.objectMode ||
    options.writableObjectMode ||
    options.readableObjectMode ||
    options.decodeStrings === false
  ) {
    throw new TypeError(
      'Lz4 transforms are byte-only: objectMode, writableObjectMode, readableObjectMode, and decodeStrings:false are not supported',
    )
  }
  for (const option of ['construct', 'read', 'write', 'writev', 'final', 'destroy', 'transform', 'flush']) {
    if (options[option] != null) {
      throw new TypeError(`Lz4 transforms manage the ${option} lifecycle hook internally`)
    }
  }
}

class Lz4Compress extends Lz4TransformBridge {
  constructor(options) {
    const { blockSize, blockMode, blockChecksums, contentChecksum, contentSize, ...transformOpts } = options || {}
    super(
      transformOpts,
      (webInput, frameOptions) => compressStream(webInput, frameOptions),
      pickFrameOptions({ blockSize, blockMode, blockChecksums, contentChecksum, contentSize }),
    )
  }
}

class Lz4Decompress extends Lz4TransformBridge {
  constructor(options) {
    const { blockSize, blockMode, blockChecksums, contentChecksum, contentSize, ...transformOpts } = options || {}
    super(transformOpts, (webInput) => decompressStream(webInput), null)
  }
}

function createLz4Compress(options) {
  return new Lz4Compress(options)
}

function createLz4Decompress(options) {
  return new Lz4Decompress(options)
}

// ───────────────────────────────────────────────────────────────────
module.exports = {
  // block one-shot
  compress,
  compressSync,
  uncompress,
  uncompressSync,
  // frame one-shot (gzip-style dual mode)
  compressFrame,
  compressFrameSync,
  decompressFrame,
  decompressFrameSync,
  // Node Transform (zlib-style)
  createLz4Compress,
  createLz4Decompress,
  Lz4Compress,
  Lz4Decompress,
  // Web ReadableStream (zero-copy native)
  compressStream,
  decompressStream,
  // enums
  BlockSize: native.BlockSize,
  BlockMode: native.BlockMode,
}
