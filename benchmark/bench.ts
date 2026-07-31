import { readFileSync } from 'fs'
import { Transform, Readable, Writable, pipeline } from 'node:stream'
import { fileURLToPath } from 'node:url'
import { join } from 'path'
import { promisify } from 'util'
import {
  gzip,
  deflate,
  brotliCompress,
  inflate,
  brotliDecompress,
  gzipSync,
  deflateSync,
  brotliCompressSync,
  gunzip,
  createGzip,
  createGunzip,
  createDeflate,
  createInflate,
  createBrotliCompress,
  createBrotliDecompress,
} from 'zlib'

import b from 'benny'
import snappy from 'snappy'

import {
  compress,
  compressFrame,
  decompressFrame,
  uncompress,
  compressSync,
  compressFrameSync,
  createLz4Compress,
  createLz4Decompress,
} from '../index'

const gzipAsync = promisify(gzip)
const brotliCompressAsync = promisify(brotliCompress)
const deflateAsync = promisify(deflate)
const gunzipAsync = promisify(gunzip)
const inflateAsync = promisify(inflate)
const brotliDecompressAsync = promisify(brotliDecompress)

const FIXTURE = readFileSync(join(fileURLToPath(import.meta.url), '..', '..', 'yarn.lock'))
const FIXTURE_DICT = readFileSync(join(fileURLToPath(import.meta.url), '..', '..', '__test__/dict.bin'))
const LZ4_COMPRESSED_FIXTURE = Buffer.from(compressSync(FIXTURE))
const LZ4_FRAME_COMPRESSED_FIXTURE = Buffer.from(compressFrameSync(FIXTURE))
const SNAPPY_COMPRESSED_FIXTURE = Buffer.from(snappy.compressSync(FIXTURE))
const GZIP_FIXTURE = gzipSync(FIXTURE)
const DEFLATED_FIXTURE = deflateSync(FIXTURE)
const BROTLI_COMPRESSED_FIXTURE = brotliCompressSync(FIXTURE)

// Chunk size aligned with the LZ4 default block size (64 KiB).
const STREAM_CHUNK_SIZE = 64 * 1024

function chunkedInput(input: Buffer, chunkSize: number): Buffer[] {
  if (input.length === 0) return [Buffer.alloc(0)]
  const chunks: Buffer[] = []
  for (let i = 0; i < input.length; i += chunkSize) {
    chunks.push(input.subarray(i, i + chunkSize))
  }
  return chunks
}

// Node-stream pipeline helper: feed `input` chunked through the given
// Transform, collecting the output into a single Buffer.
function runStream(input: Buffer, transform: Transform, chunkSize: number = STREAM_CHUNK_SIZE): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    pipeline(
      // Binary-mode source so Buffers flow as byte chunks (not objects).
      Readable.from(chunkedInput(input, chunkSize), { objectMode: false }),
      transform,
      new Writable({
        write(c, _encoding, cb) {
          chunks.push(c)
          cb()
        },
      }),
      (err) => (err ? reject(err) : resolve(Buffer.concat(chunks))),
    )
  })
}

async function run() {
  await b.suite(
    'Compress',

    b.add('lz4', () => {
      return compress(FIXTURE)
    }),

    b.add('lz4 dict', () => {
      return compress(FIXTURE, FIXTURE_DICT)
    }),

    b.add('lz4 frame', () => {
      return compressFrame(FIXTURE)
    }),

    b.add('snappy', () => {
      return snappy.compress(FIXTURE)
    }),

    b.add('gzip', () => {
      return gzipAsync(FIXTURE)
    }),

    b.add('deflate', () => {
      return deflateAsync(FIXTURE)
    }),

    b.add('brotli', () => {
      return brotliCompressAsync(FIXTURE)
    }),

    b.cycle(),
    b.complete(),
  )

  await b.suite(
    'Decompress',

    b.add('lz4', () => {
      return uncompress(LZ4_COMPRESSED_FIXTURE)
    }),

    b.add('lz4 dict', () => {
      return uncompress(LZ4_COMPRESSED_FIXTURE, FIXTURE_DICT)
    }),

    b.add('lz4 frame', () => {
      return decompressFrame(LZ4_FRAME_COMPRESSED_FIXTURE)
    }),

    b.add('snappy', () => {
      return snappy.uncompress(SNAPPY_COMPRESSED_FIXTURE)
    }),

    b.add('gzip', () => {
      return gunzipAsync(GZIP_FIXTURE)
    }),

    b.add('deflate', () => {
      return inflateAsync(DEFLATED_FIXTURE)
    }),

    b.add('brotli', () => {
      return brotliDecompressAsync(BROTLI_COMPRESSED_FIXTURE)
    }),

    b.cycle(),
    b.complete(),
  )

  await b.suite(
    'Stream Compress',

    b.add('lz4 frame', () => {
      return runStream(FIXTURE, createLz4Compress())
    }),

    b.add('gzip', () => {
      return runStream(FIXTURE, createGzip())
    }),

    b.add('deflate', () => {
      return runStream(FIXTURE, createDeflate())
    }),

    b.add('brotli', () => {
      return runStream(FIXTURE, createBrotliCompress())
    }),

    b.cycle(),
    b.complete(),
  )

  await b.suite(
    'Stream Decompress',

    b.add('lz4 frame', () => {
      return runStream(LZ4_FRAME_COMPRESSED_FIXTURE, createLz4Decompress())
    }),

    b.add('gzip', () => {
      return runStream(GZIP_FIXTURE, createGunzip())
    }),

    b.add('deflate', () => {
      return runStream(DEFLATED_FIXTURE, createInflate())
    }),

    b.add('brotli', () => {
      return runStream(BROTLI_COMPRESSED_FIXTURE, createBrotliDecompress())
    }),

    b.cycle(),
    b.complete(),
  )
}

run().catch((e) => {
  console.error(e)
})
