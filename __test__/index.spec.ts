import { readFileSync } from 'fs'
import { Readable, Writable, pipeline } from 'node:stream'
import { promisify } from 'util'

import test from 'ava'

import {
  compress,
  compressSync,
  uncompress,
  uncompressSync,
  compressFrame,
  compressFrameSync,
  decompressFrame,
  decompressFrameSync,
  compressStream,
  decompressStream,
  createLz4Compress,
  createLz4Decompress,
  BlockSize,
} from '../index.js'

const pipelineAsync = promisify(pipeline)

const stringToCompress = 'adewqeqweqwewleekqwoekqwoekqwpoekqwpoekqwpoekqwpoekqwpoekqwpokeeqw'
const dict = readFileSync('__test__/dict.bin')

const sampleLarge = Buffer.concat([
  Buffer.from(stringToCompress),
  Buffer.from(stringToCompress),
  Buffer.from(stringToCompress),
  Buffer.from(stringToCompress),
])

// ───────────────────────────────────────────────────────────────────
// Web ReadableStream helpers
// ───────────────────────────────────────────────────────────────────

function toWebChunks(buf: Buffer | Uint8Array, size = 64 * 1024): ReadableStream<Uint8Array> {
  const chunks: Uint8Array[] = []
  for (let i = 0; i < buf.length; i += size) {
    chunks.push(buf.subarray(i, i + size) as Uint8Array)
  }
  return new ReadableStream({
    start(c) {
      for (const ch of chunks) c.enqueue(ch)
      c.close()
    },
  })
}

async function collectStream(stream: ReadableStream<Uint8Array>): Promise<Buffer> {
  const reader = stream.getReader()
  const parts: Buffer[] = []
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    parts.push(Buffer.from(value))
  }
  return Buffer.concat(parts)
}

// ───────────────────────────────────────────────────────────────────
// Block format (size-prepended)
// ───────────────────────────────────────────────────────────────────

test('compress should return smaller value', async (t) => {
  const before = Buffer.from(stringToCompress)
  const compressed = await compress(before)
  t.true(before.length > compressed.length)
  t.true(compressed.length !== 0)
})

test('compress decompress should work', async (t) => {
  const before = Buffer.from(stringToCompress)
  const compressed = await compress(before)
  t.true(before.length > compressed.length)
  const decompressed = await uncompress(compressed)
  t.is(before.toString('utf8'), decompressed.toString('utf8'))
})

test('compress decompress should work with dict', async (t) => {
  const before = Buffer.from(stringToCompress)
  const compressed = await compress(before, dict)
  t.true(before.length > compressed.length)
  const decompressed = await uncompress(compressed, dict)
  t.is(before.toString('utf8'), decompressed.toString('utf8'))
})

test('compress decompress sync should work', (t) => {
  const before = Buffer.from(stringToCompress)
  const compressed = compressSync(before)
  t.true(before.length > compressed.length)
  const decompressed = uncompressSync(compressed)
  t.is(before.toString('utf8'), decompressed.toString('utf8'))
})

test('compress should take all input types', async (t) => {
  const stringBuffer = Buffer.from(stringToCompress)
  await t.notThrowsAsync(compress(stringToCompress))
  await t.notThrowsAsync(compress(stringBuffer))
  await t.notThrowsAsync(compress(new Uint8Array(stringBuffer)))
})

test('uncompress should take all input types', async (t) => {
  const compressedValue = compressSync(stringToCompress)
  await t.notThrowsAsync(uncompress(compressedValue))
  await t.notThrowsAsync(uncompress(new Uint8Array(compressedValue)))
})

// ───────────────────────────────────────────────────────────────────
// Frame format (one-shot)
// ───────────────────────────────────────────────────────────────────

test('compressFrame should return smaller value', async (t) => {
  const before = Buffer.from(stringToCompress)
  const compressed = await compressFrame(before)
  t.true(before.length > compressed.length)
  t.true(compressed.length !== 0)
})

test('compress and decompress frame should work', async (t) => {
  const before = Buffer.from(stringToCompress)
  const compressed = await compressFrame(before)
  t.true(before.length > compressed.length)
  const decompressed = await decompressFrame(compressed)
  t.is(before.toString('utf8'), decompressed.toString('utf8'))
})

test('compressFrameSync should return smaller value', (t) => {
  const before = Buffer.from(stringToCompress)
  const compressed = compressFrameSync(before)
  t.true(before.length > compressed.length)
  t.true(compressed.length !== 0)
})

test('compress and decompress frame sync should work', (t) => {
  const before = Buffer.from(stringToCompress)
  const compressed = compressFrameSync(before)
  t.true(before.length > compressed.length)
  const decompressed = decompressFrameSync(compressed)
  t.is(before.toString('utf8'), decompressed.toString('utf8'))
})

test('compressFrameSync should take all input types', (t) => {
  const stringBuffer = Buffer.from(stringToCompress)
  t.notThrows(() => compressFrameSync(stringToCompress))
  t.notThrows(() => compressFrameSync(stringBuffer))
  t.notThrows(() => compressFrameSync(new Uint8Array(stringBuffer)))
})

test('decompressFrameSync should take all input types', (t) => {
  const compressedValue = compressFrameSync(stringToCompress)
  t.notThrows(() => decompressFrameSync(compressedValue))
  t.notThrows(() => decompressFrameSync(new Uint8Array(compressedValue)))
})

test('frame sync and async should produce compatible output', async (t) => {
  const before = Buffer.from(stringToCompress)
  const compressedSync = compressFrameSync(before)
  const compressedAsync = await compressFrame(before)

  const decompressedSync = decompressFrameSync(compressedSync)
  const decompressedAsync = await decompressFrame(compressedSync)
  t.is(before.toString('utf8'), decompressedSync.toString('utf8'))
  t.is(before.toString('utf8'), decompressedAsync.toString('utf8'))

  const decompressedSync2 = decompressFrameSync(compressedAsync)
  const decompressedAsync2 = await decompressFrame(compressedAsync)
  t.is(before.toString('utf8'), decompressedSync2.toString('utf8'))
  t.is(before.toString('utf8'), decompressedAsync2.toString('utf8'))
})

// ───────────────────────────────────────────────────────────────────
// Frame streaming via Web ReadableStream
// ───────────────────────────────────────────────────────────────────

test('compressStream returns a Web ReadableStream', (t) => {
  const out = compressStream(toWebChunks(sampleLarge))
  t.true(out instanceof ReadableStream)
  t.true(decompressStream(toWebChunks(sampleLarge)) instanceof ReadableStream)
})

test('compressStream then decompressStream roundtrips', async (t) => {
  const compressed = await collectStream(compressStream(toWebChunks(sampleLarge)))
  t.true(compressed.length > 0)
  t.true(compressed.length < sampleLarge.length)
  const decompressed = await collectStream(decompressStream(toWebChunks(compressed)))
  t.is(decompressed.toString('utf8'), sampleLarge.toString('utf8'))
  t.is(decompressed.length, sampleLarge.length)
})

test('compressStream output is decodable by decompressFrameSync', async (t) => {
  const compressed = await collectStream(compressStream(toWebChunks(sampleLarge)))
  const decompressed = decompressFrameSync(compressed)
  t.is(decompressed.toString('utf8'), sampleLarge.toString('utf8'))
})

test('compressStream accepts fragmented input', async (t) => {
  const fragments = []
  for (let i = 0; i < sampleLarge.length; i += 13) {
    fragments.push(sampleLarge.subarray(i, i + 13))
  }
  const compressed = await collectStream(compressStream(toWebChunks(Buffer.concat(fragments))))
  const decompressed = await collectStream(decompressStream(toWebChunks(compressed)))
  t.is(decompressed.toString('utf8'), sampleLarge.toString('utf8'))
})

test('decompressStream handles fragmented compressed input', async (t) => {
  const compressed = await collectStream(compressStream(toWebChunks(sampleLarge)))
  const fragments = []
  for (let i = 0; i < compressed.length; i += 7) {
    fragments.push(compressed.subarray(i, i + 7))
  }
  const stream = decompressStream(
    new ReadableStream({
      start(c) {
        for (const ch of fragments) c.enqueue(ch as Uint8Array)
        c.close()
      },
    }),
  )
  const decompressed = await collectStream(stream)
  t.is(decompressed.toString('utf8'), sampleLarge.toString('utf8'))
})

test('compressStream passes options to the encoder', async (t) => {
  const compressed = await collectStream(
    compressStream(toWebChunks(sampleLarge), {
      blockSize: BlockSize.Max64KB,
      contentChecksum: true,
      contentSize: sampleLarge.length,
    }),
  )
  t.true(compressed.length > 0)
  const decompressed = await collectStream(decompressStream(toWebChunks(compressed)))
  t.is(decompressed.toString('utf8'), sampleLarge.toString('utf8'))
})

test('decompressStream propagates decompression errors via stream', async (t) => {
  const bad = new ReadableStream({
    start(c) {
      c.enqueue(Buffer.from('definitely not an lz4 frame') as Uint8Array)
      c.close()
    },
  })
  await t.throwsAsync(() => collectStream(decompressStream(bad)), {
    message: /Magic|Frame|Decompres|fail/i,
  })
})

test('compressStream on empty input produces a valid frame', async (t) => {
  const compressed = await collectStream(compressStream(toWebChunks(Buffer.alloc(0))))
  t.true(compressed.length > 0)
  const decompressed = await collectStream(decompressStream(toWebChunks(compressed)))
  t.is(decompressed.length, 0)
})

test('compressStream output matches compressFrameSync for same frame options', async (t) => {
  const streamOut = await collectStream(
    compressStream(toWebChunks(sampleLarge), {
      blockSize: BlockSize.Max64KB,
      contentChecksum: true,
    }),
  )
  const syncOut = compressFrameSync(sampleLarge, {
    blockSize: BlockSize.Max64KB,
    contentChecksum: true,
  })
  t.deepEqual(streamOut, syncOut)
})

test('compressStream cancellation releases the input reader', async (t) => {
  const stream = compressStream(
    new ReadableStream({
      start(c) {
        for (let i = 0; i < 1024; i++) c.enqueue(Buffer.alloc(64 * 1024) as Uint8Array)
      },
    }),
  )
  const reader = stream.getReader()
  const first = await reader.read()
  t.false(first.done)
  await reader.cancel('done')
  t.pass()
})

test('decompressStream tolerates interleaved empty chunks', async (t) => {
  const compressed = await collectStream(compressStream(toWebChunks(sampleLarge)))
  const fragments: Uint8Array[] = []
  for (let i = 0; i < compressed.length; i += 32) {
    fragments.push(compressed.subarray(i, i + 32) as Uint8Array)
    fragments.push(new Uint8Array(0))
  }
  const stream = decompressStream(
    new ReadableStream({
      start(c) {
        for (const ch of fragments) c.enqueue(ch)
        c.close()
      },
    }),
  )
  const decompressed = await collectStream(stream)
  t.is(decompressed.toString('utf8'), sampleLarge.toString('utf8'))
  t.is(decompressed.length, sampleLarge.length)
})

test('decompressStream: source rejection surfaces its error', async (t) => {
  const stream = decompressStream(
    new ReadableStream({
      start(c) {
        c.enqueue(compressFrameSync(sampleLarge).subarray(0, 32) as Uint8Array)
        c.error(new Error('source boom'))
      },
    }),
  )
  await t.throwsAsync(() => collectStream(stream), { message: /source boom/i })
})

// ───────────────────────────────────────────────────────────────────
// Node Transform bridge (zlib-style createLz4Compress / createLz4Decompress)
// ───────────────────────────────────────────────────────────────────

const bridgeFixture = Buffer.alloc(8 * 1024 * 1024)
for (let i = 0; i < bridgeFixture.length; i++) bridgeFixture[i] = i & 0xff

function bridgeCollect(highWater = 1024) {
  const chunks: Buffer[] = []
  const sink = new Writable({
    highWaterMark: highWater,
    write(c, _e, cb) {
      chunks.push(Buffer.from(c))
      cb()
    },
  })
  return [sink, () => Buffer.concat(chunks)] as const
}

test('createLz4Compress / Decompress roundtrip under backpressure', async (t) => {
  const [s1, get1] = bridgeCollect()
  await pipelineAsync(Readable.from([bridgeFixture], { objectMode: false }), createLz4Compress(), s1)
  const compressed = get1()
  t.true(compressed.length > 0)
  const [s2, get2] = bridgeCollect()
  await pipelineAsync(Readable.from([compressed], { objectMode: false }), createLz4Decompress(), s2)
  const back = get2()
  t.is(back.length, bridgeFixture.length)
  t.deepEqual(back, bridgeFixture)
})

test('createLz4Decompress rejects invalid frame via stream error', async (t) => {
  const [sink] = bridgeCollect()
  await t.throwsAsync(
    () =>
      pipelineAsync(
        Readable.from([Buffer.from('definitely not an lz4 frame')], { objectMode: false }),
        createLz4Decompress(),
        sink,
      ),
    { message: /Magic|Frame|Decompres|fail/i },
  )
})

test('createLz4Decompress drops trailing bytes after frame EOF (matches gunzip)', async (t) => {
  const small = bridgeFixture.subarray(0, 1024)
  const frame = compressFrameSync(small)
  const extra = Buffer.concat([frame, Buffer.from('trailing bytes after frame end')])
  const [sink, get] = bridgeCollect()
  await pipelineAsync(Readable.from([extra], { objectMode: false }), createLz4Decompress(), sink)
  t.deepEqual(get(), small)
})

test('createLz4Decompress handles frame split across many small chunks', async (t) => {
  const small = bridgeFixture.subarray(0, 4096)
  const frame = compressFrameSync(small)
  const frags: Buffer[] = []
  for (let i = 0; i < frame.length; i += 13) frags.push(frame.subarray(i, i + 13))
  const [sink, get] = bridgeCollect()
  await pipelineAsync(Readable.from(frags, { objectMode: false }), createLz4Decompress(), sink)
  t.deepEqual(get(), small)
})

test('createLz4Compress destroys mid-stream without hanging', async (t) => {
  const chunks: Buffer[] = []
  for (let i = 0; i < 1000; i++) chunks.push(Buffer.alloc(64 * 1024))
  const transform = createLz4Compress()
  const done = pipelineAsync(
    Readable.from(chunks, { objectMode: false }),
    transform,
    new Writable({
      write(_c, _e, cb) {
        cb()
      },
    }),
  )
  await new Promise((r) => setTimeout(r, 30))
  transform.destroy(new Error('aborted by caller'))
  let settled = false
  let captured: Error | null = null
  try {
    await done
    settled = true
  } catch (e) {
    settled = true
    captured = e as Error
  }
  t.true(settled, 'pipeline must settle after destroy')
  if (captured) t.regex(captured.message, /aborted/i)
})

test('createLz4Compress rejects objectMode / writableObjectMode / readableObjectMode', (t) => {
  t.throws(() => createLz4Compress({ objectMode: true } as never), { instanceOf: TypeError })
  t.throws(() => createLz4Decompress({ writableObjectMode: true } as never), { instanceOf: TypeError })
  t.throws(() => createLz4Compress({ readableObjectMode: true } as never), { instanceOf: TypeError })
  t.throws(() => createLz4Compress({ decodeStrings: false } as never), { instanceOf: TypeError })
})

test('createLz4Compress rejects lifecycle hook overrides', (t) => {
  for (const option of ['construct', 'read', 'write', 'writev', 'final', 'destroy', 'transform', 'flush']) {
    t.throws(() => createLz4Compress({ [option]: () => {} } as never), { instanceOf: TypeError })
  }
})

test('createLz4Compress reports destroy errors to pending write callbacks', async (t) => {
  const transform = createLz4Compress()
  const abortError = new Error('aborted by caller')
  transform.on('error', () => {})
  transform.write(Buffer.from('first chunk'))
  const callbackError = new Promise<Error | null | undefined>((resolve) => {
    transform.write(Buffer.from('second chunk'), resolve)
  })
  transform.destroy(abortError)
  t.is(await callbackError, abortError)
})

test('createLz4Compress accepts frame options and roundtrips', async (t) => {
  const [s1, get1] = bridgeCollect()
  await pipelineAsync(
    Readable.from([bridgeFixture], { objectMode: false }),
    createLz4Compress({ blockSize: BlockSize.Max64KB, contentChecksum: true }),
    s1,
  )
  const compressed = get1()
  t.true(compressed.length > 0)
  const [s2, get2] = bridgeCollect()
  await pipelineAsync(Readable.from([compressed], { objectMode: false }), createLz4Decompress(), s2)
  t.deepEqual(get2(), bridgeFixture)
})

test('createLz4Decompress drops trailing frame delivered as a separate post-EOF chunk', async (t) => {
  // Complete frame in one chunk, then trailing garbage in a SECOND chunk.
  // The native single-frame decoder EOFs after the frame; the post-EOF write
  // must be dropped (not error) so the pipeline settles — matching the
  // single-chunk trailing test and `decompressFrame` (one frame, drop rest).
  const small = Buffer.alloc(64 * 1024, 0xcd)
  const frame = compressFrameSync(small, { contentChecksum: true })
  const [sink, get] = bridgeCollect()
  await pipelineAsync(
    Readable.from([frame, Buffer.from('trailing garbage in its own chunk')], { objectMode: false }),
    createLz4Decompress(),
    sink,
  )
  t.deepEqual(get(), small)
})

test('createLz4Compress applies ingress backpressure when downstream stalls', (t) => {
  // Sink that accepts writes but never reads (refuses to drain). With the
  // pull-coupled _transform callback, the Transform's writable buffer must
  // fill and `write()` must return `false` once it exceeds highWaterMark —
  // proving a fast producer cannot accumulate unbounded input.
  const stalledSink = new Writable({
    write(_c, _e, _cb) {
      /* never drain */
    },
  })
  stalledSink.on('error', () => {})
  const transform = createLz4Compress({ highWaterMark: 16 })
  transform.pipe(stalledSink)
  let sawBackpressure = false
  for (let i = 0; i < 64; i++) {
    const ok = transform.write(Buffer.alloc(64 * 1024))
    if (!ok) {
      sawBackpressure = true
      break
    }
  }
  transform.destroy()
  t.true(sawBackpressure, 'write() must return false under sustained load with a stalled sink')
})
