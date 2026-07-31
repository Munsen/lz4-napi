import { Transform, TransformOptions } from 'node:stream'

export * from './native'

export interface Lz4FrameOptions {
  blockSize?: BlockSize
  blockMode?: BlockMode
  blockChecksums?: boolean
  contentChecksum?: boolean
  contentSize?: number
}

// Node Transform options pass-through (highWaterMark, etc.). Byte-stream
// only: objectMode / writableObjectMode / readableObjectMode are not supported
// (the native codec only accepts `Buffer` / `Uint8Array`), and `decodeStrings`
// is forced `true` so callers cannot feed strings. Lifecycle hooks are managed
// by the LZ4 bridge and cannot be overridden.
export type Lz4TransformOptions = Omit<
  TransformOptions,
  | 'objectMode'
  | 'writableObjectMode'
  | 'readableObjectMode'
  | 'decodeStrings'
  | 'construct'
  | 'read'
  | 'write'
  | 'writev'
  | 'final'
  | 'destroy'
  | 'transform'
  | 'flush'
> &
  Lz4FrameOptions

export declare class Lz4Compress extends Transform {
  constructor(options?: Lz4TransformOptions)
}

export declare class Lz4Decompress extends Transform {
  constructor(options?: Lz4TransformOptions)
}

export declare function createLz4Compress(options?: Lz4TransformOptions): Lz4Compress
export declare function createLz4Decompress(options?: Lz4TransformOptions): Lz4Decompress

// gzip-style dual-mode (zlib.gzip(buf, cb) analogue): omit the callback to
// receive a Promise; pass an error-first callback to use a callback.
export declare function compressFrame(
  data: string | Uint8Array,
  callback: (err: Error | null, result: Buffer) => void,
): void
export declare function compressFrame(
  data: string | Uint8Array,
  options: Lz4FrameOptions,
  callback: (err: Error | null, result: Buffer) => void,
): void
export declare function compressFrame(data: string | Uint8Array, options?: Lz4FrameOptions): Promise<Buffer>

export declare function decompressFrame(
  data: string | Uint8Array,
  callback: (err: Error | null, result: Buffer) => void,
): void
export declare function decompressFrame(data: string | Uint8Array): Promise<Buffer>
