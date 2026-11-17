/**
 * `zlib` with zstd when the runtime has it (Node >= 22.15 / 24) and null
 * otherwise. Kept in its own module so the transfer strategy can degrade to an
 * uncompressed tar stream instead of failing.
 */

let zlib = null
try {
  // eslint-disable-next-line no-undef
  zlib = await import('node:zlib')
  if (typeof zlib.createZstdCompress !== 'function') zlib = null
} catch {
  zlib = null
}

export { zlib }
