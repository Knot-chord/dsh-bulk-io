/**
 * The I/O worker: every filesystem syscall happens here, synchronously, on its
 * own thread.
 *
 * Why blocking calls on purpose: Node's async fs is dispatched to libuv's
 * process-wide thread pool, which defaults to four threads no matter how many
 * promises you await. Four blocking calls, each on its own off-main thread, are
 * real parallelism; `Promise.all` over `fs.promises` is not. It also keeps the
 * Harness event loop free while thousands of files are written.
 *
 * The module exports runOp() so the engine can execute the exact same operation
 * in-thread as a fallback when a worker thread cannot be created.
 */

import {
  appendFileSync,
  closeSync,
  constants as FS,
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  openSync,
  readSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  unlinkSync,
  utimesSync,
  writeFileSync,
} from 'node:fs'
import { dirname, join, relative, resolve, sep } from 'node:path'
import { isMainThread, parentPort } from 'node:worker_threads'
import { matcher, toPosix } from './glob.js'

const BINARY_SNIFF = 8192
const DIR_CACHE = new Set()
const DIR_CACHE_MAX = 20_000

function ensureDir (dir) {
  if (DIR_CACHE.has(dir)) return
  mkdirSync(dir, { recursive: true })
  if (DIR_CACHE.size >= DIR_CACHE_MAX) DIR_CACHE.clear()
  DIR_CACHE.add(dir)
}

/**
 * Run an operation that needs a directory to exist, and treat ENOENT as proof
 * that the cache is stale rather than as a failure.
 *
 * The cache exists because creating 100,000 directories is expensive. It is
 * wrong the moment something outside this worker removes one — a previous
 * delete, another tool, the user — and the failure mode is nasty: every write
 * into a recreated tree fails with ENOENT while the cache says the directory is
 * there. One retry after clearing the cache costs nothing in the normal path.
 */
function withDir (abs, operation) {
  try {
    return operation()
  } catch (err) {
    if (err.code !== 'ENOENT') throw err
    DIR_CACHE.clear()
    mkdirSync(dirname(abs), { recursive: true })
    return operation()
  }
}

/** Forget a removed path and everything under it. */
function forgetDir (target) {
  if (DIR_CACHE.size === 0) return
  const prefix = target.endsWith(sep) ? target : target + sep
  for (const entry of DIR_CACHE) {
    if (entry === target || entry.startsWith(prefix)) DIR_CACHE.delete(entry)
  }
}

function relPosix (root, abs) {
  const rel = relative(root, abs)
  return rel === '' ? '.' : toPosix(rel)
}

function errCode (err) {
  return (err && (err.code || err.message)) || 'unknown error'
}

/**
 * Write many files. Directories are created once and remembered, because the
 * classic slow batch writer re-creates the same directory for every file.
 */
function opWrite (payload) {
  const root = resolve(payload.root)
  const atomic = payload.atomic === true
  let files = 0
  let bytes = 0
  const failed = []
  const t0 = performance.now()

  for (const entry of payload.files) {
    const rel = String(entry.path)
    try {
      const abs = resolve(root, rel)
      ensureDir(dirname(abs))
      const data = entry.base64 === true
        ? Buffer.from(entry.content ?? '', 'base64')
        : String(entry.content ?? '')
      withDir(abs, () => {
        if (entry.append === true) appendFileSync(abs, data)
        else if (atomic) {
          const tmp = abs + '.dsh-bulk-' + process.pid + '-' + Math.random().toString(36).slice(2, 8)
          writeFileSync(tmp, data)
          renameSync(tmp, abs)
        } else writeFileSync(abs, data)
      })
      files += 1
      bytes += typeof data === 'string' ? Buffer.byteLength(data) : data.length
    } catch (err) {
      if (failed.length < 200) failed.push({ path: rel, error: errCode(err) })
    }
  }
  return { files, bytes, failed, ms: performance.now() - t0 }
}

function readWhole (abs, maxBytes) {
  const st = statSync(abs)
  if (!st.isFile()) {
    const err = new Error('not a regular file')
    err.code = 'EISDIR'
    throw err
  }
  const want = Math.min(st.size, Math.max(0, maxBytes))
  const fd = openSync(abs, 'r')
  try {
    const buf = Buffer.allocUnsafe(want)
    let off = 0
    while (off < want) {
      const got = readSync(fd, buf, off, want - off, off)
      if (got <= 0) break
      off += got
    }
    return { buf: off === want ? buf : buf.subarray(0, off), size: st.size, mtimeMs: st.mtimeMs }
  } finally {
    closeSync(fd)
  }
}

function looksBinary (buf) {
  const end = Math.min(buf.length, BINARY_SNIFF)
  for (let i = 0; i < end; i++) if (buf[i] === 0) return true
  return false
}

function sliceLines (text, head, tail) {
  if (!head && !tail) return text
  const lines = text.split('\n')
  if (head && tail) {
    if (lines.length <= head + tail) return text
    const omitted = lines.length - head - tail
    return [...lines.slice(0, head), `… [${omitted} lines omitted] …`, ...lines.slice(-tail)].join('\n')
  }
  if (head && lines.length > head) {
    return [...lines.slice(0, head), `… [${lines.length - head} lines omitted] …`].join('\n')
  }
  if (tail && lines.length > tail) {
    return [`… [${lines.length - tail} lines omitted] …`, ...lines.slice(-tail)].join('\n')
  }
  return text
}

function opRead (payload) {
  const root = resolve(payload.root)
  const maxBytesPerFile = Number.isFinite(payload.maxBytesPerFile) ? payload.maxBytesPerFile : 65536
  const grep = typeof payload.grep === 'string' && payload.grep !== ''
    ? new RegExp(payload.grep, payload.grepFlags || 'i')
    : null
  const head = Number.isFinite(payload.headLines) ? Math.max(0, payload.headLines) : 0
  const tail = Number.isFinite(payload.tailLines) ? Math.max(0, payload.tailLines) : 0
  const binary = payload.binary || 'skip'

  const items = []
  let bytes = 0
  let skippedBinary = 0
  let skippedGrep = 0
  const t0 = performance.now()

  for (const req of payload.paths) {
    const rel = String(req)
    try {
      const { buf, size, mtimeMs } = readWhole(resolve(root, rel), maxBytesPerFile)
      const isBin = looksBinary(buf)
      const truncated = size > buf.length

      if (isBin) {
        if (binary === 'skip') {
          skippedBinary += 1
          items.push({ path: rel, size, binary: true, skipped: true })
          continue
        }
        const text = binary === 'hex' ? buf.toString('hex') : buf.toString('base64')
        bytes += text.length
        items.push({ path: rel, size, binary: true, encoding: binary, text, truncated })
        continue
      }

      let text = buf.toString('utf8')
      if (grep) {
        const kept = text.split('\n').filter((line) => grep.test(line))
        if (kept.length === 0) {
          skippedGrep += 1
          continue
        }
        text = kept.join('\n')
      }
      text = sliceLines(text, head, tail)
      bytes += Buffer.byteLength(text)
      items.push({ path: rel, size, mtimeMs, text, truncated })
    } catch (err) {
      items.push({ path: rel, error: errCode(err) })
    }
  }
  return { items, bytes, skippedBinary, skippedGrep, ms: performance.now() - t0 }
}

function opCopy (payload) {
  const overwrite = payload.overwrite !== false
  const preserveTimes = payload.preserveTimes !== false
  // COPYFILE_FICLONE asks the filesystem for a copy-on-write clone (ReFS, APFS,
  // btrfs, XFS). It falls back to a real copy where unsupported, so the probe
  // only decides whether paying the attempt is worth it.
  const cloneFlag = payload.clone === true ? FS.COPYFILE_FICLONE : 0
  const mode = (overwrite ? 0 : FS.COPYFILE_EXCL) | cloneFlag
  let files = 0
  let bytes = 0
  const failed = []
  const t0 = performance.now()

  for (const pair of payload.pairs) {
    try {
      ensureDir(dirname(pair.dst))
      withDir(pair.dst, () => copyFileSync(pair.src, pair.dst, mode))
      if (preserveTimes && process.platform !== 'win32') {
        // CopyFileW already carries timestamps and attributes across; only the
        // POSIX path needs this extra pair of syscalls.
        try {
          const st = statSync(pair.src)
          utimesSync(pair.dst, st.atime, st.mtime)
        } catch { /* timestamps are best effort */ }
      }
      files += 1
      bytes += pair.size || 0
    } catch (err) {
      if (failed.length < 200) failed.push({ src: pair.src, dst: pair.dst, error: errCode(err) })
    }
  }
  return { files, bytes, failed, ms: performance.now() - t0 }
}

function moveOne (src, dst, overwrite) {
  if (!overwrite && existsSync(dst)) {
    const err = new Error('destination exists')
    err.code = 'EEXIST'
    throw err
  }
  try {
    renameSync(src, dst)
    return 'rename'
  } catch (err) {
    if (err.code !== 'EXDEV' && err.code !== 'EPERM' && err.code !== 'EACCES') throw err
  }
  copyFileSync(src, dst, overwrite ? 0 : FS.COPYFILE_EXCL)
  unlinkSync(src)
  return 'copy'
}

function opMove (payload) {
  const overwrite = payload.overwrite !== false
  let files = 0
  let bytes = 0
  let renames = 0
  const failed = []
  const t0 = performance.now()

  for (const pair of payload.pairs) {
    try {
      ensureDir(dirname(pair.dst))
      if (moveOne(pair.src, pair.dst, overwrite) === 'rename') renames += 1
      files += 1
      bytes += pair.size || 0
    } catch (err) {
      if (failed.length < 200) failed.push({ src: pair.src, dst: pair.dst, error: errCode(err) })
    }
  }
  return { files, bytes, renames, failed, ms: performance.now() - t0 }
}

function opDelete (payload) {
  let files = 0
  let bytes = 0
  const failed = []
  const t0 = performance.now()
  for (const raw of payload.targets) {
    // A target may carry a weight: a subtree of 500 files counts as 500 so the
    // caller's progress is in files, not in "pieces of work finished".
    const target = typeof raw === 'string' ? raw : raw.path
    const weight = typeof raw === 'string' ? 1 : (raw.weight ?? 1)
    try {
      const st = lstatSync(target, { throwIfNoEntry: false })
      if (!st) continue
      if (st.isDirectory() && !st.isSymbolicLink()) {
        rmSync(target, { recursive: true, force: true })
        forgetDir(target)
      } else unlinkSync(target)
      files += weight
      bytes += st.size || 0
    } catch (err) {
      // The postcondition of a delete is "the path is absent". If something else
      // removed it between the lstat and the unlink — a parent leaf, another
      // tool — the goal is already met, so this is not a failure.
      if (err.code === 'ENOENT') {
        files += weight
        continue
      }
      if (failed.length < 200) failed.push({ path: target, error: errCode(err) })
    }
  }
  return { files, bytes, failed, ms: performance.now() - t0 }
}

/**
 * Walk a tree and return the files under it plus the frontier directories that
 * were not descended into. The engine re-queues that frontier as new jobs, which
 * is what makes one wide tree (node_modules) parallel instead of serial.
 */
function opScan (payload) {
  const root = resolve(payload.root)
  const include = matcher(payload.include)
  const exclude = matcher(payload.exclude)
  const maxDepth = Number.isFinite(payload.maxDepth) ? payload.maxDepth : -1
  const chunkDepth = Number.isFinite(payload.chunkDepth) ? payload.chunkDepth : 3
  const maxFiles = Number.isFinite(payload.maxFiles) ? payload.maxFiles : 200_000
  const withSize = payload.withSize !== false

  const files = []
  const frontier = []
  const errors = []
  const stack = [{ dir: resolve(root, payload.dir ?? '.'), depth: payload.depth || 0 }]
  const t0 = performance.now()

  while (stack.length > 0) {
    const { dir, depth } = stack.pop()
    let entries
    try {
      entries = readdirSync(dir, { withFileTypes: true })
    } catch (err) {
      if (errors.length < 50) errors.push({ path: relPosix(root, dir), error: errCode(err) })
      continue
    }
    for (const entry of entries) {
      const abs = join(dir, entry.name)
      const rel = relPosix(root, abs)
      // A junction or symlink is neither followed nor returned: resolving it is
      // how a "delete under root" ends up deleting something outside root.
      if (entry.isSymbolicLink()) continue
      if (entry.isDirectory()) {
        if (exclude && exclude(rel)) continue
        if (maxDepth >= 0 && depth + 1 > maxDepth) continue
        if (depth + 1 >= chunkDepth) frontier.push({ dir: abs, depth: depth + 1 })
        else stack.push({ dir: abs, depth: depth + 1 })
        continue
      }
      if (!entry.isFile()) continue
      if (include && !include(rel)) continue
      if (exclude && exclude(rel)) continue
      if (files.length >= maxFiles) {
        return { files, frontier, truncated: true, errors, ms: performance.now() - t0 }
      }
      let size = 0
      if (withSize) {
        try {
          size = statSync(abs).size
        } catch {
          continue
        }
      }
      files.push({ path: rel, size })
    }
  }
  return { files, frontier, truncated: false, errors, ms: performance.now() - t0 }
}

/**
 * Aggregate a tree instead of listing it: counts, bytes, extension histogram and
 * the largest members. This is what makes a 200,000-file directory answerable
 * without materialising 200,000 path strings or 200,000 rows of output.
 */
function opStats (payload) {
  const root = resolve(payload.root)
  const include = matcher(payload.include)
  const exclude = matcher(payload.exclude)
  const maxDepth = Number.isFinite(payload.maxDepth) ? payload.maxDepth : -1
  const chunkDepth = Number.isFinite(payload.chunkDepth) ? payload.chunkDepth : 3
  const maxFiles = Number.isFinite(payload.maxFiles) ? payload.maxFiles : 5_000_000
  const topN = Math.max(0, Math.min(100, Number.isFinite(payload.topN) ? payload.topN : 20))
  const withSize = payload.withSize !== false

  const byExt = new Map()
  const largest = []
  const frontier = []
  const errors = []
  const stack = [{ dir: resolve(root, payload.dir ?? '.'), depth: payload.depth || 0 }]
  let files = 0
  let bytes = 0
  let truncated = false

  const noteExt = (name, size) => {
    const dot = name.lastIndexOf('.')
    const ext = dot > 0 ? name.slice(dot + 1).toLowerCase().slice(0, 16) : '(none)'
    const entry = byExt.get(ext) ?? { count: 0, bytes: 0 }
    entry.count += 1
    entry.bytes += size
    byExt.set(ext, entry)
  }

  const noteLargest = (rel, size) => {
    if (topN === 0) return
    if (largest.length >= topN && size <= largest[largest.length - 1].size) return
    largest.push({ path: rel, size })
    largest.sort((a, b) => b.size - a.size)
    if (largest.length > topN) largest.length = topN
  }

  const t0 = performance.now()
  while (stack.length > 0) {
    const { dir, depth } = stack.pop()
    let entries
    try {
      entries = readdirSync(dir, { withFileTypes: true })
    } catch (err) {
      if (errors.length < 50) errors.push({ path: relPosix(root, dir), error: errCode(err) })
      continue
    }
    for (const entry of entries) {
      const abs = join(dir, entry.name)
      const rel = relPosix(root, abs)
      if (entry.isSymbolicLink()) continue
      if (entry.isDirectory()) {
        if (exclude && exclude(rel)) continue
        if (maxDepth >= 0 && depth + 1 > maxDepth) continue
        if (depth + 1 >= chunkDepth) frontier.push({ dir: abs, depth: depth + 1 })
        else stack.push({ dir: abs, depth: depth + 1 })
        continue
      }
      if (!entry.isFile()) continue
      if (include && !include(rel)) continue
      if (exclude && exclude(rel)) continue
      if (files >= maxFiles) {
        truncated = true
        break
      }
      let size = 0
      if (withSize) {
        try {
          size = statSync(abs).size
        } catch {
          continue
        }
      }
      files += 1
      bytes += size
      noteExt(entry.name, size)
      noteLargest(rel, size)
    }
    if (truncated) break
  }

  return {
    files,
    bytes,
    byExt: [...byExt.entries()],
    largest,
    frontier,
    truncated,
    errors,
    ms: performance.now() - t0,
  }
}

/**
 * Count entries beneath directories using readdir only. Weights for a delete
 * plan are computed here, across the pool, because doing it on one thread costs
 * ~30 s for 160,000 entries against ~7 s spread over eight workers.
 */
function opCount (payload) {
  const pairs = []
  const skip = new Set(payload.skipNames ?? [])
  let total = 0
  for (const dir of payload.dirs) {
    const abs = resolve(dir)
    let count = 0
    const stack = [abs]
    while (stack.length > 0) {
      const current = stack.pop()
      let entries
      try {
        entries = readdirSync(current, { withFileTypes: true })
      } catch {
        continue
      }
      for (const entry of entries) {
        // The same names the scan skips: counting the trash would make the
        // progress denominator larger than the work it is measuring.
        if (skip.has(entry.name)) continue
        const isDirectory = entry.isDirectory() && !entry.isSymbolicLink()
        if (!isDirectory || !payload.filesOnly) count += 1
        if (isDirectory) stack.push(join(current, entry.name))
      }
    }
    pairs.push({ dir, count })
    total += count
  }
  return { pairs, total }
}

const OPS = {
  write: opWrite,
  read: opRead,
  copy: opCopy,
  move: opMove,
  delete: opDelete,
  count: opCount,
  scan: opScan,
  stats: opStats,
}

export function runOp (op, payload) {
  const fn = OPS[op]
  if (!fn) throw new Error('unknown bulk-io op: ' + op)
  return fn(payload)
}

function serve () {
  if (!parentPort) return
  parentPort.on('message', (message) => {
    const { id, op, payload } = message
    try {
      parentPort.postMessage({ id, ok: true, value: runOp(op, payload) })
    } catch (err) {
      parentPort.postMessage({ id, ok: false, error: errCode(err) })
    }
  })
}

if (!isMainThread) serve()
