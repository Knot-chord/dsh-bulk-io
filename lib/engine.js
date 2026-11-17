/**
 * The batch engine: planning, safety checks, strategy choice and result
 * assembly. The syscalls themselves happen in lib/worker.js.
 *
 * Every public function returns the same shape so the tools and the CLI can
 * share it: { ok, text, summary, ...fields }.
 */

import { existsSync, lstatSync, mkdirSync, readdirSync, realpathSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { rename as renameAsync } from 'node:fs/promises'
import { basename, dirname, isAbsolute, join, parse, relative, resolve, sep } from 'node:path'
import * as fmt from './format.js'
import { defaultPoolSize, getPool } from './pool.js'
import { describePolicy, proveWritable, proveWritableDirs } from './policy.js'
import { cloneSupport, scannerName } from './probe.js'
import * as tuning from './tuning.js'

import { createMeter, runPaced, scanPairsStream } from './pipeline.js'
import { archivePipe, isUNC, robocopy, sameVolume } from './transfer.js'
import { runOp } from './worker.js'

/* ---------------------------------------------------------------- helpers --
 * Path/list helpers, the batch dispatcher and the delete planner live here.
 * They were briefly separate modules; three files of 37, 47 and 169 lines for
 * one call chain is indirection without benefit, so they are back in one place
 * with section markers instead of a dependency graph.
 */

export function asList (value) {
  if (value === undefined || value === null || value === '') return []
  return Array.isArray(value) ? value.map(String) : [String(value)]
}

export function hasGlob (text) {
  return /[*?[\]{}]/.test(text)
}

export function splitGlob (pattern) {
  const posix = String(pattern).replace(/\\/g, '/')
  const segments = posix.split('/')
  const base = []
  let index = 0
  for (; index < segments.length; index++) {
    if (hasGlob(segments[index])) break
    base.push(segments[index])
  }
  return { base: base.join('/') || '.', pattern: segments.slice(index).join('/') }
}

export function chunk (items, size) {
  if (items.length === 0) return []
  const width = Math.max(1, size)
  const out = []
  for (let i = 0; i < items.length; i += width) out.push(items.slice(i, i + width))
  return out
}

export function poolFor (size) {
  const pool = getPool(size)
  if (size > 0) pool.grow(size)
  if (!pool.ready && !pool.broken) pool.warm()
  return pool
}

export async function exec (pool, op, payload) {
  if (pool.broken) return runOp(op, payload)
  return pool.run(op, payload)
}

/**
 * One op over many payloads. If the worker pool cannot be used at all — no
 * threads in this build, or every worker dying on import — the same operations
 * run in-thread so the tool still answers instead of silently doing nothing.
 */
export async function runBatch (pool, op, payloads, onValue, signal) {
  if (payloads.length === 0) return []
  if (!pool.broken) {
    try {
      return await pool.map(op, payloads, (values) => values, onValue, signal)
    } catch (err) {
      if (!pool.broken) throw err
    }
  }
  const values = []
  for (const payload of payloads) {
    if (signal?.aborted) break
    const value = runOp(op, payload)
    try { onValue?.(value) } catch { /* progress must not break the run */ }
    values.push(value)
  }
  return values
}

export function countEntries (dir, { signal, filesOnly = false, skipNames = [TRASH_DIR], skipLinks = false } = {}) {
  let total = 0
  const stack = [dir]
  while (stack.length > 0) {
    if (signal?.aborted) break
    const current = stack.pop()
    let entries
    try {
      entries = readdirSync(current, { withFileTypes: true })
    } catch {
      continue
    }
    for (const entry of entries) {
      if (skipNames.includes(entry.name)) continue
      // A link is neither a file to copy nor a directory to descend: the scan
      // does not follow it, so a count that includes it would announce work the
      // transfer will never do.
      if (skipLinks && entry.isSymbolicLink()) continue
      const isDirectory = entry.isDirectory() && !entry.isSymbolicLink()
      if (isDirectory) stack.push(join(current, entry.name))
      if (!isDirectory || !filesOnly) total += 1
    }
  }
  return total
}

/**
 * Prove a delete plan instead of trusting it.
 *
 * Every bug in this planner has been the same shape: a rule that assumed
 * something nobody checked. A single-entry directory was assumed to be one unit
 * (so a 93,000-file tree ran on one worker); a weight was assumed to be a file
 * count (so progress read 103,627 for 93,009 files); units were assumed not to
 * overlap (so a parent could be removed while a child was in flight).
 *
 * The plan is small — hundreds of units, never hundreds of thousands — so it can
 * be checked on every call in O(n log n): sorted paths make ancestor detection a
 * neighbour test, and the weight sum must equal the announced total. When the
 * check fails it throws with the invariant's name, which is the difference
 * between "it got slow" and "the planner is wrong".
 */
export function verifyPlan (plan) {
  const problems = []
  const seen = new Set()
  for (const leaf of plan.leaves) {
    if (seen.has(leaf.path)) problems.push(`duplicate unit: ${leaf.path}`)
    seen.add(leaf.path)
    if (!Number.isFinite(leaf.weight) || leaf.weight < 0) problems.push(`invalid weight: ${leaf.path}`)
  }
  for (const root of plan.roots) {
    if (seen.has(root.path)) problems.push(`unit is also listed as its own parent: ${root.path}`)
  }
  const sum = plan.leaves.reduce((total, leaf) => total + leaf.weight, 0)
    + plan.roots.reduce((total, root) => total + root.weight, 0)
  if (sum !== plan.total) problems.push(`total ${plan.total} does not equal the sum of weights ${sum}`)

  const leaves = [...plan.leaves].sort((a, b) => (a.path < b.path ? -1 : 1))
  for (let i = 1; i < leaves.length; i += 1) {
    const previous = leaves[i - 1].path
    const current = leaves[i].path
    if (current.startsWith(previous.endsWith(sep) ? previous : previous + sep)) {
      problems.push(`overlapping units: ${previous} contains ${current}`)
    }
  }
  return problems
}

/**
 * Plan a permanent delete: split the targets into units small enough to run in
 * parallel, to report file-accurate progress, and to stop quickly when
 * cancelled.
 *
 * Splitting must go deep. A first version stopped at the first directory with
 * more than one child, which on `dest / workspace / {7 directories}` produced
 * seven leaves — one slice, one worker, 320 files/s, no progress until the end.
 * The plan now descends until it has `minLeaves` units, so a 160,000-file tree
 * becomes hundreds of independent deletions.
 *
 * Weights (the entry count inside each unit) make progress file-accurate and are
 * computed across the pool: 160,000 entries take ~7 s on eight workers instead
 * of ~30 s on one.
 */
export async function planDelete (targets, { maxDepth = 8, minLeaves = tuning.deleteUnits(), pool, signal } = {}) {
  const leaves = []
  const parents = []
  const queue = targets.map((path) => ({ path, depth: 0 }))

  while (queue.length > 0) {
    if (signal?.aborted) break
    const { path, depth } = queue.shift()
    let stat
    try {
      stat = lstatSync(path, { throwIfNoEntry: false })
    } catch {
      stat = null
    }
    if (!stat) continue
    if (!stat.isDirectory() || stat.isSymbolicLink()) {
      leaves.push({ path, weight: 1, kind: 'file' })
      continue
    }
    let entries
    try {
      entries = readdirSync(path, { withFileTypes: true })
    } catch {
      leaves.push({ path, weight: null, kind: 'dir' })
      continue
    }
    if (entries.length === 0) {
      leaves.push({ path, weight: 0, kind: 'dir' })
      continue
    }
    // A directory with a single child is a chain, not a unit: descending through
    // it is what turns `dest / a / b / {600 directories}` into 600 parallel
    // units instead of one leaf that occupies a single worker. This check must
    // come before the width rule — a chain of one entry is always narrow, and
    // gating it on width silently collapses the whole tree into one unit.
    if (entries.length === 1 && leaves.length < minLeaves && depth < maxDepth) {
      parents.push(path)
      queue.push({ path: join(path, entries[0].name), depth: depth + 1 })
      continue
    }
    // Split only where there is a subtree to split into. A directory whose
    // children are all files is one efficient rmSync; turning its 100 files into
    // 100 units costs 100 dispatches and 100 lstats for the same entries.
    const hasSubdirectories = entries.some((entry) => entry.isDirectory() && !entry.isSymbolicLink())
    const roomToSplit = hasSubdirectories && leaves.length < minLeaves && depth < maxDepth
    if (!roomToSplit) {
      leaves.push({ path, weight: null, kind: 'dir' })
      continue
    }
    parents.push(path)
    for (const entry of entries) queue.push({ path: join(path, entry.name), depth: depth + 1 })
  }

  const pending = leaves.filter((leaf) => leaf.weight === null)
  if (pending.length > 0) {
    if (pool && !pool.broken) {
      const width = Math.max(1, Math.ceil(pending.length / (pool.size * 2)))
      const slices = chunk(pending, width)
      const results = await runBatch(
        pool,
        'count',
        slices.map((slice) => ({ dirs: slice.map((leaf) => leaf.path), filesOnly: true, skipNames: [TRASH_DIR] })),
        null,
        signal,
      )
      const counts = new Map()
      for (const result of results) {
        for (const pair of result.pairs) counts.set(pair.dir, pair.count)
      }
      for (const leaf of pending) leaf.weight = counts.get(leaf.path) ?? 1
    } else {
      for (const leaf of pending) leaf.weight = Math.max(1, countEntries(leaf.path, { filesOnly: true, signal }))
    }
  }

  const total = leaves.reduce((sum, leaf) => sum + (leaf.weight ?? 0), 0)
  // Deepest first, so a parent is removed only after everything under it.
  const roots = parents.reverse().map((path) => ({ path, weight: 0, kind: 'dir' }))
  const dirs = leaves.filter((leaf) => leaf.kind === 'dir').length + roots.length
  const plan = { leaves, roots, total, dirs }
  const problems = verifyPlan(plan)
  if (problems.length > 0) {
    throw Object.assign(
      new Error(`delete plan failed its own check: ${problems.slice(0, 3).join('; ')}`),
      { code: 'EPLANNVERIFY' },
    )
  }
  return plan
}


/**
 * Estimate how many entries a set of paths holds, using readdir only.
 * Returns null when a path is missing or is a glob, so the caller can fall back
 * to the time-based budget instead of trusting a bad number.
 */
export function estimateEntries (paths, options = {}) {
  const cwd = options.cwd
  const counting = { skipLinks: true, ...options }
  let total = 0
  for (const path of paths ?? []) {
    if (typeof path !== 'string' || hasGlob(path)) return null
    const abs = fromCwd(cwd, path)
    let stat
    try {
      stat = statSync(abs, { throwIfNoEntry: false })
    } catch {
      return null
    }
    if (!stat) return null
    total += stat.isDirectory() ? countEntries(abs, counting) : 1
  }
  return total
}

export async function planDeletePaths ({ root, paths, pool, signal, cwd }) {
  const rootAbs = fromCwd(cwd, root)
  const targets = asList(paths).map((path) => resolve(rootAbs, path))
  if (targets.length === 0) return null
  return planDelete(targets, { pool: pool ?? poolFor(0), signal })
}


/**
 * What each operation costs in primitives, and on which plane.
 *
 * File work is not one kind of I/O. There is a namespace plane (create, rename,
 * unlink, stat, readdir: a fixed cost per entry and no bytes at all) and a data
 * plane (read, write: bytes plus a per-call cost). Copy and move are not the
 * same layer as write — they are composites whose cost is the sum of the
 * primitives they issue:
 *
 *   write   3/entry   create + write + close            (one data stream)
 *   read    4/entry   open + stat + read + close
 *   copy    5/entry   create + open + read + write + close
 *   move    2/entry   open + rename                     (no bytes)
 *   delete  3/entry   lstat + unlink + its share of readdir
 *   scan    1/entry   its share of readdir
 *
 * The counts are what this plugin issues, not what the kernel adds around them.
 * They are here so a result can say "465,000 primitives for 93,000 files" and a
 * reader can divide by the machine's per-primitive cost instead of guessing why
 * a copy and a move of the same tree differ by 300x.
 */
const PLANE = {
  write: { primitives: 3, data: true },
  read: { primitives: 4, data: true },
  copy: { primitives: 5, data: true },
  move: { primitives: 2, data: false },
  delete: { primitives: 3, data: false },
  scan: { primitives: 1, data: false },
}

/**
 * The result text for every transfer path.
 *
 * There used to be two of these — the thread pipeline and the robocopy/archive
 * branch — and when the cost model changed only one was updated, so the same
 * operation reported two different formats depending on which strategy ran.
 * One builder, called by both.
 */
function transferText ({ verb, files, bytes, ms, strategy, via, destAbs, failed = [], costOp, cold, scanner }) {
  return [
    `${verb} ${fmt.fmtCount(files)} files (${fmt.fmtBytes(bytes)}) in ${fmt.fmtMs(ms)} via ${strategy}${via ? ` (${via})` : ''} -> ${destAbs}`,
    failed.length > 0
      ? `${failed.length} failed:\n` + failed.slice(0, 20).map((f) => `  ${f.src}: ${f.error}`).join('\n')
      : '',
    describeCost(costOp, files, bytes, ms),
    cold ? '' : fmt.slowHint(files, ms, scanner),
  ].filter(Boolean).join('\n')
}

/** The one-line cost model a large result can show. */
function describeCost (op, files, bytes, ms) {
  const plane = PLANE[op]
  if (!plane || !Number.isFinite(files) || files < 1000) return ''
  const ops = files * plane.primitives
  const rate = ms > 0 ? fmt.fmtCount(Math.round(ops / (ms / 1000))) : '?'
  const parts = []
  if (plane.data && bytes > 0) parts.push(`${fmt.fmtRate(bytes, ms)} through the data plane`)
  else parts.push('namespace plane only, no file data moved')
  parts.push(`${fmt.fmtCount(ops)} primitives at ${rate}/s (${plane.primitives}/file)`)
  const budget = fmt.perThousand(files, ms)
  if (budget) parts.push(budget)
  return parts.join(' · ')
}

/**
 * Resolve a caller-supplied path.
 *
 * The tools run inside one process that serves many Sessions, and a Session has
 * its own working directory — so `process.cwd()` is the wrong answer for "src".
 * The caller passes its Session directory; anything absolute is left alone.
 */
function fromCwd (cwd, value) {
  const text = String(value)
  return isAbsolute(text) ? resolve(text) : resolve(cwd ?? process.cwd(), text)
}

const TRASH_DIR = '.dsh-trash'
const DEFAULT_EXCLUDES = [`**/${TRASH_DIR}`, `**/${TRASH_DIR}/**`]
const MAX_WRITE_ENTRIES = 50_000
const MAX_SCAN_FILES = 200_000

/** Used by the CLI and by tests: no session, no boundary. */
export const OPEN_POLICY = {
  mode: 'danger-full-access',
  workspaceRoot: process.cwd(),
  additionalWritableRoots: [],
  source: 'open',
}

/* ------------------------------------------------------------------ helpers */

function inside (root, target) {
  const r = resolve(root)
  const t = resolve(target)
  if (t === r) return true
  const prefix = r.endsWith(sep) ? r : r + sep
  return t.toLowerCase().startsWith(prefix.toLowerCase())
}

export function assertDeletable (rootAbs, targetAbs) {
  const r = resolve(rootAbs)
  const t = resolve(targetAbs)
  if (r === parse(r).root) {
    throw Object.assign(new Error(`refusing to operate with a filesystem root as root (${r})`), { code: 'EUNSAFEROOT' })
  }
  if (t === parse(t).root) {
    throw Object.assign(new Error(`refusing to delete a filesystem root (${t})`), { code: 'EUNSAFETARGET' })
  }
  if (!inside(r, t)) {
    throw Object.assign(new Error(`refusing to delete outside root: ${t}`), { code: 'EOUTSIDE' })
  }
  if (t === r) {
    throw Object.assign(new Error('refusing to delete the root itself'), { code: 'EUNSAFETARGET' })
  }
}

function realpathOrNull (path) {
  try {
    return realpathSync.native ? realpathSync.native(path) : realpathSync(path)
  } catch {
    return null
  }
}

function realpathDeep (path) {
  const direct = realpathOrNull(path)
  if (direct) return direct
  const parent = dirname(path)
  if (parent === path) return null
  const resolved = realpathDeep(parent)
  if (!resolved) return null
  return join(resolved, basename(path))
}

/**
 * Textual containment is not containment: a junction inside root resolves to a
 * directory outside it, so `root/link/file` passes a string prefix check and
 * then unlinks someone else's file. That actually happened during testing.
 *
 * The rule is therefore "unprovable is refused". The *parent* is resolved
 * through the real filesystem and must be inside the resolved root; the final
 * component is deliberately NOT resolved, so that deleting a junction entry
 * removes the link and never touches what it points at.
 */
export function proveDeletable (rootAbs, targetAbs) {
  assertDeletable(rootAbs, targetAbs)
  const unprovable = (why) => Object.assign(
    new Error(`cannot prove ${targetAbs} is inside ${rootAbs} (${why}); refusing`),
    { code: 'EUNPROVABLE' },
  )
  const realRoot = realpathOrNull(rootAbs)
  if (!realRoot) throw unprovable('root does not resolve')
  const realParent = realpathDeep(dirname(targetAbs))
  if (!realParent) throw unprovable('no ancestor resolves')
  if (!inside(realRoot, realParent)) {
    throw Object.assign(
      new Error(`refusing to delete outside root: ${targetAbs} resolves under ${realParent}`),
      { code: 'EOUTSIDE' },
    )
  }
  const proven = join(realParent, basename(targetAbs))
  if (proven === realRoot) {
    throw Object.assign(new Error('refusing to delete the root itself'), { code: 'EUNSAFETARGET' })
  }
  return proven
}

/* --------------------------------------------------------------------- scan */

/**
 * Walk a tree in widening rounds: each round scans every pending directory up
 * to `chunkDepth`, and whatever lies deeper becomes the next round's work. The
 * result is that one huge directory (node_modules) still uses every worker.
 */
export async function scanTree ({
  cwd,
  root,
  dir = '.',
  include = [],
  exclude = [],
  maxDepth = -1,
  maxFiles = MAX_SCAN_FILES,
  withSize = true,
  pool,
  signal,
} = {}) {
  const activePool = pool ?? poolFor(0)
  const rootAbs = fromCwd(cwd, root)
  const patterns = asList(include)
  const excludes = [...DEFAULT_EXCLUDES, ...asList(exclude)]
  const files = []
  const errors = []
  let truncated = false
  let level = [{ dir, depth: 0 }]
  let round = 0

  while (level.length > 0 && !truncated) {
    signal?.throwIfAborted?.()
    // The first round splits at the top level so a shallow, wide tree (200
    // sibling directories, the common shape) spreads over every worker
    // immediately. Later rounds use a deeper chunk to keep messages few.
    const chunkDepth = round === 0 ? 1 : 3
    round += 1
    const results = await Promise.all(level.map((job) => exec(activePool, 'scan', {
      root: rootAbs,
      dir: job.dir,
      depth: job.depth,
      include: patterns,
      exclude: excludes,
      maxDepth,
      maxFiles: Math.max(1, maxFiles - files.length),
      withSize,
      chunkDepth,
    })))

    const frontier = []
    for (const result of results) {
      for (const file of result.files) {
        if (files.length >= maxFiles) {
          truncated = true
          break
        }
        files.push(file)
      }
      if (result.truncated) truncated = true
      for (const error of result.errors ?? []) if (errors.length < 50) errors.push(error)
      for (const next of result.frontier ?? []) frontier.push(next)
    }
    level = frontier
  }

  return { root: rootAbs, files, errors, truncated }
}

/* -------------------------------------------------------------------- write */

export async function writeFiles ({
  root,
  cwd,
  files,
  atomic = false,
  dryRun = false,
  concurrency = 0,
  policy = OPEN_POLICY,
  onProgress,
  signal,
} = {}) {
  if (!root) throw new Error('root is required')
  if (!Array.isArray(files) || files.length === 0) throw new Error('files must be a non-empty array')
  if (files.length > MAX_WRITE_ENTRIES) {
    throw new Error(`too many entries in one call (${files.length}); split into batches of ${MAX_WRITE_ENTRIES}`)
  }

  const rootAbs = fromCwd(cwd, root)
  const entries = files.map((entry) => {
    if (!entry || typeof entry.path !== 'string' || entry.path === '') {
      throw new Error('every file needs a non-empty string path')
    }
    return {
      path: entry.path,
      content: String(entry.content ?? ''),
      base64: entry.base64 === true,
      append: entry.append === true,
    }
  })

  // One boundary check per distinct directory, not per file: still resolves
  // every directory the batch will touch, but stays O(dirs).
  if (!dryRun) proveWritableDirs(policy, rootAbs, entries.map((entry) => entry.path))

  const declared = entries.reduce((sum, entry) => {
    const length = entry.base64 ? Math.floor(entry.content.length * 3 / 4) : Buffer.byteLength(entry.content)
    return sum + length
  }, 0)

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      files: entries.length,
      bytes: declared,
      text: `[dry run] would write ${fmt.fmtCount(entries.length)} files (${fmt.fmtBytes(declared)}) under ${rootAbs}`,
      summary: `dry run: ${fmt.fmtCount(entries.length)} files`,
    }
  }

  mkdirSync(rootAbs, { recursive: true })
  const pool = poolFor(concurrency)
  const cold = pool.isCold()
  const started = performance.now()
  const width = tuning.chunkSize(entries.length)
  const slices = chunk(entries, width)
  let written = 0
  let writtenBytes = 0
  const results = await runBatch(pool, 'write', slices.map((slice) => ({ root: rootAbs, files: slice, atomic })), (value) => {
    written += value.files
    writtenBytes += value.bytes
    try { onProgress?.({ files: written, bytes: writtenBytes, phase: 'writing' }) } catch { /* ignore */ }
  })

  const merged = results.reduce((acc, value) => ({
    files: acc.files + value.files,
    bytes: acc.bytes + value.bytes,
    failed: acc.failed.concat(value.failed),
  }), { files: 0, bytes: 0, failed: [] })

  const ms = performance.now() - started
  tuning.noteThroughput(merged.files, ms)
  const scope = policy.mode === 'danger-full-access' ? '' : ` · ${describePolicy(policy)}`
  const lines = [`wrote ${fmt.fmtCount(merged.files)} files (${fmt.fmtBytes(merged.bytes)}) in ${fmt.fmtMs(ms)} under ${rootAbs}${scope}`]
  const cost = describeCost('write', merged.files, merged.bytes, ms)
  if (cost) lines.push(cost)
  const hint = cold ? '' : fmt.slowHint(merged.files, ms, scannerName())
  if (hint) lines.push(hint)
  if (merged.failed.length > 0) {
    lines.push(`${merged.failed.length} failed:`)
    for (const failure of merged.failed.slice(0, 20)) lines.push(`  ${failure.path}: ${failure.error}`)
    if (merged.failed.length > 20) lines.push(`  … ${merged.failed.length - 20} more`)
  }
  return {
    ok: merged.failed.length === 0,
    files: merged.files,
    bytes: merged.bytes,
    ms,
    failed: merged.failed,
    summary: fmt.summaryLine({ files: merged.files, bytes: merged.bytes, ms }),
    text: lines.join('\n'),
  }
}

/* --------------------------------------------------------------------- read */

export async function readFiles ({
  root,
  cwd,
  paths = [],
  include = [],
  exclude = [],
  maxDepth = -1,
  maxFiles = 200,
  maxBytesPerFile = 65536,
  maxTotalBytes = 262144,
  binary = 'skip',
  headLines = 0,
  tailLines = 0,
  grep = '',
  concurrency = 0,
  signal,
} = {}) {
  if (!root) throw new Error('root is required')
  const rootAbs = fromCwd(cwd, root)
  const pool = poolFor(concurrency)
  const cold = pool.isCold()

  let targets = asList(paths)
  let scanTruncated = false
  let scanned = targets.length
  if (targets.length === 0) {
    const scan = await scanTree({
      root: rootAbs,
      include: asList(include),
      exclude: asList(exclude),
      maxDepth,
      maxFiles: Math.max(1, maxFiles),
      // The read itself reports real sizes; skipping stat here saves one
      // syscall per file, which dominates on a machine with on-access scanning.
      withSize: false,
      pool,
      signal,
    })
    targets = scan.files.map((file) => file.path)
    scanned = scan.files.length
    scanTruncated = scan.truncated
  }
  if (targets.length === 0) {
    return { ok: true, files: 0, bytes: 0, text: `no files matched under ${rootAbs}`, summary: 'no files matched' }
  }

  const limited = targets.slice(0, maxFiles)
  const width = tuning.chunkSize(limited.length)
  const slices = chunk(limited, width)

  const started = performance.now()
  const results = await runBatch(pool, 'read', slices.map((slice) => ({
    root: rootAbs,
    paths: slice,
    maxBytesPerFile,
    binary,
    headLines,
    tailLines,
    grep,
  })))

  const items = results.flatMap((value) => value.items)
  const ms = performance.now() - started

  let used = 0
  let dropped = 0
  const blocks = []
  for (const item of items) {
    if (item.text !== undefined) {
      const cost = Buffer.byteLength(item.text)
      if (used + cost > maxTotalBytes) {
        dropped += 1
        continue
      }
      used += cost
      const label = item.truncated
        ? `${item.path} · ${fmt.fmtBytes(item.size)} (first ${fmt.fmtBytes(cost)})`
        : `${item.path} · ${fmt.fmtBytes(item.size)}`
      blocks.push(`=== ${label} ===\n${item.text}`)
      continue
    }
    if (item.skipped) {
      blocks.push(`=== ${item.path} · binary (${fmt.fmtBytes(item.size)}), skipped ===`)
      continue
    }
    if (item.error) blocks.push(`=== ${item.path} · ERROR ${item.error} ===`)
  }

  const skippedBinary = items.filter((item) => item.skipped).length
  const failed = items.filter((item) => item.error).length
  const header = [
    `read ${fmt.fmtCount(items.length)} files (${fmt.fmtBytes(used)}) in ${fmt.fmtMs(ms)}`,
    dropped > 0 ? `${dropped} more omitted to stay under the ${fmt.fmtBytes(maxTotalBytes)} output budget` : '',
    scanTruncated ? `scan stopped at ${maxFiles} files; narrow include for the rest` : '',
    skippedBinary > 0 ? `${skippedBinary} binary files skipped (binary:"base64" includes them)` : '',
    failed > 0 ? `${failed} unreadable` : '',
    describeCost('read', items.length, used, ms),
    cold ? '' : fmt.slowHint(items.length, ms, scannerName()),
  ].filter(Boolean)

  return {
    ok: failed === 0,
    files: items.length,
    bytes: used,
    ms,
    dropped,
    scanned,
    text: [...header, '', ...blocks].join('\n'),
    summary: fmt.summaryLine({ files: items.length, bytes: used, ms }),
  }
}

/* ----------------------------------------------------------------- transfer */

const TRANSFER_OPS = new Set(['copy', 'move', 'sync'])

export async function transferFiles ({
  op = 'copy',
  sources = [],
  dest,
  cwd,
  strategy = 'auto',
  threads = 32,
  overwrite = true,
  dryRun = false,
  exclude = [],
  concurrency = 0,
  policy = OPEN_POLICY,
  onProgress,
  knownTotal,
  signal,
} = {}) {
  if (!TRANSFER_OPS.has(op)) throw new Error(`op must be one of ${[...TRANSFER_OPS].join(', ')}`)
  if (!dest) throw new Error('dest is required')
  const sourceList = asList(sources)
  if (sourceList.length === 0) throw new Error('sources must be a non-empty array')

  const destAbs = fromCwd(cwd, dest)
  const pool = poolFor(concurrency)
  const cold = pool.isCold()
  const started = performance.now()

  const items = []
  for (const raw of sourceList) {
    if (hasGlob(raw)) {
      const { base, pattern } = splitGlob(raw)
      const baseAbs = fromCwd(cwd, base)
      const scan = await scanTree({ root: baseAbs, include: [pattern], exclude: asList(exclude), pool, signal })
      for (const file of scan.files) {
        items.push({ kind: 'file', src: join(baseAbs, file.path), dst: join(destAbs, file.path), size: file.size })
      }
      continue
    }
    const abs = fromCwd(cwd, raw)
    const stat = statSync(abs, { throwIfNoEntry: false })
    if (!stat) throw new Error(`source does not exist: ${abs}`)
    items.push(stat.isDirectory()
      ? { kind: 'dir', src: abs, dst: join(destAbs, basename(abs)), size: 0 }
      : { kind: 'file', src: abs, dst: join(destAbs, basename(abs)), size: stat.size })
  }
  if (items.length === 0) throw new Error('no sources matched')

  // The destination is a write; a move also deletes every source.
  proveWritable(policy, destAbs)
  if (op === 'move') for (const item of items) proveWritable(policy, item.src)

  const dirs = items.filter((item) => item.kind === 'dir')
  const singles = items.filter((item) => item.kind === 'file')
  const allSameVolume = items.every((item) => sameVolume(item.src, destAbs))

  let chosen = strategy
  if (!strategy || strategy === 'auto') {
    if (op === 'sync') chosen = 'robocopy'
    else if (op === 'move' && allSameVolume) chosen = 'rename'
    else if (isUNC(destAbs)) chosen = 'robocopy'
    else chosen = 'threads'
  }

  // Renaming a whole entry onto an existing directory fails, and merging is
  // what the caller meant: drop to per-file moves, which create and merge.
  if (chosen === 'rename' && items.some((item) => existsSync(item.dst))) chosen = 'threads'

  // One scan per directory source: it supplies the file/byte accounting and,
  // for the thread strategy, the actual work list. Native strategies still get
  // an accurate summary because of it. A rename moves whole entries, so it only
  // needs a file count and skips the per-file stat entirely.
  // A directory rename needs only a file count and native strategies want an
  // accurate summary, so they scan up front. The threaded strategy streams and
  // never materialises the tree.
  // A rename needs no accounting at all: counting the tree to print "N files
  // inside" made a 93,000-file move take 5.7 s for a 2 ms syscall.
  const wantSizes = chosen !== 'rename'
  const needAccounting = dryRun || (chosen !== 'threads' && chosen !== 'rename')
  const scannedDirs = needAccounting
    ? await Promise.all(dirs.map(async (item) => {
      const scan = await scanTree({ root: item.src, exclude: asList(exclude), withSize: wantSizes, pool, signal })
      return { item, files: scan.files }
    }))
    : []
  const dirFiles = scannedDirs.reduce((sum, entry) => sum + entry.files.length, 0)
  const dirBytes = scannedDirs.reduce((sum, entry) => sum + entry.files.reduce((s, f) => s + (f.size || 0), 0), 0)
  const singleBytes = singles.reduce((sum, item) => sum + (item.size || 0), 0)
  const totalFiles = needAccounting ? dirFiles + singles.length : null
  const totalBytes = needAccounting ? dirBytes + singleBytes : null

  if (dryRun) {
    const preview = items.slice(0, 50).map((item) => `${item.kind === 'dir' ? 'dir ' : 'file'} ${item.src} -> ${item.dst}`)
    return {
      ok: true,
      dryRun: true,
      strategy: chosen,
      files: totalFiles,
      bytes: totalBytes,
      text: [
        `[dry run] ${op} ${fmt.fmtCount(totalFiles)} files (${fmt.fmtBytes(totalBytes)}) -> ${destAbs} via ${chosen}`,
        ...preview,
        items.length > 50 ? `… ${items.length - 50} more` : '',
      ].filter(Boolean).join('\n'),
      summary: `dry run: ${op} ${fmt.fmtCount(totalFiles)} files via ${chosen}`,
    }
  }

  mkdirSync(destAbs, { recursive: true })

  if (chosen === 'rename') {
    let moved = 0
    const failed = []
    for (const item of items) {
      try {
        if (!overwrite && existsSync(item.dst)) throw Object.assign(new Error('destination exists'), { code: 'EEXIST' })
        mkdirSync(dirname(item.dst), { recursive: true })
        await renameAsync(item.src, item.dst)
        moved += 1
      } catch (err) {
        failed.push({ src: item.src, error: err.code || err.message })
      }
    }
    const ms = performance.now() - started
    return {
      ok: failed.length === 0,
      strategy: 'rename',
      files: totalFiles,
      dirs: dirs.length,
      ms,
      failed,
      summary: `renamed ${fmt.fmtCount(items.length)} entries in ${fmt.fmtMs(ms)}`,
      text: [
        `moved ${fmt.fmtCount(moved)} entries (${dirs.length} dirs, ${singles.length} files) into ${destAbs} by rename in ${fmt.fmtMs(ms)} — no file data was copied, and the tree was not counted (bulk_scan reports that in one pass)`,
        failed.length > 0 ? `${failed.length} failed: ${failed.slice(0, 10).map((f) => `${f.src}: ${f.error}`).join('; ')}` : '',
      ].filter(Boolean).join('\n'),
    }
  }

  try {
    if (chosen === 'robocopy' || chosen === 'archive') {
      for (const entry of scannedDirs) {
        if (chosen === 'archive') {
          await archivePipe({ src: entry.item.src, dst: entry.item.dst, signal })
          if (op === 'move') rmSync(entry.item.src, { recursive: true, force: true })
        } else {
          await robocopy({
            src: entry.item.src,
            dst: entry.item.dst,
            move: op === 'move',
            sync: op === 'sync',
            threads,
            exclude: asList(exclude),
            signal,
          })
          // /MOV takes the files and leaves the directories; a move removes the
          // source tree, so what remains (now empty) goes too.
          if (op === 'move') rmSync(entry.item.src, { recursive: true, force: true })
        }
      }
      // Only standalone file sources remain: copy them through the pool.
      if (singles.length === 0) {
        const ms = performance.now() - started
        const verb = op === 'move' ? 'moved' : op === 'sync' ? 'synced' : 'copied'
        return {
          ok: true,
          strategy: chosen,
          files: totalFiles,
          dirs: dirs.length,
          bytes: totalBytes,
          ms,
          failed: [],
          summary: fmt.summaryLine({ files: totalFiles, bytes: totalBytes, ms }),
          text: transferText({
            verb,
            files: totalFiles,
            bytes: totalBytes,
            ms,
            strategy: chosen,
            destAbs,
            costOp: op === 'move' ? 'move' : 'copy',
            cold,
            scanner: scannerName(),
          }),
        }
      }
    }
  } catch (err) {
    if (err.code !== 'ENOENT' && err.code !== 'ETAR' && err.code !== 'ESPAWN') throw err
    // tar or robocopy missing: keep the same plan, fall back to the thread pool.
    chosen = 'threads'
  }

  // ---- threaded strategy: streamed, paced, bounded memory -----------------
  //
  // Pairs are produced batch by batch and consumed as they arrive, so memory is
  // a function of the channel depth and the window, never of the tree size. The
  // window is what keeps the device busy: it is measured against achieved
  // throughput instead of being a fixed thread count.
  const workerOp = op === 'move' ? 'move' : 'copy'
  const meter = createMeter()
  const failures = []
  const clone = op !== 'move' && cloneSupport(destAbs)
  let finalWindow = 0

  // A watching caller wants a percentage and an ETA, and both need a total. A
  // count-only pass costs one readdir walk with no per-file stat — 0.42 s for
  // 200,000 files — which is nothing against the minutes the copy will take.
  // It runs only when somebody is watching, and is skipped when the caller
  // already measured the tree to decide whether to promote this call.
  if (onProgress && !Number.isFinite(knownTotal) && (dirs.length > 0 || singles.length > 0)) {
    let counted = singles.length
    for (const item of dirs) {
      try {
        const stats = await scanStats({
          root: item.src,
          exclude: [...DEFAULT_EXCLUDES, ...asList(exclude)],
          withSize: false,
          topN: 0,
          pool,
          signal,
        })
        counted += stats.files
      } catch { /* a failed count only costs the percentage */ }
    }
    try {
      onProgress({ files: 0, bytes: 0, phase: 'counting', totalFiles: counted })
    } catch { /* ignore */ }
  }

  const source = (async function * pairBatches () {
    const direct = singles
      .map((item) => ({ src: item.src, dst: item.dst, size: item.size }))
      .filter((pair) => pair.src !== pair.dst)
    if (direct.length > 0) yield direct

    for (const item of dirs) {
      const stream = scanPairsStream({
        root: item.src,
        destRoot: item.dst,
        exclude: [...DEFAULT_EXCLUDES, ...asList(exclude)],
        pool,
        exec,
        signal,
        capacity: 8,
        batch: tuning.batchSize(),
      })
      for await (const batch of stream) {
        const usable = batch.filter((pair) => pair.src !== pair.dst)
        if (usable.length > 0) yield usable
      }
    }
  })()

  const paced = await runPaced({
    source,
    run: (batch) => exec(pool, workerOp, { pairs: batch, overwrite, preserveTimes: true, clone }),
    onValue: (value) => {
      meter.add(value)
      if (value.failed?.length > 0) failures.push(...value.failed)
    },
    onProgress: (snapshot) => {
      if (!onProgress) return
      const totals = meter.snapshot()
      try {
        onProgress({
          files: totals.files,
          bytes: totals.bytes,
          phase: 'copying',
          window: snapshot.window,
        })
      } catch { /* ignore */ }
    },
    // An explicit concurrency is taken at face value; otherwise the window
    // starts at the pool size and hill-climbs until throughput stops improving.
    window: concurrency > 0 ? concurrency : Math.max(2, pool.size),
    minWindow: 1,
    maxWindow: concurrency > 0 ? concurrency : Math.max(4, pool.size * 2),
    adaptive: !(concurrency > 0),
    signal,
  })
  finalWindow = paced?.window ?? 0

  const snapshot = meter.snapshot()
  const ms = performance.now() - started
  tuning.noteThroughput(snapshot.files, ms)
  const files = snapshot.files
  const bytes = snapshot.bytes
  const verb = op === 'move' ? 'moved' : op === 'sync' ? 'synced' : 'copied'
  const via = `window ${finalWindow}`

  return {
    ok: failures.length === 0,
    strategy: chosen,
    files,
    dirs: dirs.length,
    bytes,
    ms,
    window: finalWindow,
    failed: failures,
    summary: fmt.summaryLine({ files, bytes, ms }),
    text: transferText({
      verb,
      files,
      bytes,
      ms,
      strategy: chosen,
      via,
      destAbs,
      failed: failures,
      costOp: op === 'move' ? 'move' : 'copy',
      cold,
      scanner: scannerName(),
    }),
  }
}

/**
 * True when a move will be a same-volume rename, which is O(1) per entry.
 * Counting the tree to decide whether to background it would be the slowest
 * part of moving 160,000 files that are about to be renamed in milliseconds.
 */
export function willRename (op, sources, dest, cwd) {
  if (op !== 'move' || !dest) return false
  const list = asList(sources)
  if (list.length === 0) return false
  let destAbs
  try {
    destAbs = fromCwd(cwd, dest)
  } catch {
    return false
  }
  return list.every((source) => {
    try {
      return sameVolume(fromCwd(cwd, source), destAbs)
    } catch {
      return false
    }
  })
}

/* ------------------------------------------------------------------- delete */

export async function deleteFiles ({
  root,
  cwd,
  paths = [],
  include = [],
  exclude = [],
  maxDepth = -1,
  permanent = false,
  quarantine = false,
  dryRun = false,
  maxFiles = 20_000,
  force = false,
  concurrency = 0,
  policy = OPEN_POLICY,
  onProgress,
  planned,
  signal,
} = {}) {
  if (!root) throw new Error('root is required')
  const rootAbs = fromCwd(cwd, root)
  if (!existsSync(rootAbs) || !statSync(rootAbs).isDirectory()) {
    throw new Error(`root is not a directory: ${rootAbs}`)
  }
  proveWritable(policy, rootAbs)

  const pool = poolFor(concurrency)
  const explicit = asList(paths)
  let targets
  if (explicit.length > 0) {
    // An explicit path can walk through a junction, so every one of them has to
    // be proven against the resolved filesystem before anything is touched.
    targets = explicit.map((path) => proveDeletable(rootAbs, resolve(rootAbs, path)))
  } else {
    const scan = await scanTree({
      root: rootAbs,
      include: asList(include),
      exclude: asList(exclude),
      maxDepth,
      maxFiles,
      withSize: false,
      pool,
      signal,
    })
    // The scan never descends through a link, so these are contained by
    // construction; keep the cheap textual gate as a second line of defence.
    targets = scan.files.map((file) => resolve(rootAbs, file.path))
    for (const target of targets) assertDeletable(rootAbs, target)
  }

  if (targets.length > maxFiles && !force) {
    throw new Error(`${targets.length} targets exceed maxFiles=${maxFiles}; narrow the pattern or pass force:true`)
  }
  if (targets.length === 0) {
    return { ok: true, files: 0, bytes: 0, text: `nothing matched under ${rootAbs}`, summary: 'nothing matched' }
  }

  if (dryRun) {
    return {
      ok: true,
      dryRun: true,
      files: targets.length,
      permanent,
      text: [
        `[dry run] would ${permanent ? 'permanently delete' : 'move to trash'} ${fmt.fmtCount(targets.length)} paths under ${rootAbs}`,
        ...targets.slice(0, 50).map((target) => '  ' + relative(rootAbs, target)),
        targets.length > 50 ? `  … ${targets.length - 50} more` : '',
      ].filter(Boolean).join('\n'),
      summary: `dry run: ${fmt.fmtCount(targets.length)} paths`,
    }
  }

  const started = performance.now()

  // Rename-in-place: one syscall per entry frees the path immediately, and the
  // bytes are reclaimed later by whoever runs the cleanup. This is the cheapest
  // way to "delete" a huge tree, because nothing walks it.
  if (quarantine) {
    const stamp = new Date().toISOString().replace(/[:.]/g, '-')
    const pairs = targets.map((target) => {
      let dst = `${target}.old-${stamp}`
      for (let n = 1; existsSync(dst); n++) dst = `${target}.old-${stamp}-${n}`
      return { src: target, dst, size: 0 }
    })
    const slices = chunk(pairs, tuning.chunkSize(pairs.length))
    const results = await runBatch(pool, 'move', slices.map((slice) => ({ pairs: slice, overwrite: false, preserveTimes: true })))
    const merged = results.reduce((acc, value) => ({
      files: acc.files + value.files,
      failed: acc.failed.concat(value.failed),
    }), { files: 0, failed: [] })
    const ms = performance.now() - started
    return {
      ok: merged.failed.length === 0,
      mode: 'quarantine',
      files: merged.files,
      bytes: 0,
      ms,
      renamed: pairs.slice(0, 50).map((pair) => ({ from: pair.src, to: pair.dst })),
      failed: merged.failed,
      summary: `quarantined ${fmt.fmtCount(merged.files)} entries by rename in ${fmt.fmtMs(ms)}`,
      text: [
        `renamed ${fmt.plural(merged.files, 'entry', 'entries')} to *.old-${stamp} in ${fmt.fmtMs(ms)} — the paths are free immediately and no file was walked`,
        merged.failed.length === 0
          ? 'reclaim the space later, or undo with: rename each *.old-* back to its original name'
          : `${merged.failed.length} failed: ` + merged.failed.slice(0, 5).map((f) => `${f.path ?? f.src}: ${f.error}`).join('; '),
      ].join('\n'),
    }
  }

  if (permanent) {
    const plan = planned ?? await planDelete(targets, { pool, signal })
    try {
      onProgress?.({ files: 0, bytes: 0, phase: 'planning', totalFiles: plan.total })
    } catch { /* ignore */ }
    let removed = 0
    let removedBytes = 0
    const report = (value) => {
      removed += value.files
      removedBytes += value.bytes
      try { onProgress?.({ files: removed, bytes: removedBytes, phase: 'deleting' }) } catch { /* ignore */ }
    }
    const deleteAll = async (list) => {
      const slices = chunk(list, tuning.chunkSize(list.length))
      return slices.length === 0
        ? []
        : runBatch(
          pool,
          'delete',
          slices.map((slice) => ({ targets: slice.map((entry) => ({ path: entry.path, weight: entry.weight })) })),
          report,
          signal,
        )
    }
    // Subtrees first, then the now-empty parents: a parent deleted while its
    // children are still in flight would fail with ENOTEMPTY.
    const leafResults = await deleteAll(plan.leaves)
    const rootResults = await deleteAll(plan.roots)
    const results = leafResults.concat(rootResults)
    const merged = results.reduce((acc, value) => ({
      files: acc.files + value.files,
      bytes: acc.bytes + value.bytes,
      failed: acc.failed.concat(value.failed),
    }), { files: 0, bytes: 0, failed: [] })
    const ms = performance.now() - started
    tuning.noteThroughput(merged.files, ms)
    return {
      ok: merged.failed.length === 0,
      mode: 'permanent',
      files: merged.files,
      bytes: merged.bytes,
      ms,
      failed: merged.failed,
      dirs: plan.dirs,
      summary: fmt.summaryLine({ files: merged.files, bytes: merged.bytes, ms }),
      text: [
        `permanently deleted ${fmt.plural(merged.files, 'file')}${plan.dirs ? ` and ${fmt.plural(plan.dirs, 'directory', 'directories')}` : ''} in ${fmt.fmtMs(ms)}`,
        describeCost('delete', merged.files, merged.bytes, ms),
        merged.failed.length > 0
          ? `${merged.failed.length} failed:\n` + merged.failed.slice(0, 5).map((f) => `  ${f.path ?? f.src}: ${f.error}`).join('\n')
          : '',
      ].filter(Boolean).join('\n'),
    }
  }

  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const trashRoot = join(rootAbs, TRASH_DIR, stamp)
  const pairs = targets.map((target) => ({
    src: target,
    dst: join(trashRoot, relative(rootAbs, target)),
    size: 0,
  }))
  const slices = chunk(pairs, Math.max(64, Math.ceil(pairs.length / Math.max(1, pool.size * 2))))
  let movedToTrash = 0
  const results = await runBatch(pool, 'move', slices.map((slice) => ({ pairs: slice, overwrite: true, preserveTimes: true })), (value) => {
    movedToTrash += value.files
    try { onProgress?.({ files: movedToTrash, bytes: 0, phase: 'moving to trash' }) } catch { /* ignore */ }
  })
  const merged = results.reduce((acc, value) => ({
    files: acc.files + value.files,
    bytes: acc.bytes + value.bytes,
    failed: acc.failed.concat(value.failed),
  }), { files: 0, bytes: 0, failed: [] })

  mkdirSync(trashRoot, { recursive: true })
  writeFileSync(join(trashRoot, 'manifest.json'), JSON.stringify({
    at: new Date().toISOString(),
    root: rootAbs,
    entries: targets.map((target) => relative(rootAbs, target)),
  }, null, 2))

  const relatives = targets.map((target) => relative(rootAbs, target))
  const topLevel = [...new Set(relatives.map((rel) => rel.split(/[\\/]/)[0]))]
  const restoreSources = topLevel
    .map((name) => `"${join(trashRoot, name).replace(/\\/g, '/')}"`)
    .join(', ')
  const ms = performance.now() - started
  return {
    ok: merged.failed.length === 0,
    mode: 'trash',
    files: merged.files,
    bytes: merged.bytes,
    ms,
    trashPath: trashRoot,
    failed: merged.failed,
    summary: `moved ${fmt.fmtCount(merged.files)} paths to trash in ${fmt.fmtMs(ms)}`,
    text: [
      `moved ${fmt.plural(merged.files, 'path')} to ${trashRoot} in ${fmt.fmtMs(ms)}`,
      `restore: bulk_transfer op:"move" sources:[${restoreSources}] dest:"${rootAbs.replace(/\\/g, '/')}" (manifest.json lists every moved path)`,
      merged.failed.length > 0
        ? `${merged.failed.length} failed: ` + merged.failed.slice(0, 5).map((f) => `${f.path}: ${f.error}`).join('; ')
        : '',
    ].filter(Boolean).join('\n'),
  }
}

/* -------------------------------------------------------------------- stats */

/**
 * Aggregate a tree instead of listing it. At 200,000 files the answer an agent
 * actually needs is "how many, how big, what kind, what is biggest" — not
 * 200,000 paths. Memory stays bounded because only aggregates cross the worker
 * boundary.
 */
export async function scanStats ({
  root,
  cwd,
  include = [],
  exclude = [],
  maxDepth = -1,
  maxFiles = 5_000_000,
  topN = 20,
  withSize = true,
  concurrency = 0,
  signal,
} = {}) {
  if (!root) throw new Error('root is required')
  const rootAbs = fromCwd(cwd, root)
  const pool = poolFor(concurrency)
  const includes = asList(include)
  const excludes = [...DEFAULT_EXCLUDES, ...asList(exclude)]
  const started = performance.now()

  let files = 0
  let bytes = 0
  let truncated = false
  let level = [{ dir: '.', depth: 0 }]
  let round = 0
  const byExt = new Map()
  const largest = []
  const errors = []

  while (level.length > 0 && !truncated) {
    signal?.throwIfAborted?.()
    const chunkDepth = round === 0 ? 1 : 3
    round += 1
    const results = await Promise.all(level.map((job) => exec(pool, 'stats', {
      root: rootAbs,
      dir: job.dir,
      depth: job.depth,
      include: includes,
      exclude: excludes,
      maxDepth,
      maxFiles: Math.max(1, maxFiles - files),
      topN,
      withSize,
      chunkDepth,
    })))
    const frontier = []
    for (const result of results) {
      files += result.files
      bytes += result.bytes
      if (result.truncated) truncated = true
      for (const [ext, entry] of result.byExt) {
        const current = byExt.get(ext) ?? { count: 0, bytes: 0 }
        current.count += entry.count
        current.bytes += entry.bytes
        byExt.set(ext, current)
      }
      for (const file of result.largest) largest.push(file)
      for (const error of result.errors ?? []) if (errors.length < 50) errors.push(error)
      for (const next of result.frontier ?? []) frontier.push(next)
    }
    level = frontier
  }

  largest.sort((a, b) => b.size - a.size)
  largest.length = Math.min(largest.length, Math.max(0, topN))
  const ms = performance.now() - started
  const extensions = [...byExt.entries()]
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 8)

  const lines = [
    `${fmt.fmtCount(files)} files · ${fmt.fmtBytes(bytes)} (${bytes.toLocaleString('en-US')} bytes) · ${fmt.fmtMs(ms)} under ${rootAbs}${truncated ? ' (stopped at maxFiles)' : ''}`,
  ]
  if (extensions.length > 1) {
    lines.push('by extension: ' + extensions
      .map(([ext, entry]) => `.${ext} ${fmt.fmtCount(entry.count)} (${fmt.fmtBytes(entry.bytes)})`)
      .join(' · '))
  }
  if (largest.length > 0) {
    lines.push('largest:')
    for (const file of largest.slice(0, 5)) lines.push(`  ${fmt.fmtBytes(file.size).padStart(9)}  ${file.path}`)
  }
  if (errors.length > 0) lines.push(`${errors.length} unreadable directories`)

  return {
    ok: true,
    root: rootAbs,
    files,
    bytes,
    ms,
    truncated,
    byExtension: Object.fromEntries(extensions),
    largest,
    errors,
    summary: `${fmt.fmtCount(files)} files · ${fmt.fmtBytes(bytes)}`,
    text: lines.join('\n'),
  }
}

/* ------------------------------------------------------------- engine info */

export function engineInfo () {
  const pool = getPool(0)
  return {
    poolSize: pool.size || defaultPoolSize(),
    workers: pool.workers.length,
    broken: pool.broken ? String(pool.broken.message || pool.broken) : null,
    jobs: pool.stats.jobs,
    spawns: pool.stats.spawns,
    retries: pool.stats.retries,
  }
}








