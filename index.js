/**
 * Bulk file I/O for DeepSeek Harness.
 *
 * Five tools replace the pattern that makes an agent slow on many small files:
 * one tool call, one tool result and one process spawn per file. Each tool takes
 * a whole batch, does the syscalls on a warm worker-thread pool, and answers once
 * with counts, elapsed time and the primitives it issued. bulk_job watches the
 * work that was moved off the call.
 *
 *   bulk_write     write thousands of files in one call
 *   bulk_read      read an explicit list or a globbed tree, bounded output
 *   bulk_scan      size a tree without listing it
 *   bulk_transfer  copy / move / sync through rename, robocopy or tar|zstd
 *   bulk_delete    delete by pattern, into a reversible trash by default
 *   bulk_job       progress and result of a background operation
 *
 * No client half, no service of its own, no external dependency: Node's own fs,
 * worker_threads, zlib (zstd) and the native robocopy/tar that Windows ships.
 */

import * as engine from './lib/engine.js'
import { closePool } from './lib/pool.js'
import { resolvePolicy } from './lib/policy.js'
import * as tuning from './lib/tuning.js'
import {
  cancelJob,
  describeJob,
  failJob,
  finishJob,
  getJob,
  listJobs,
  progressDir,
  startJob,
  updateJob,
  view,
  waitForJob,
} from './lib/jobs.js'

export const inject = ['tools', 'systemPrompt']

const PROMPT = `## Bulk small-file I/O (read/write optimizer)
Trigger: the work touches more than ~5 files, or any directory you did not just create. One bulk call replaces N tool calls and N shell spawns.
- writing or replacing many files -> bulk_write (creates parents, reports per-file failures)
- reading several files -> bulk_read with include globs, not one read per file
- copying, moving or syncing trees -> bulk_transfer (a same-volume move is one rename, instant)
- deleting anything you did not just write -> bulk_scan for the size first, then bulk_delete (trash by default)
- a shell loop over files (for / foreach / xargs / robocopy) -> this plugin replaces it; do not spawn one.
Never rm -rf or Remove-Item -Recurse a path whose size you have not measured. An irreversible delete is not worth saving one round trip.
A call that would exceed ~15 s comes back as a job id. Do not restart it: bulk_job id:"..." waitSeconds:25 returns progress and, once finished, the full result. settle:true waits quietly instead of polling.
Writes, moves and deletes enforce the same file policy as write/edit, junctions included; reads follow the filesystem. A refused call is refused, do not route around it.
Keep read/write/edit for one or two files and glob/grep for discovery.`

const output = {
  schema: { type: 'object', additionalProperties: true },
  render: (_args, value) => [{ type: 'text', text: String(value?.text ?? value?.summary ?? '') }],
}

const CONCURRENCY = {
  type: 'number',
  description: 'Worker threads for this call. Default is half the core count (2-16, derived from the machine); raising it past that rarely helps because the filesystem filter saturates first.',
}

function tool (ctx, definition) {
  ctx.effect(() => ctx.tools.register({ ...definition, output }))
}

/**
 * The directory a Session calls home.
 *
 * The tools run in one process that serves many Sessions, so `process.cwd()`
 * answers for the server, not for the caller. A relative "src" must mean the
 * same thing it means to the host's own read/write tools.
 */
function sessionCwd (ctx, exec) {
  try {
    const session = exec?.agent?.session
    if (!session) return process.cwd()
    return ctx.get?.('workingDirectory')?.get?.(session) ?? process.cwd()
  } catch {
    return process.cwd()
  }
}

/** The reply for work that was moved off the call: an id and how to watch it. */
function backgroundResult (job, hint) {
  return {
    jobId: job.id,
    state: job.state,
    summary: `started ${job.label}`,
    text: [
      `started in the background: ${job.label}`,
      hint,
      `watch it with bulk_job id:"${job.id}" waitSeconds:25 — each call returns progress and, once finished, the full result.`,
      `a human can read the same counters from ${progressDir()}\\${job.id}.json`,
    ].filter(Boolean).join('\n'),
  }
}

/**
 * Run a tool operation without ever holding the model for more than `AUTO_MS`.
 *
 * A foreground tool call keeps the whole turn open — the agent loop waits on it,
 * and nothing else can happen until it returns. A seven-minute copy therefore
 * costs seven minutes of a stalled conversation even though the disk is the only
 * thing that is busy.
 *
 * So the work always starts as a job, and the call races it against a short
 * timer. Finish first and the caller gets the ordinary result with no job id
 * anywhere. Miss the deadline and the same job is handed back as a handle while
 * the work continues — the model is free immediately, and `bulk_job` (or the
 * progress file) reports the rest.
 *
 * `background: true` skips the race and promotes immediately.
 */
const DEFAULT_AUTO_BACKGROUND_MS = 15_000

/** Overridable for tests and for a user who wants a different patience budget. */
function autoBackgroundMs () {
  const override = Number(process.env.DSH_BULK_AUTO_BACKGROUND_MS)
  return Number.isFinite(override) && override > 0 ? override : DEFAULT_AUTO_BACKGROUND_MS
}

async function runTool ({ kind, label, forceBackground = false, exec, estimate, run }) {
  let total = null
  if (estimate) {
    try {
      total = await estimate()
    } catch {
      total = null
    }
  }

  const known = Number.isFinite(total) && total >= 0
  const budget = autoBackgroundMs()
  // The decision is a projection, not a count: will this finish inside the
  // patience budget on a machine like this one? The first batch of a process
  // uses a pessimistic seed, every later one uses what was actually measured.
  const promoteNow = forceBackground || (known && tuning.shouldPromote(total, budget))

  const job = startJob(kind, label)
  if (known) updateJob(job, { totalFiles: total })
  const signal = job.controller.signal

  // Until promotion the call owns the work, so the tool-call abort still stops
  // it. After promotion the call is gone and the job owns cancellation itself.
  let promoted = promoteNow
  const forwardAbort = () => { if (!promoted) job.controller.abort() }
  exec?.signal?.addEventListener?.('abort', forwardAbort, { once: true })

  const work = Promise.resolve().then(() => run((patch) => updateJob(job, patch), signal, known ? total : undefined))

  if (promoteNow) {
    work.then((result) => finishJob(job, result), (error) => failJob(job, error))
    const hint = forceBackground || !known
      ? undefined
      : `Counted ${total.toLocaleString('en-US')} entries first, so it was moved off this call instead of waiting for the budget.`
    return backgroundResult(job, hint)
  }

  let timer
  const deadline = new Promise((resolve) => {
    timer = setTimeout(() => { promoted = true; resolve(null) }, budget)
    timer.unref?.()
  })

  const settled = waitSettled(work)
  const first = await Promise.race([settled, deadline])
  clearTimeout(timer)

  if (first === null) {
    work.then((result) => finishJob(job, result), (error) => failJob(job, error))
    return backgroundResult(job, `Took longer than ${Math.round(budget / 1000)} s, so it was moved off this call.`)
  }

  exec?.signal?.removeEventListener?.('abort', forwardAbort)
  if (first instanceof Error) {
    failJob(job, first)
    throw first
  }
  finishJob(job, first)
  return first
}

/** A promise that resolves with the value or the error, never rejects. */
function waitSettled (promise) {
  return promise.then((value) => value, (error) => error)
}

export function apply (ctx) {
  ctx.effect(() => ctx.systemPrompt.section({ name: 'bulk-io:usage', order: 8500, text: PROMPT }))

  tool(ctx, {
    name: 'bulk_write',
    description:
      'Use when a task creates, generates or replaces more than a handful of files: one round trip instead of one per file, parent directories created, writes spread over worker threads, and per-file failures reported instead of stopping at the first. Paths with base64:true are written as decoded bytes.',
    parameters: {
      type: 'object',
      properties: {
        root: { type: 'string', description: 'Base directory; every path is resolved under it. Created when missing.' },
        files: {
          type: 'array',
          description: 'Files to write. Later entries win when two entries name the same path.',
          items: {
            type: 'object',
            properties: {
              path: { type: 'string', description: 'Path relative to root.' },
              content: { type: 'string', description: 'File content (UTF-8 text, or base64 when base64 is true).' },
              base64: { type: 'boolean', description: 'Decode content as base64 instead of UTF-8 text.' },
              append: { type: 'boolean', description: 'Append instead of replacing the file.' },
            },
            required: ['path'],
            additionalProperties: false,
          },
        },
        atomic: { type: 'boolean', description: 'Write to a temporary name and rename, so readers never see a half-written file.' },
        dryRun: { type: 'boolean', description: 'Report the plan and total size without writing.' },
        background: { type: 'boolean', description: 'Return immediately with a job id; watch it with bulk_job.' },
        concurrency: CONCURRENCY,
      },
      required: ['root', 'files'],
      additionalProperties: false,
    },
    timeoutMs: 600_000,
    isConcurrencySafe: () => false,
    execute: async (args, exec) => runTool({
      kind: 'write',
      label: `write ${args.files?.length ?? 0} files under ${args.root}`,
      forceBackground: args.background === true,
      exec,
      estimate: () => (Array.isArray(args.files) ? args.files.length : null),
      run: (onProgress, signal) => engine.writeFiles({
        root: args.root,
        cwd: sessionCwd(ctx, exec),
        files: args.files,
        atomic: args.atomic === true,
        dryRun: args.dryRun === true,
        concurrency: args.concurrency,
        policy: resolvePolicy(ctx, exec?.agent),
        onProgress,
        signal,
      }),
    }),
  })

  tool(ctx, {
    name: 'bulk_read',
    description:
      'Use when a task needs the contents of several files or a whole tree: one call with globs and a byte budget instead of one read per file. Pass paths for an explicit list, or include/exclude globs. Every file prefix says whether it was truncated or omitted, so you always know what you did not see. Binary files are skipped unless binary is base64 or hex.',
    parameters: {
      type: 'object',
      properties: {
        root: { type: 'string', description: 'Base directory for paths, globs and relative output names.' },
        paths: { type: 'array', items: { type: 'string' }, description: 'Explicit files relative to root. When given, include is ignored.' },
        include: { type: 'array', items: { type: 'string' }, description: 'Glob(s) such as "src/**/*.ts"; a pattern without a slash matches at any depth.' },
        exclude: { type: 'array', items: { type: 'string' }, description: 'Glob(s) to skip; a leading ! on an include also excludes.' },
        maxDepth: { type: 'number', description: 'Maximum directory depth; -1 (default) means no limit.' },
        maxFiles: { type: 'number', description: 'Maximum files to read (default 200).' },
        maxBytesPerFile: { type: 'number', description: 'Bytes read per file (default 64 KiB).' },
        maxTotalBytes: { type: 'number', description: 'Total output budget (default 256 KiB); files past it are reported as omitted.' },
        binary: { type: 'string', enum: ['skip', 'base64', 'hex'], description: 'What to do with files that contain NUL bytes; default skip.' },
        headLines: { type: 'number', description: 'Keep only the first N lines of each file.' },
        tailLines: { type: 'number', description: 'Keep only the last N lines of each file.' },
        grep: { type: 'string', description: 'Regular expression; only matching lines are returned, and files with no match are dropped.' },
        concurrency: CONCURRENCY,
      },
      required: ['root'],
      additionalProperties: false,
    },
    timeoutMs: 600_000,
    execute: async (args, exec) => engine.readFiles({
      root: args.root,
      cwd: sessionCwd(ctx, exec),
      paths: args.paths,
      include: args.include,
      exclude: args.exclude,
      maxDepth: args.maxDepth,
      maxFiles: args.maxFiles,
      maxBytesPerFile: args.maxBytesPerFile,
      maxTotalBytes: args.maxTotalBytes,
      binary: args.binary,
      headLines: args.headLines,
      tailLines: args.tailLines,
      grep: args.grep,
      concurrency: args.concurrency,
      signal: exec?.signal,
    }),
  })

  tool(ctx, {
    name: 'bulk_transfer',
    description:
      'Use when copying, moving or syncing a tree rather than a file or two. A move inside one volume is a rename (instant, no data copied). Other copies pick the cheapest available path: parallel file copies, Windows robocopy /MT, or one tar|zstd stream for a high-latency destination. Directory sources keep their name under dest; glob sources keep their relative path.',
    parameters: {
      type: 'object',
      properties: {
        op: { type: 'string', enum: ['copy', 'move', 'sync'], description: 'sync copies only files newer than dest and never deletes.' },
        sources: { type: 'array', items: { type: 'string' }, description: 'Files, directories or globs (absolute, or relative to the working directory).' },
        dest: { type: 'string', description: 'Destination directory, created when missing.' },
        strategy: {
          type: 'string',
          enum: ['auto', 'threads', 'robocopy', 'archive'],
          description: 'auto (default) uses rename for a same-volume move, robocopy for a UNC destination or sync, and worker threads otherwise. archive streams tar|zstd, which wins on high-latency links rather than on a LAN.',
        },
        threads: { type: 'number', description: 'robocopy /MT thread count (default 32).' },
        overwrite: { type: 'boolean', description: 'Overwrite existing destinations; default true.' },
        exclude: { type: 'array', items: { type: 'string' }, description: 'Glob(s) to skip while expanding directory sources.' },
        dryRun: { type: 'boolean', description: 'Report the resolved plan, counts and bytes without copying.' },
        background: { type: 'boolean', description: 'Return immediately with a job id instead of blocking. Use it when the tree is big enough that the wait would be minutes, then watch it with bulk_job.' },
        concurrency: CONCURRENCY,
      },
      required: ['op', 'sources', 'dest'],
      additionalProperties: false,
    },
    timeoutMs: 1_800_000,
    isConcurrencySafe: () => false,
    execute: async (args, exec) => runTool({
      kind: 'transfer',
      label: `${args.op} ${args.sources.length} source(s) -> ${args.dest}`,
      forceBackground: args.background === true,
      exec,
      estimate: () => {
        const cwd = sessionCwd(ctx, exec)
        return args.dryRun || engine.willRename(args.op, args.sources, args.dest, cwd)
          ? null
          : engine.estimateEntries(args.sources, { filesOnly: true, cwd })
      },
      run: (onProgress, signal, total) => engine.transferFiles({
        op: args.op,
        sources: args.sources,
        dest: args.dest,
        cwd: sessionCwd(ctx, exec),
        strategy: args.strategy,
        threads: args.threads,
        overwrite: args.overwrite,
        dryRun: args.dryRun === true,
        exclude: args.exclude,
        concurrency: args.concurrency,
        policy: resolvePolicy(ctx, exec?.agent),
        onProgress,
        knownTotal: total,
        signal,
      }),
    }),
  })

  tool(ctx, {
    name: 'bulk_delete',
    description:
      'Use when deleting files or whole trees; measure first with bulk_scan. Reversible by default: without permanent, a directory target costs one rename into <root>/.dsh-trash/<timestamp>/ with a manifest.json, so a wrong glob is undone by moving it back. permanent:true splits the tree into subtrees, deletes them in parallel and reports file-accurate progress; quarantine:true renames entries to <name>.old-<stamp> in place instead (instant, nothing walked). A target is refused when it cannot be proven inside root on the resolved filesystem — which includes a path that reaches outside through a junction — and when it is the root itself or a drive root.',
    parameters: {
      type: 'object',
      properties: {
        root: { type: 'string', description: 'Base directory; nothing outside it can be deleted.' },
        paths: { type: 'array', items: { type: 'string' }, description: 'Explicit files or directories relative to root. When given, include is ignored. A directory here is one rename in trash/quarantine mode and a parallel subtree delete with permanent:true.' },
        include: { type: 'array', items: { type: 'string' }, description: 'Glob(s) selecting the files to delete.' },
        exclude: { type: 'array', items: { type: 'string' }, description: 'Glob(s) to keep.' },
        maxDepth: { type: 'number', description: 'Maximum directory depth; -1 (default) means no limit.' },
        permanent: { type: 'boolean', description: 'Really unlink instead of moving to the trash.' },
        quarantine: { type: 'boolean', description: 'Rename each target to <name>.old-<stamp> in place: frees the path in one syscall and leaves reclaiming the bytes for later.' },
        dryRun: { type: 'boolean', description: 'List what would go, without touching it.' },
        maxFiles: { type: 'number', description: 'Refuse above this many targets (default 20000) unless force is true.' },
        force: { type: 'boolean', description: 'Allow more targets than maxFiles.' },
        background: { type: 'boolean', description: 'Return immediately with a job id; watch it with bulk_job.' },
        concurrency: CONCURRENCY,
      },
      required: ['root'],
      additionalProperties: false,
    },
    timeoutMs: 600_000,
    isConcurrencySafe: () => false,
    execute: async (args, exec) => {
      // The delete planner is the only thing that can say how big this is, and
      // computing it twice (once to decide, once to run) would double the cost
      // of a job whose prefix is already the slowest part.
      let planned = null
      return runTool({
        kind: 'delete',
        label: `delete under ${args.root}`,
        forceBackground: args.background === true,
        exec,
        estimate: async () => {
          if (args.dryRun || !Array.isArray(args.paths) || args.paths.length === 0) return null
          try {
            planned = await engine.planDeletePaths({ root: args.root, paths: args.paths, cwd: sessionCwd(ctx, exec) })
            return planned?.total ?? null
          } catch {
            planned = null
            return null
          }
        },
        run: (onProgress, signal) => engine.deleteFiles({
          root: args.root,
          cwd: sessionCwd(ctx, exec),
          paths: args.paths,
          include: args.include,
          exclude: args.exclude,
          maxDepth: args.maxDepth,
          permanent: args.permanent === true,
          quarantine: args.quarantine === true,
          dryRun: args.dryRun === true,
          maxFiles: args.maxFiles,
          force: args.force === true,
          concurrency: args.concurrency,
          policy: resolvePolicy(ctx, exec?.agent),
          onProgress,
          planned,
          signal,
        }),
      })
    },
  })

  tool(ctx, {
    name: 'bulk_scan',
    description:
      'Use before moving or deleting a tree, or when a directory has far too many files to list. Returns file count, total bytes, an extension histogram and the largest members without listing anything. One pass over the tree, bounded aggregates in memory, so it answers for hundreds of thousands of files where a listing would not fit.',
    parameters: {
      type: 'object',
      properties: {
        root: { type: 'string', description: 'Directory to summarise.' },
        include: { type: 'array', items: { type: 'string' }, description: 'Glob(s) to include; default all files.' },
        exclude: { type: 'array', items: { type: 'string' }, description: 'Glob(s) to skip.' },
        maxDepth: { type: 'number', description: 'Maximum directory depth; -1 (default) means no limit.' },
        maxFiles: { type: 'number', description: 'Stop counting after this many files (default 5,000,000).' },
        topN: { type: 'number', description: 'How many largest files to report (default 20, max 100).' },
        withSize: { type: 'boolean', description: 'Stat each file for sizes (default true). False makes it a pure count, which is much faster.' },
        concurrency: CONCURRENCY,
      },
      required: ['root'],
      additionalProperties: false,
    },
    timeoutMs: 600_000,
    execute: async (args, exec) => engine.scanStats({
      root: args.root,
      cwd: sessionCwd(ctx, exec),
      include: args.include,
      exclude: args.exclude,
      maxDepth: args.maxDepth,
      maxFiles: args.maxFiles,
      topN: args.topN,
      withSize: args.withSize !== false,
      concurrency: args.concurrency,
      signal: exec?.signal,
    }),
  })

  tool(ctx, {
    name: 'bulk_job',
    description:
      'Watch a background bulk operation. With no id it lists recent jobs. With an id it returns one progress snapshot, waiting up to waitSeconds for something to change, so calling it in a loop turns a long copy into visible movement instead of a silent wait; once the job finished its full result is included. cancel:true asks a running job to stop.',
    parameters: {
      type: 'object',
      properties: {
        id: { type: 'string', description: 'Job id returned by a bulk tool started with background:true.' },
        waitSeconds: { type: 'number', description: 'How long to wait before answering (default 25, max 55).' },
        settle: { type: 'boolean', description: 'Wait for the job to finish instead of returning at the next progress tick. Use it when you want to sit still; the result arrives with the final state.' },
        cancel: { type: 'boolean', description: 'Ask the running job to stop.' },
      },
      additionalProperties: false,
    },
    timeoutMs: 120_000,
    execute: async (args) => {
      if (!args.id) {
        const jobs = listJobs(10)
        return {
          jobs,
          text: jobs.length === 0 ? 'no bulk jobs in this process' : jobs.map(describeJob).join('\n'),
          summary: jobs.length === 0 ? 'no jobs' : `${jobs.length} job(s)`,
        }
      }
      if (args.cancel === true) {
        const requested = cancelJob(args.id)
        const job = getJob(args.id)
        return {
          ...(job ? view(job) : { id: args.id, state: 'unknown' }),
          text: requested ? `cancel requested for ${args.id}; it stops at the next checkpoint` : `${args.id} is not running`,
          summary: requested ? 'cancelling' : 'not running',
        }
      }
      const seconds = Math.min(55, Math.max(0, Number.isFinite(args.waitSeconds) ? args.waitSeconds : 25))
      const snapshot = await waitForJob(args.id, seconds * 1000, { settleOnly: args.settle === true })
      if (!snapshot) return { id: args.id, state: 'unknown', text: `unknown job id: ${args.id}`, summary: 'unknown job' }
      const job = getJob(args.id)
      const finished = snapshot.state !== 'running'
      const result = finished ? (job?.result ?? null) : null
      const text = [
        describeJob(snapshot),
        finished && result?.text ? result.text : '',
      ].filter(Boolean).join('\n\n')
      return { ...snapshot, result, text, summary: describeJob(snapshot) }
    },
  })

  // Warm the pool while the user is still typing, so the first batch is fast
  // without delaying plugin activation. Disabling the plugin takes the threads
  // down with it.
  const timer = setTimeout(() => {
    try {
      engine.poolFor(0).warm()
    } catch { /* a pool that cannot start falls back in-thread */ }
  }, 2500)
  ctx.effect(() => () => {
    clearTimeout(timer)
    closePool()
  })
}


export { runTool }

