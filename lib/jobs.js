/**
 * Long-running work that can be watched.
 *
 * A bulk call that takes seven minutes is indistinguishable from one that has
 * hung, and the model has no way to say "still going" from inside a single tool
 * result. So a large operation can run as a job: the call returns immediately
 * with an id, and `bulk_job` reports files done, bytes, rate and ETA while it
 * runs — which is something the agent can turn into a sentence, and something a
 * human can watch in a file.
 *
 * The job object is deliberately plain data plus one AbortController. Nothing
 * here touches the session log: the durable record of what happened is still the
 * final tool result.
 */

import { mkdirSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

const JOBS = new Map()
const FILE_THROTTLE_MS = 750
let sequence = 0

export function progressDir () {
  return join(tmpdir(), 'dsh-bulk-io')
}

function progressFile (job) {
  return join(progressDir(), `${job.id}.json`)
}

/** Keep the on-disk snapshot cheap: at most one write per 750 ms per job. */
function flush (job) {
  const now = performance.now()
  if (now - job._lastFlush < FILE_THROTTLE_MS && job.state === 'running') return
  job._lastFlush = now
  try {
    mkdirSync(progressDir(), { recursive: true })
    writeFileSync(progressFile(job), JSON.stringify(view(job), null, 2))
  } catch { /* a progress file is a convenience, never a failure */ }
}

/** Keep the temp directory from growing one small file per job, forever. */
let swept = false
function sweepProgressFiles () {
  if (swept) return
  swept = true
  try {
    const dir = progressDir()
    const cutoff = Date.now() - 24 * 60 * 60 * 1000
    for (const name of readdirSync(dir)) {
      if (!name.endsWith('.json')) continue
      const path = join(dir, name)
      try {
        if (statSync(path).mtimeMs < cutoff) rmSync(path, { force: true })
      } catch { /* a file someone else is reading is not our problem */ }
    }
  } catch { /* no directory yet */ }
}

/**
 * Finished jobs are kept for their result, but not forever: a long session can
 * start hundreds, and each one holds its text. Sixty-four is far more than any
 * caller can still be polling, and running jobs are never dropped.
 */
const MAX_JOBS = 64

function pruneJobs () {
  if (JOBS.size <= MAX_JOBS) return
  const finished = [...JOBS.values()]
    .filter((job) => job.state !== 'running')
    .sort((a, b) => (a.finishedAt ?? a.startedAt) - (b.finishedAt ?? b.startedAt))
  for (const job of finished) {
    if (JOBS.size <= MAX_JOBS) break
    JOBS.delete(job.id)
  }
}

export function startJob (kind, label, detail = {}) {
  sweepProgressFiles()
  const id = `${kind}-${Date.now().toString(36)}-${++sequence}`
  const job = {
    id,
    kind,
    label,
    state: 'running',
    phase: 'starting',
    files: 0,
    bytes: 0,
    totalFiles: null,
    totalBytes: null,
    current: '',
    startedAt: performance.now(),
    finishedAt: null,
    error: null,
    result: null,
    controller: new AbortController(),
    detail,
    _lastFlush: 0,
  }
  JOBS.set(id, job)
  pruneJobs()
  flush(job)
  return job
}

export function updateJob (job, patch = {}) {
  if (!job || job.state !== 'running') return job
  if (patch.files !== undefined) job.files = patch.files
  if (patch.bytes !== undefined) job.bytes = patch.bytes
  if (patch.phase !== undefined) job.phase = patch.phase
  if (patch.current !== undefined) job.current = patch.current
  if (patch.totalFiles !== undefined) job.totalFiles = patch.totalFiles
  if (patch.totalBytes !== undefined) job.totalBytes = patch.totalBytes
  flush(job)
  return job
}

export function finishJob (job, result) {
  if (!job) return
  // A cancelled job that happens to return a partial result is still cancelled:
  // reporting it as done would tell the caller the work completed.
  job.state = job.controller.signal.aborted ? 'cancelled' : 'done'
  job.phase = job.state
  job.result = result
  job.finishedAt = performance.now()
  flush(job)
}

export function failJob (job, error) {
  if (!job) return
  job.state = job.controller.signal.aborted ? 'cancelled' : 'failed'
  job.phase = job.state
  job.error = String(error?.message ?? error)
  job.finishedAt = performance.now()
  flush(job)
}

export function cancelJob (id) {
  const job = JOBS.get(id)
  if (!job || job.state !== 'running') return false
  job.controller.abort()
  return true
}

export function getJob (id) {
  return JOBS.get(id)
}

export function view (job) {
  const now = job.finishedAt ?? performance.now()
  const ms = Math.max(0, now - job.startedAt)
  const perSecond = ms > 0 ? job.files / (ms / 1000) : 0
  const etaMs = job.totalFiles && perSecond > 0
    ? Math.max(0, Math.round(((job.totalFiles - job.files) / perSecond) * 1000))
    : null
  return {
    id: job.id,
    kind: job.kind,
    label: job.label,
    state: job.state,
    phase: job.phase,
    files: job.files,
    bytes: job.bytes,
    totalFiles: job.totalFiles,
    totalBytes: job.totalBytes,
    elapsedMs: Math.round(ms),
    filesPerSecond: Math.round(perSecond),
    etaMs,
    current: job.current,
    error: job.error,
    progressFile: progressFile(job),
  }
}

export function listJobs (limit = 10) {
  return [...JOBS.values()]
    .sort((a, b) => b.startedAt - a.startedAt)
    .slice(0, limit)
    .map(view)
}

/**
 * Block until the job settles, advances, or `waitMs` passes.
 *
 * Returning on progress — not only on settlement — is the whole point: a poll
 * that waits the full timeout while files are visibly moving is exactly the
 * "model sits there doing nothing" problem this feature exists to remove.
 */
export function waitForJob (id, waitMs = 25_000, { minWaitMs = 3000, settleOnly = false } = {}) {
  const job = JOBS.get(id)
  if (!job) return Promise.resolve(undefined)
  if (job.state !== 'running' || waitMs <= 0) return Promise.resolve(view(job))
  const baselineFiles = job.files
  const baselinePhase = job.phase
  return new Promise((resolve) => {
    const started = performance.now()
    const timer = setInterval(() => {
      const current = JOBS.get(id)
      if (!current) {
        clearInterval(timer)
        resolve({ id, state: 'unknown' })
        return
      }
      const elapsed = performance.now() - started
      const advanced = current.files !== baselineFiles || current.phase !== baselinePhase
      if (current.state !== 'running' || elapsed >= waitMs || (advanced && !settleOnly && elapsed >= minWaitMs)) {
        clearInterval(timer)
        resolve(view(current))
      }
    }, 200)
  })
}

/** Run work in the background and return the job immediately. */
export function runAsJob (kind, label, work, detail = {}) {
  const job = startJob(kind, label, detail)
  const tick = (snapshot) => updateJob(job, snapshot)
  Promise.resolve()
    .then(() => work(tick, job.controller.signal))
    .then((result) => finishJob(job, result))
    .catch((error) => failJob(job, error))
  return job
}

/* --------------------------------------------------------------- formatting */

function fmtBytes (n) {
  if (!Number.isFinite(n) || n < 0) return '?'
  if (n < 1024) return n + ' B'
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KiB'
  if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' MiB'
  return (n / 1024 / 1024 / 1024).toFixed(2) + ' GiB'
}

function fmtDuration (ms) {
  if (!Number.isFinite(ms)) return '?'
  if (ms < 1000) return Math.round(ms) + ' ms'
  const seconds = Math.round(ms / 100) / 10
  if (seconds < 60) return seconds + ' s'
  const minutes = Math.floor(seconds / 60)
  return `${minutes} min ${Math.round(seconds - minutes * 60)} s`
}

/** One line an agent can repeat, and one a human can read from a file. */
export function describeJob (job) {
  const line = [
    `${job.files.toLocaleString('en-US')} files`,
    fmtBytes(job.bytes),
    fmtDuration(job.elapsedMs),
  ]
  if (job.files >= 50 && job.elapsedMs > 0) {
    const per = (job.elapsedMs / job.files) * 1000
    line.push(per < 1000 ? `${Math.round(per)} ms per 1,000 files` : `${(per / 1000).toFixed(1)} s per 1,000 files`)
  }
  if (job.totalFiles) line.push(`${Math.round((job.files / job.totalFiles) * 100)}% of ${job.totalFiles.toLocaleString('en-US')}`)
  if (job.etaMs !== null) line.push(`eta ${fmtDuration(job.etaMs)}`)
  else if (job.filesPerSecond > 0) line.push(`eta unknown (${job.phase})`)

  const head = {
    running: `[${job.state}] ${job.label}`,
    done: `[done] ${job.label}`,
    failed: `[failed] ${job.label}`,
    cancelled: `[cancelled] ${job.label}`,
  }[job.state] ?? `[${job.state}] ${job.label}`
  return `${head} · ${line.join(' · ')}`
}

