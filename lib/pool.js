/**
 * A fixed-size pool of warm worker threads.
 *
 * The pool is the "optimised startup" part of the design: the expensive thing
 * about a `pwsh -Command ...` per batch is process creation (200-900 ms), not
 * the I/O. A pool created once per Harness process and reused for every call
 * makes a 5,000-file write cost one message hop instead of one process spawn.
 *
 * Workers stay alive between calls. If one dies it is replaced and the job it
 * held is retried once. If threads cannot be created at all (a hardened Electron
 * build, an exotic sandbox), the pool reports broken and the engine runs the
 * same operation in-thread instead of failing the tool.
 */

import { Worker } from 'node:worker_threads'
import { poolSize as tuningPoolSize } from './tuning.js'

const WORKER_URL = new URL('./worker.js', import.meta.url)

export function defaultPoolSize () {
  return tuningPoolSize()
}

export class WorkerPool {
  constructor ({ size = 0, name = 'bulk-io' } = {}) {
    this.name = name
    this.size = Math.max(1, Math.min(64, size || defaultPoolSize()))
    this.workers = []
    this.queue = []
    this.pending = new Map()
    this.nextId = 1
    this.broken = null
    this.closed = false
    this.failures = 0
    this.stats = { jobs: 0, retries: 0, spawns: 0 }
  }

  get ready () {
    return this.workers.length > 0 && this.broken === null
  }

  /** True before this pool has served anything: the first batch pays spawn cost. */
  isCold () {
    return this.stats.jobs === 0
  }

  /**
   * Add threads up to `size`. The pool is shared, so this is additive: a call
   * that asks for more concurrency raises the ceiling for everyone instead of
   * racing to create a second pool with a different size.
   */
  grow (size) {
    const want = Math.max(1, Math.min(64, Math.floor(size) || 0))
    if (want <= this.size && this.workers.length >= this.size) return this.size
    this.size = Math.max(this.size, want)
    if (!this.broken && !this.closed) this.#ensure(this.size)
    return this.size
  }

  /** Create the threads ahead of the first real call. Fire and forget. */
  warm () {
    if (this.broken || this.closed) return
    this.#ensure(this.size)
  }

  #ensure (want) {
    if (this.broken || this.closed) return
    while (this.workers.length < want) {
      try {
        const worker = new Worker(WORKER_URL, {
          name: `${this.name}-${this.workers.length}`,
          // DSH hosts can be launched with --input-type=module --eval. A Worker
          // loading a file must not inherit that argv, or Node rejects it with
          // "--input-type can only be used with ... --eval, --print, or STDIN".
          execArgv: [],
        })
        const slot = { worker, busy: false, id: this.workers.length, alive: true }
        worker.on('message', (message) => this.#settle(slot, message))
        worker.on('error', (err) => this.#onFailure(slot, err))
        worker.on('exit', (code) => {
          if (this.closed) return
          if (slot.alive) this.#onFailure(slot, new Error('worker exited with code ' + code))
        })
        this.workers.push(slot)
        this.stats.spawns += 1
      } catch (err) {
        this.broken = err
        return
      }
    }
  }

  #onFailure (slot, err) {
    slot.alive = false
    this.failures += 1
    const held = [...this.pending.entries()].filter(([, entry]) => entry.slot === slot)
    for (const [id, entry] of held) {
      this.pending.delete(id)
      entry.slot.busy = false
      if (entry.retried) entry.reject(err)
      else {
        this.stats.retries += 1
        this.queue.unshift({ ...entry.job, retried: true })
      }
    }
    const index = this.workers.indexOf(slot)
    if (index >= 0) this.workers.splice(index, 1)
    // A worker that dies on import dies every time: stop respawning and let the
    // engine run the same operation in-thread instead.
    if (this.failures > this.size * 3) {
      this.broken = err
      this.queue.splice(0).forEach((entry) => entry.reject(err))
      return
    }
    if (!this.closed) this.#ensure(this.size)
    this.#pump()
  }

  #settle (slot, message) {
    const entry = this.pending.get(message.id)
    if (!entry) return
    this.pending.delete(message.id)
    slot.busy = false
    this.failures = 0
    if (message.ok) entry.resolve(message.value)
    else entry.reject(new Error(message.error || 'worker failed'))
    this.#pump()
  }

  #pump () {
    if (this.broken || this.closed) {
      if (this.broken) {
        const err = this.broken
        for (const entry of this.queue.splice(0)) entry.reject(err)
      }
      return
    }
    while (this.queue.length > 0) {
      const slot = this.workers.find((w) => w.alive && !w.busy)
      if (!slot) return
      const job = this.queue.shift()
      const id = this.nextId++
      slot.busy = true
      this.pending.set(id, { slot, job, resolve: job.resolve, reject: job.reject, retried: job.retried === true })
      this.stats.jobs += 1
      slot.worker.postMessage({ id, op: job.op, payload: job.payload })
    }
  }

  run (op, payload) {
    if (this.broken || this.closed) return Promise.reject(this.broken ?? new Error('pool closed'))
    this.#ensure(this.size)
    return new Promise((resolve, reject) => {
      this.queue.push({ op, payload, resolve, reject, retried: false })
      this.#pump()
    })
  }

  /** Run one op over many payloads and merge the results with `merge`. */
  async map (op, payloads, merge, onValue, signal) {
    if (payloads.length === 0) return merge([], 0)
    const results = new Array(payloads.length)
    let cursor = 0
    const failures = []
    const lane = async () => {
      while (cursor < payloads.length) {
        // Cancellation has to stop dispatching, not just be observed later:
        // one in-flight call cannot be interrupted, but no new one should start.
        if (signal?.aborted) return
        const index = cursor++
        try {
          results[index] = await this.run(op, payloads[index])
          try { onValue?.(results[index]) } catch { /* progress must not break the run */ }
        } catch (err) {
          failures.push(err)
          results[index] = null
        }
      }
    }
    const lanes = Math.max(1, Math.min(this.size, payloads.length))
    await Promise.all(Array.from({ length: lanes }, lane))
    if (failures.length === payloads.length) {
      // Every job failed: this is infrastructure, not per-file trouble. Let the
      // caller fall back instead of reporting zero work done.
      throw failures[0]
    }
    const values = results.filter((r) => r !== null)
    if (failures.length > 0) {
      // A slice died and the rest survived. Dropping it silently would return a
      // smaller result that still says ok — so the loss is surfaced as a failure
      // entry the engine already knows how to report.
      values.push({
        files: 0,
        bytes: 0,
        ok: false,
        failed: failures.map((err) => ({ path: '(batch)', src: '(batch)', error: String(err?.message ?? err) })),
      })
    }
    return merge(values, failures.length)
  }

  close () {
    this.closed = true
    for (const slot of this.workers) {
      slot.alive = false
      try {
        slot.worker.terminate()
      } catch { /* already gone */ }
    }
    this.workers = []
    const closed = Object.assign(new Error('pool closed'), { name: 'AbortError' })
    for (const entry of this.queue.splice(0)) entry.reject(closed)
    // In-flight promises must settle too: clearing the map would leave their
    // callers awaiting a reply that can never arrive, which on plugin unload is
    // a tool call that hangs until the turn times out.
    for (const entry of this.pending.values()) entry.reject(closed)
    this.pending.clear()
  }
}

let shared = null

export function getPool (size) {
  if (!shared || shared.broken || shared.closed) shared = new WorkerPool({ size })
  return shared
}

export function closePool () {
  if (shared) shared.close()
  shared = null
}
