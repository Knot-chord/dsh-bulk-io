/**
 * The I/O pipeline: a bounded producer, an adaptive consumer, and nothing in
 * between that grows with the size of the tree.
 *
 * The 200,000-file measurement showed two separate problems and they need two
 * separate mechanisms:
 *
 * 1. Memory. Materialising every (src, dst) pair before starting cost 542 MiB.
 *    A bounded channel plus a batch-wise producer keeps in-flight work to
 *    `capacity` batches no matter how many files exist.
 *
 * 2. Utilisation. A fixed number of workers is a guess. The device, the filter
 *    driver and the file shape together decide how many operations must be
 *    outstanding to keep the disk busy, and that number is different on every
 *    machine. `runPaced` measures achieved throughput and moves the window up
 *    or down until it stops improving — the same hill-climb a database uses for
 *    I/O concurrency, and the reason it can find a window of 2 on this machine
 *    and 32 on another without being told.
 */

import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

/* ------------------------------------------------------------------ channel */

/**
 * A bounded async queue. `push` resolves immediately while there is room and
 * waits once the consumer falls behind, which is what turns "scan everything,
 * then work" into "scan a little, work a little".
 */
export function createChannel (capacity = 8) {
  const items = []
  const consumers = []
  let producers = []
  let closed = false

  const wakeConsumers = () => {
    const pending = consumers.splice(0)
    for (const resolve of pending) resolve()
  }

  const wakeProducers = () => {
    const pending = producers
    producers = []
    for (const resolve of pending) resolve()
  }

  return {
    push (item) {
      items.push(item)
      wakeConsumers()
      if (items.length <= capacity) return Promise.resolve()
      return new Promise((resolve) => producers.push(resolve))
    },
    close () {
      closed = true
      wakeConsumers()
      wakeProducers()
    },
    async * [Symbol.asyncIterator] () {
      for (;;) {
        if (items.length > 0) {
          const item = items.shift()
          if (items.length < capacity) wakeProducers()
          yield item
          continue
        }
        if (closed) return
        await new Promise((resolve) => consumers.push(resolve))
      }
    },
  }
}

/* ------------------------------------------------------------- pair stream */

/**
 * Walk a tree with bounded concurrency and yield work in batches.
 *
 * Directories are scanned `slots` at a time; finished listings become batches on
 * a channel that holds at most `capacity` of them. The total memory is therefore
 * `capacity + slots` listings, not the size of the tree.
 */
export async function * scanPairsStream ({
  root,
  destRoot,
  include = [],
  exclude = [],
  maxDepth = -1,
  withSize = true,
  pool,
  exec,
  signal,
  capacity = 8,
  batch = 256,
}) {
  const channel = createChannel(capacity)
  const queue = [{ dir: '.', depth: 0 }]
  const ready = []
  const errors = []
  let active = 0
  let notify = null
  const wake = () => { const r = notify; notify = null; r?.() }
  const waitTick = () => new Promise((resolve) => { notify = resolve })
  const limit = Math.max(1, pool.size)

  const producer = (async () => {
    try {
      for (;;) {
        if (signal?.aborted) break
        if (queue.length > 0 && active < limit) {
          const job = queue.shift()
          active += 1
          exec(pool, 'scan', {
            root,
            dir: job.dir,
            depth: job.depth,
            include,
            exclude,
            maxDepth,
            withSize,
            maxFiles: Number.MAX_SAFE_INTEGER,
            chunkDepth: 1,
          }).then((result) => {
            for (const next of result.frontier ?? []) queue.push(next)
            for (const error of result.errors ?? []) if (errors.length < 50) errors.push(error)
            if (result.files?.length > 0) ready.push(result.files)
          }, (error) => {
            if (errors.length < 50) errors.push({ path: job.dir, error: String(error.message ?? error) })
          }).finally(() => {
            active -= 1
            wake()
          })
          continue
        }
        if (ready.length > 0) {
          const files = ready.shift()
          const pairs = []
          for (const file of files) {
            pairs.push({
              src: join(root, file.path),
              dst: join(destRoot, file.path),
              size: file.size ?? 0,
            })
            if (pairs.length >= batch) {
              await channel.push(pairs.splice(0))
            }
          }
          if (pairs.length > 0) await channel.push(pairs)
          continue
        }
        if (active > 0) {
          await waitTick()
          continue
        }
        break
      }
    } finally {
      channel.close()
    }
  })()

  try {
    for await (const pairs of channel) yield pairs
  } finally {
    await producer.catch(() => {})
  }

  return { errors }
}

/* ------------------------------------------------------------------- paced */

/**
 * Run `run(payload)` over an async source, keeping `window` operations in
 * flight, and move the window until throughput stops improving.
 *
 * The controller is deliberately simple: every `tickMs`, compare this interval's
 * rate with the best seen. A clear improvement grows the window; a clear
 * regression shrinks it. That is enough to find the knee without a model of the
 * machine, and it costs one timer per call.
 */
export async function runPaced ({
  source,
  run,
  onValue,
  onProgress,
  window = 8,
  minWindow = 1,
  maxWindow = 32,
  adaptive = true,
  tickMs = 400,
  signal,
} = {}) {
  const iterator = source[Symbol.asyncIterator]()
  const startedAt = performance.now()
  let current = clamp(window, minWindow, maxWindow)
  let inFlight = 0
  let exhausted = false
  let failure = null
  let processed = 0
  let intervalCount = 0

  // The controller explores in one direction and then stops. A hill-climb that
  // is free to shrink reacts to the pipeline filling up as if that were a
  // regression, collapses to the minimum and never recovers — measured at
  // 1,447 files/s against 4,737 for the same work. So: warm up, step up while it
  // pays, step back once, freeze.
  const seed = current
  const candidate = Math.min(maxWindow, Math.max(seed, seed * 2))
  let phase = candidate > seed ? 'seed' : 'frozen'
  let ticks = 0
  let sum = 0
  let seedRate = 0

  const timer = adaptive && tickMs > 0 && candidate > seed
    ? setInterval(() => {
      const rate = intervalCount / (tickMs / 1000)
      intervalCount = 0
      if (phase === 'frozen') return
      sum += rate
      ticks += 1
      // Two ticks per window: one sample on this machine is too noisy to decide
      // (16 threads measured 4,737 files/s once and 2,868 the next run).
      if (ticks < 2) return
      const average = sum / ticks
      sum = 0
      ticks = 0
      if (phase === 'seed') {
        seedRate = average
        current = candidate
        phase = 'candidate'
      } else {
        current = average > seedRate * 1.05 ? candidate : seed
        phase = 'frozen'
      }
    }, tickMs)
    : null
  timer?.unref?.()

  // Progress is reported on its own timer: it must keep working when the window
  // is fixed and adaptation is off.
  const progressTimer = onProgress && tickMs > 0
    ? setInterval(() => {
      try {
        onProgress({ processed, inFlight, window: current, elapsedMs: performance.now() - startedAt })
      } catch { /* reporting must never break the transfer */ }
    }, Math.max(250, tickMs))
    : null
  progressTimer?.unref?.()

  let resolveIdle
  let idle = new Promise((resolve) => { resolveIdle = resolve })
  const checkIdle = () => {
    if (exhausted && inFlight === 0) resolveIdle()
  }

  // Cancellation has to settle the run. Without this the pump simply stops
  // dispatching, `exhausted` never becomes true, and the caller waits forever.
  const onAbort = () => {
    exhausted = true
    checkIdle()
  }
  signal?.addEventListener?.('abort', onAbort, { once: true })

  const pump = async () => {
    while (!exhausted && inFlight < current && !signal?.aborted) {
      let next
      try {
        next = await iterator.next()
      } catch (error) {
        failure = failure ?? error
        exhausted = true
        break
      }
      if (next.done) {
        exhausted = true
        break
      }
      inFlight += 1
      Promise.resolve()
        .then(() => run(next.value))
        .then((value) => {
          processed += 1
          intervalCount += 1
          try { onValue?.(value) } catch { /* a consumer error must not stop the run */ }
        }, (error) => {
          failure = failure ?? error
        })
        .finally(() => {
          inFlight -= 1
          pump()
          checkIdle()
        })
    }
    checkIdle()
  }

  // A signal that was already aborted never fires its event, so check first.
  if (signal?.aborted) exhausted = true
  pump()
  // setInterval returns a Timeout, which has no `clear` method — the old
  // `timer?.clear?.()` was a no-op, so both timers kept firing after every
  // transfer for the life of the process. clearInterval, and in a finally so a
  // throw on the way out cannot skip it.
  try {
    await idle
  } finally {
    if (timer) clearInterval(timer)
    if (progressTimer) clearInterval(progressTimer)
    signal?.removeEventListener?.('abort', onAbort)
  }

  if (signal?.aborted) {
    const error = new Error('cancelled')
    error.name = 'AbortError'
    throw error
  }
  if (failure) throw failure
  try {
    onProgress?.({ processed, inFlight, window: current, elapsedMs: performance.now() - startedAt })
  } catch { /* ignore */ }
  return { processed, window: current, seedRate }
}

function clamp (value, low, high) {
  return Math.max(low, Math.min(high, Math.floor(value)))
}

/* --------------------------------------------------------------- throughput */

/** Cumulative counters with a wall-clock rate, for reporting utilisation. */
export function createMeter () {
  const started = performance.now()
  let files = 0
  let bytes = 0
  let ops = 0
  return {
    add (value) {
      if (!value) return
      files += value.files ?? 0
      bytes += value.bytes ?? 0
      ops += 1
    },
    snapshot () {
      const ms = performance.now() - started
      return {
        files,
        bytes,
        ops,
        ms,
        filesPerSecond: ms > 0 ? Math.round(files / (ms / 1000)) : 0,
        bytesPerSecond: ms > 0 ? Math.round(bytes / (ms / 1000)) : 0,
      }
    },
  }
}
