/**
 * Every number this plugin would otherwise hard-code, derived instead.
 *
 * A constant like "promote above 50,000 files" is a guess about someone else's
 * machine. On a 16-core desktop it is conservative; on a 4-core laptop with an
 * on-access scanner it promotes far too late; on a fast server it promotes a
 * copy that would have finished inline. The same is true of the worker count,
 * the dispatch size and the number of units a tree is split into.
 *
 * So the tuning lives in one place, and it is derived from two measurements:
 *
 *   cores    — how much parallelism the machine offers at all
 *   rate     — what this process actually achieved, as an exponential moving
 *              average over finished work (seeded from cores, replaced by
 *              reality after the first batch)
 *
 * Nothing here is a promise about performance; it is a starting point that
 * corrects itself. An explicit environment variable always wins, for the case
 * where the operator knows better than the measurement.
 */

import { availableParallelism } from 'node:os'

function clamp (value, low, high) {
  return Math.max(low, Math.min(high, value))
}

function numericEnv (name) {
  const raw = Number(process.env[name])
  return Number.isFinite(raw) && raw > 0 ? raw : null
}

export const cores = Math.max(1, availableParallelism?.() ?? 4)

/**
 * Worker threads to keep warm.
 *
 * Half the cores, because the queue depth that wins on a real disk is not one
 * per core: measured on 16 cores the curve peaked at 8 and fell off at 16 and
 * 32, and the filesystem filter, not the CPU, is what saturates first.
 */
export function poolSize () {
  return clamp(numericEnv('DSH_BULK_CONCURRENCY') ?? Math.round(cores / 2), 2, 16)
}

/** Work items per dispatch. Big enough that message overhead is noise. */
export function batchSize () {
  return clamp(poolSize() * 32, 64, 1024)
}

/**
 * How many units a delete plan should split a tree into.
 *
 * A few units per lane, so one slow subtree cannot stall the whole run and
 * every worker has something queued.
 */
export function deleteUnits () {
  return clamp(poolSize() * 32, 64, 2048)
}

/** How many directory listings to keep in flight while walking a tree. */
/** Chunk size for a flat target list (per-file work). */
export function chunkSize (total) {
  return Math.max(poolSize(), Math.ceil(total / (poolSize() * 2)))
}

/* ------------------------------------------------------------- throughput */

let rateEma = 0
let samples = 0

/**
 * Feed finished work back in. The average is weighted towards recent reality
 * but resistant to one bad batch, which is what a fixed seed cannot be.
 */
export function noteThroughput (files, ms) {
  if (!Number.isFinite(files) || !Number.isFinite(ms) || files <= 0 || ms <= 0) return
  if (files < 32) return // tiny batches measure startup, not the device
  const rate = files / (ms / 1000)
  if (rateEma === 0) rateEma = rate
  else rateEma = rateEma * 0.7 + rate * 0.3
  samples += 1
}

/** Best current guess for small-file throughput on this machine, files/s. */
export function filesPerSecond () {
  const override = numericEnv('DSH_BULK_RATE')
  if (override) return override
  if (rateEma > 0 && samples >= 1) return rateEma
  // A deliberately pessimistic seed: a cheap SSD with an on-access scanner
  // manages ~400 files/s per core for tiny files.
  return 400 * cores
}

/**
 * Should this call be handed off before it starts?
 *
 * The rule is time, not count: promote when the estimated work would spend most
 * of the patience budget. That keeps the decision correct on a machine this one
 * has never seen, and it corrects itself as soon as the process has measured
 * one real transfer.
 */
export function shouldPromote (files, budgetMs) {
  if (!Number.isFinite(files) || files < 0) return false
  if (numericEnv('DSH_BULK_PROMOTE_ABOVE')) {
    return files >= Number(process.env.DSH_BULK_PROMOTE_ABOVE)
  }
  const projected = (files / filesPerSecond()) * 1000
  return projected > budgetMs * 0.6
}

/** Snapshot for `doctor` and for explaining a decision to a user. */
export function describe () {
  return {
    cores,
    poolSize: poolSize(),
    batchSize: batchSize(),
    deleteUnits: deleteUnits(),
    chunkSize: chunkSize(1000),
    filesPerSecond: Math.round(filesPerSecond()),
    measured: samples > 0,
  }
}
