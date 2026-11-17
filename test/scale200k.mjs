/**
 * 200,000-file scale test.
 *
 * Shape: 2,000 directories x 100 files of 100 B — the node_modules-like shape
 * the user actually has. Measures every operation the tools offer, plus peak
 * RSS so "bounded memory" is a measurement rather than a claim.
 */

import { mkdirSync, rmSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { performance } from 'node:perf_hooks'
import * as engine from '../lib/engine.js'
import { closePool } from '../lib/pool.js'

const N = Number(process.argv[2] || 200_000)
const FILES_PER_DIR = 100
const W = join(process.env.DSH_BULK_BENCH_ROOT ?? tmpdir(), '.bulkio-200k')
const BODY = 'x'.repeat(100)

let peak = 0
const rssTimer = setInterval(() => {
  const rss = process.memoryUsage().rss
  if (rss > peak) peak = rss
}, 50)
peak = process.memoryUsage().rss

function mib (bytes) { return (bytes / 1048576).toFixed(0) + ' MiB' }

async function time (label, fn) {
  const before = process.memoryUsage().rss
  const t0 = performance.now()
  const value = await fn()
  const ms = performance.now() - t0
  const rss = process.memoryUsage().rss
  const heap = process.memoryUsage().heapUsed
  if (rss > peak) peak = rss
  console.log(`${label.padEnd(42)} ${String(Math.round(ms)).padStart(7)} ms  rss ${mib(rss).padStart(7)}  heap ${mib(heap).padStart(7)}`)
  return { value, ms }
}

async function main () {
  rmSync(W, { recursive: true, force: true })
  mkdirSync(W, { recursive: true })
  engine.poolFor(0).warm()
  console.log(`scale test · ${N.toLocaleString('en-US')} files · ${FILES_PER_DIR}/dir · ${BODY.length} B each\n`)

  const files = Array.from({ length: N }, (_, i) => ({
    path: `d${Math.floor(i / FILES_PER_DIR)}/f${i}.txt`,
    content: BODY,
  }))
  console.log(`payload array built, rss ${mib(process.memoryUsage().rss)}`)

  // One tool call caps at 50,000 entries: a 200,000-entry argument is not
  // something a model emits anyway. Four batches is the real agent path.
  const CHUNK = 50_000
  let written = 0
  let writeMs = 0
  for (let offset = 0; offset < N; offset += CHUNK) {
    const slice = files.slice(offset, offset + CHUNK)
    const result = await time(
      `bulk_write batch ${Math.floor(offset / CHUNK) + 1}/${Math.ceil(N / CHUNK)} (${slice.length.toLocaleString('en-US')})`,
      () => engine.writeFiles({ root: W, files: slice }),
    )
    written += result.value.files
    writeMs += result.ms
  }
  console.log(`  -> ${written.toLocaleString('en-US')} files total in ${Math.round(writeMs)} ms = ${Math.round(written / (writeMs / 1000)).toLocaleString('en-US')} files/s`)
  files.length = 0

  const scan = await time('bulk_scan (with sizes)', () => engine.scanStats({ root: W, topN: 5 }))
  console.log(`  -> ${scan.value.files.toLocaleString('en-US')} files, ${(scan.value.bytes / 1048576).toFixed(1)} MiB`)

  const count = await time('bulk_scan (count only)', () => engine.scanStats({ root: W, withSize: false, topN: 0 }))

  const copyDest = W + '-copy'
  rmSync(copyDest, { recursive: true, force: true })
  const copy = await time('bulk_transfer copy whole tree', () => engine.transferFiles({ op: 'copy', sources: [W], dest: copyDest }))
  console.log(`  -> ${Math.round(copy.value.files / (copy.ms / 1000)).toLocaleString('en-US')} files/s, strategy ${copy.value.strategy}`)

  const movedDest = W + '-moved'
  rmSync(movedDest, { recursive: true, force: true })
  const move = await time('bulk_transfer move whole tree (rename)', () => engine.transferFiles({ op: 'move', sources: [copyDest], dest: movedDest }))
  console.log(`  -> strategy ${move.value.strategy}`)

  const q = await time('bulk_delete quarantine 200 dirs', () => engine.deleteFiles({ root: W, paths: Array.from({ length: 200 }, (_, i) => `d${i}`), quarantine: true }))
  console.log(`  -> ${q.value.files} entries, ${q.value.ms.toFixed(0)} ms`)

  const perm = await time('bulk_delete permanent 200 dirs', () => engine.deleteFiles({ root: W, paths: Array.from({ length: 200 }, (_, i) => `d${i + 200}`), permanent: true }))
  console.log(`  -> ${perm.value.files} entries, ${(perm.value.files / ((perm.value.ms || 1) / 1000)).toFixed(0)} files/s`)

  const rest = await time('bulk_delete permanent the full tree', () => engine.deleteFiles({ root: W, paths: ['.'], permanent: true }).catch((e) => ({ error: e.message })))
  if (rest.value.error) console.log(`  -> refused as expected: ${rest.value.error}`)

  const final = await time('bulk_delete permanent everything left', async () => {
    const dirs = Array.from({ length: 2000 }, (_, i) => `d${i}`).filter((d) => {
      try { statSync(join(W, d)); return true } catch { return false }
    })
    return engine.deleteFiles({ root: W, paths: dirs, permanent: true })
  })
  console.log(`  -> ${final.value.files} dirs, ${(final.value.files * FILES_PER_DIR / ((final.value.ms || 1) / 1000)).toFixed(0)} files/s`)

  clearInterval(rssTimer)
  console.log(`\npeak rss ${mib(peak)}`)
  rmSync(W, { recursive: true, force: true })
  rmSync(copyDest, { recursive: true, force: true })
  rmSync(movedDest, { recursive: true, force: true })
  closePool()
}

main().catch((err) => {
  clearInterval(rssTimer)
  console.error(err)
  rmSync(W, { recursive: true, force: true })
  closePool()
  process.exitCode = 1
})

