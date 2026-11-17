/**
 * Concurrency sweep: with `concurrency` now actually honoured, find the window
 * that keeps the disk busy, and confirm memory no longer scales with the tree.
 *
 * Explicit windows run with adaptation off, so each row is the real rate at that
 * window. The last row is the adaptive controller on its own.
 */

import { mkdirSync, rmSync } from 'node:fs'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'
import * as engine from '../lib/engine.js'
import { closePool } from '../lib/pool.js'

const N = Number(process.argv[2] || 30_000)
const BASE = 'D:\\Deepseek Hardness\\.bulkio-win'
const SRC = join(BASE, 'src')
const BODY = 'x'.repeat(100)
let peak = process.memoryUsage().rss
const timer = setInterval(() => { const r = process.memoryUsage().rss; if (r > peak) peak = r }, 50)

async function main () {
  rmSync(BASE, { recursive: true, force: true })
  mkdirSync(SRC, { recursive: true })
  engine.poolFor(0).warm()

  const files = Array.from({ length: N }, (_, i) => ({ path: `d${Math.floor(i / 100)}/f${i}.txt`, content: BODY }))
  for (let o = 0; o < files.length; o += 50_000) await engine.writeFiles({ root: SRC, files: files.slice(o, o + 50_000) })
  files.length = 0
  console.log(`${N.toLocaleString('en-US')} files ready\n`)
  console.log('work'.padEnd(26) + 'time'.padStart(9) + 'files/s'.padStart(10) + 'rss'.padStart(9) + 'heap'.padStart(9) + '  window')

  for (const concurrency of [1, 2, 4, 8, 16, 32, 0]) {
    const dest = join(BASE, `c${concurrency}`)
    const before = process.memoryUsage().rss
    const t0 = performance.now()
    const r = await engine.transferFiles({ op: 'copy', sources: [SRC], dest, concurrency })
    const ms = performance.now() - t0
    const rss = process.memoryUsage().rss
    if (rss > peak) peak = rss
    const label = concurrency === 0 ? 'adaptive' : `window ${concurrency}`
    const heap = process.memoryUsage().heapUsed
    console.log(
      label.padEnd(26) +
      `${Math.round(ms)} ms`.padStart(9) +
      `${Math.round(r.files / (ms / 1000)).toLocaleString('en-US')}`.padStart(10) +
      `${Math.round(rss / 1048576)} MiB`.padStart(9) +
      `${Math.round(heap / 1048576)} MiB`.padStart(9) +
      `  ${r.window ?? concurrency}`,
    )
    const dirs = Array.from({ length: Math.ceil(N / 100) }, (_, i) => `d${i}`)
    await engine.deleteFiles({ root: join(dest, 'src'), paths: dirs, permanent: true, concurrency: 16 })
  }

  clearInterval(timer)
  console.log(`\npeak rss ${Math.round(peak / 1048576)} MiB`)
  rmSync(BASE, { recursive: true, force: true })
  closePool()
}

main().catch((err) => {
  clearInterval(timer)
  console.error(err)
  rmSync(BASE, { recursive: true, force: true })
  closePool()
  process.exitCode = 1
})

