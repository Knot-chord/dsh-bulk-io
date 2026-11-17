import { mkdirSync, rmSync } from "node:fs"
import { join } from "node:path"
import { WorkerPool } from "../lib/pool.js"
import { performance } from "node:perf_hooks"
import { tmpdir } from "node:os"

const BENCH_ROOT = process.env.DSH_BULK_BENCH_ROOT ?? tmpdir()

const N = 1500, SIZE = 4096
const body = "x".repeat(SIZE)
const files = Array.from({length: N}, (_, i) => ({ path: `d${i%40}/f${i}.txt`, content: body }))

for (const size of [1, 2, 4, 8, 16, 32]) {
  const root = join(BENCH_ROOT, `.bench-pool-${size}`)
  rmSync(root, {recursive: true, force: true}); mkdirSync(root, {recursive: true})
  const pool = new WorkerPool({ size })
  // warm up the threads with a tiny job so spawn cost is not measured
  await pool.run("write", { root, files: [{path: "warm.txt", content: "w"}] })
  const width = Math.max(16, Math.ceil(N / Math.max(1, size * 4)))
  const slices = []
  for (let i = 0; i < files.length; i += width) slices.push({ root, files: files.slice(i, i + width) })
  const t0 = performance.now()
  const results = await Promise.all(slices.map(s => pool.run("write", s)))
  const ms = performance.now() - t0
  const ok = results.reduce((a, r) => a + r.files, 0)
  console.log(`pool=${String(size).padStart(2)}  ${String(Math.round(ms)).padStart(6)} ms  ${String(Math.round(ok/(ms/1000))).padStart(7)} files/s  ${String(Math.round(ok*SIZE/1048576/(ms/1000)*10)/10).padStart(6)} MiB/s`)
  pool.close(); rmSync(root, {recursive: true, force: true})
}

