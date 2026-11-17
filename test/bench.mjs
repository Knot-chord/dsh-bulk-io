/**
 * Measures the three things this plugin claims to improve:
 *   1. one call for a batch instead of N tool calls / N process spawns;
 *   2. real parallel syscalls instead of libuv's four-thread pool;
 *   3. the cheapest native path for a move (rename) and for a tree copy.
 *
 * Run with: node test/bench.mjs [files] [bytesPerFile]
 */

import { mkdirSync, readdirSync, rmSync, writeFileSync, writeFileSync as writeSync } from 'node:fs'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { performance } from 'node:perf_hooks'
import * as engine from '../lib/engine.js'
import { closePool } from '../lib/pool.js'

const FILES = Number(process.argv[2] || 3000)
const SIZE = Number(process.argv[3] || 4096)
const BODY = 'x'.repeat(SIZE)

function makeRoot (label) {
  const dir = mkdtempSync(join(tmpdir(), `bulk-io-${label}-`))
  return dir
}

async function time (label, fn) {
  const t0 = performance.now()
  const value = await fn()
  const ms = performance.now() - t0
  const perSec = Math.round(FILES / (ms / 1000))
  console.log(`  ${label.padEnd(38)} ${String(Math.round(ms)).padStart(7)} ms  ${String(perSec).padStart(8)} files/s  ${String(Math.round(FILES * SIZE / 1024 / 1024 / (ms / 1000) * 100) / 100).padStart(7)} MiB/s`)
  return { ms, value }
}

function cleanup (dir) {
  try { rmSync(dir, { recursive: true, force: true }) } catch { /* ignore */ }
}

async function main () {
  console.log(`bulk-io benchmark · ${FILES} files × ${SIZE} B (${(FILES * SIZE / 1024 / 1024).toFixed(1)} MiB)`)
  console.log(`node ${process.version} · ${(await import('node:os')).availableParallelism()} cpus\n`)

  const root = makeRoot('write')
  const names = Array.from({ length: FILES }, (_, i) => `d${i % 50}/f${i}.txt`)

  console.log('write')
  await time('sequential writeFileSync (naive loop)', () => {
    const t0 = performance.now()
    for (const name of names) {
      mkdirSync(join(root, name, '..'), { recursive: true })
      writeSync(join(root, name), BODY)
    }
    return performance.now() - t0
  })

  {
    const { writeFile } = await import('node:fs/promises')
    await time('Promise.all writeFile (libuv pool)', async () => {
      await Promise.all(names.map(async (name) => {
        await mkdirSync(join(root, name, '..'), { recursive: true })
        await writeFile(join(root, name), BODY)
      }))
    })
  }

  cleanup(root)
  const root2 = makeRoot('bulkwrite')
  let poolMs
  {
    const r = await engine.writeFiles({
      root: root2,
      files: names.map((path) => ({ path, content: BODY })),
    })
    poolMs = r.ms
    console.log(`  ${'bulk_write (warm worker pool)'.padEnd(38)} ${String(Math.round(r.ms)).padStart(7)} ms  ${String(Math.round(FILES / (r.ms / 1000))).padStart(8)} files/s  ${String(Math.round(FILES * SIZE / 1024 / 1024 / (r.ms / 1000) * 100) / 100).padStart(7)} MiB/s`)
    console.log(`    ${r.text.split('\n')[0]}`)
  }

  console.log('\nread')
  {
    const r = await engine.readFiles({ root: root2, include: ['**/*.txt'], maxFiles: FILES, maxTotalBytes: 4 * 1024 * 1024, maxBytesPerFile: SIZE })
    console.log(`  ${'bulk_read (warm worker pool)'.padEnd(38)} ${String(Math.round(r.ms)).padStart(7)} ms  ${String(Math.round(FILES / (r.ms / 1000))).padStart(8)} files/s  ${String(Math.round(r.bytes / 1024 / 1024 / (r.ms / 1000) * 100) / 100).padStart(7)} MiB/s`)
    console.log(`    ${r.text.split('\n')[0]}`)
  }

  console.log('\ncopy')
  const copyDir = makeRoot('copy')
  {
    const r = await engine.transferFiles({ op: 'copy', sources: [root2], dest: copyDir })
    console.log(`  ${'bulk_transfer copy (threads)'.padEnd(38)} ${String(Math.round(r.ms)).padStart(7)} ms  ${String(Math.round(r.files / (r.ms / 1000))).padStart(8)} files/s  ${String(Math.round(r.bytes / 1024 / 1024 / (r.ms / 1000) * 100) / 100).padStart(7)} MiB/s`)
  }
  const copyDir2 = makeRoot('copy-rc')
  {
    const r = await engine.transferFiles({ op: 'copy', sources: [root2], dest: copyDir2, strategy: 'robocopy' })
    console.log(`  ${'bulk_transfer copy (robocopy /MT:32)'.padEnd(38)} ${String(Math.round(r.ms)).padStart(7)} ms  ${String(Math.round(r.files / (r.ms / 1000))).padStart(8)} files/s  ${String(Math.round(r.bytes / 1024 / 1024 / (r.ms / 1000) * 100) / 100).padStart(7)} MiB/s`)
  }

  console.log('\nmove')
  const moveDir = makeRoot('move')
  {
    const r = await engine.transferFiles({ op: 'move', sources: [copyDir2], dest: moveDir })
    console.log(`  ${'bulk_transfer move (rename)'.padEnd(38)} ${String(Math.round(r.ms)).padStart(7)} ms   strategy=${r.strategy}`)
  }

  for (const dir of [root, root2, copyDir, copyDir2, moveDir]) cleanup(dir)
  closePool()
  console.log('\ndone')
}

main().catch((err) => {
  console.error(err)
  closePool()
  process.exitCode = 1
})
