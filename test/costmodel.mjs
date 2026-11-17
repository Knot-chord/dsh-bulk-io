/**
 * Cost model: where the time actually goes for small-file work on THIS machine.
 *
 * Every number is measured, not assumed. Groups:
 *   A  per-operation cost at a fixed small size
 *   B  size sweep: when does the cost stop being metadata and start being bytes
 *   C  cross-volume copy
 *   D  directory rename vs per-file rename
 *   E  copy-on-write (FICLONE) support per volume
 *   F  does the scanner care about content (AV content inspection) or only count
 */

import {
  closeSync, copyFileSync, constants as FS, mkdirSync, openSync, readFileSync, readSync, readdirSync,
  renameSync, rmSync, statSync, unlinkSync, writeFileSync,
} from 'node:fs'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

const WORK = 'D:\\Deepseek Hardness'
const results = []

function ops (label, n, fn) {
  const t0 = performance.now()
  fn()
  const ms = performance.now() - t0
  const perOp = ms / n
  results.push({ label, n, ms, perOp })
  console.log(`  ${label.padEnd(44)} ${String(Math.round(ms)).padStart(7)} ms  ${(perOp * 1000).toFixed(0).padStart(7)} µs/op  ${String(Math.round(n / (ms / 1000))).padStart(8)} ops/s`)
  return ms
}

function fresh (base, name) {
  const dir = join(base, name)
  rmSync(dir, { recursive: true, force: true })
  mkdirSync(dir, { recursive: true })
  return dir
}

function makeFiles (dir, n, size, subdirs = 20) {
  const names = []
  for (let i = 0; i < n; i++) names.push(join(dir, `d${i % subdirs}`, `f${i}.bin`))
  for (let d = 0; d < subdirs; d++) mkdirSync(join(dir, `d${d}`), { recursive: true })
  const body = Buffer.alloc(size, 0x61)
  for (const name of names) writeFileSync(name, body)
  return names
}

function probeFiclone (dir) {
  const src = join(dir, 'clone-src.bin')
  const dst = join(dir, 'clone-dst.bin')
  writeFileSync(src, Buffer.alloc(65536, 1))
  let force = 'no'
  let fallback = 'no'
  try {
    copyFileSync(src, dst, FS.COPYFILE_FICLONE_FORCE)
    force = 'YES'
  } catch { /* unsupported */ }
  try {
    rmSync(dst, { force: true })
    copyFileSync(src, dst, FS.COPYFILE_FICLONE)
    fallback = 'yes'
  } catch { /* nothing */ }
  rmSync(src, { force: true })
  rmSync(dst, { force: true })
  return { force, fallback }
}

async function main () {
  console.log(`cost model · node ${process.version}\n`)

  // ---------------------------------------------------------------- A: per-op
  console.log('A. per-operation cost, 1 KiB files, n=1500')
  const a = fresh(WORK, '.cm-a')
  const names = makeFiles(a, 1500, 1024)

  ops('create (writeFileSync, exists)', 1500, () => {
    for (let i = 0; i < 1500; i++) writeFileSync(join(a, `d${i % 20}`, `n${i}.bin`), 'x')
  })
  ops('stat', 1500, () => { for (const n of names) statSync(n).size })
  ops('read whole file (readFileSync)', 1500, () => { for (const n of names) readFileSync(n) })
  ops('open+read+close (no stat)', 1500, () => {
    for (const n of names) {
      const fd = openSync(n, 'r')
      const buf = Buffer.allocUnsafe(1024)
      readSyncCompat(fd, buf)
      closeSync(fd)
    }
  })
  const b = fresh(WORK, '.cm-b')
  ops('copy (copyFileSync)', 1500, () => {
    for (let i = 0; i < 1500; i++) {
      const dst = join(b, `d${i % 20}`, `n${i}.bin`)
      mkdirSync(join(b, `d${i % 20}`), { recursive: true })
      copyFileSync(names[i], dst)
    }
  })
  ops('rename in place', 1500, () => {
    for (let i = 0; i < 1500; i++) {
      const src = join(b, `d${i % 20}`, `n${i}.bin`)
      renameSync(src, src + '.r')
      renameSync(src + '.r', src)
    }
  })
  ops('unlink', 1500, () => { for (let i = 0; i < 1500; i++) unlinkSync(join(b, `d${i % 20}`, `n${i}.bin`)) })
  ops('readdir 20 dirs (withFileTypes)', 20, () => { for (let d = 0; d < 20; d++) readdirSync(join(a, `d${d}`), { withFileTypes: true }) })

  // -------------------------------------------------------------- B: size sweep
  console.log('\nB. write cost vs file size (total ~256 MiB of payload per row)')
  for (const size of [0, 1024, 65536, 1048576, 16777216]) {
    const dir = fresh(WORK, `.cm-b${size}`)
    const count = Math.max(8, Math.min(4000, Math.floor(268435456 / Math.max(1, size))))
    const body = Buffer.alloc(size, 0x61)
    ops(`write ${size === 0 ? 'empty' : (size / 1024) + ' KiB'} × ${count}`, count, () => {
      for (let i = 0; i < count; i++) writeFileSync(join(dir, `f${i}.bin`), body)
    })
    rmSync(dir, { recursive: true, force: true })
  }

  // ------------------------------------------------------------ C: cross volume
  console.log('\nC. copy 600 × 64 KiB: same volume vs cross volume')
  {
    const src = fresh(WORK, '.cm-c-src')
    makeFiles(src, 600, 65536, 10)
    const same = fresh(WORK, '.cm-c-same')
    ops('same volume (D: -> D:)', 600, () => {
      for (let i = 0; i < 600; i++) {
        const dst = join(same, `d${i % 10}`, `f${i}.bin`)
        mkdirSync(join(same, `d${i % 10}`), { recursive: true })
        copyFileSync(join(src, `d${i % 10}`, `f${i}.bin`), dst)
      }
    })
    const crossBase = join(tmpdir(), 'cm-cross')
    rmSync(crossBase, { recursive: true, force: true })
    mkdirSync(crossBase, { recursive: true })
    ops('cross volume (D: -> C:)', 600, () => {
      for (let i = 0; i < 600; i++) {
        const dst = join(crossBase, `d${i % 10}`, `f${i}.bin`)
        mkdirSync(join(crossBase, `d${i % 10}`), { recursive: true })
        copyFileSync(join(src, `d${i % 10}`, `f${i}.bin`), dst)
      }
    })
    rmSync(src, { recursive: true, force: true })
    rmSync(same, { recursive: true, force: true })
    rmSync(crossBase, { recursive: true, force: true })
  }

  // --------------------------------------------------------- D: dir rename
  console.log('\nD. move 1500 files: per-file rename vs one directory rename')
  {
    const src = fresh(WORK, '.cm-d')
    makeFiles(src, 1500, 1024)
    ops('per-file rename (same parent)', 1500, () => {
      for (let i = 0; i < 1500; i++) {
        const p = join(src, `d${i % 20}`, `f${i}.bin`)
        renameSync(p, p + '.m')
      }
    })
    const moved = join(WORK, '.cm-d-moved')
    rmSync(moved, { recursive: true, force: true })
    ops('one directory rename', 1, () => { renameSync(src, moved) })
    rmSync(moved, { recursive: true, force: true })
  }

  // ------------------------------------------------------------ E: FICLONE
  console.log('\nE. copy-on-write support')
  for (const [label, dir] of [['D: (workspace)', WORK], ['C: (temp)', tmpdir()]]) {
    const probeDir = mkdtempSync(join(dir, 'cm-clone-'))
    const r = probeFiclone(probeDir)
    console.log(`  ${label.padEnd(20)} FICLONE_FORCE: ${r.force === 'YES' ? 'supported' : 'not supported'}   FICLONE fallback: ${r.fallback}`)
    rmSync(probeDir, { recursive: true, force: true })
  }

  // ------------------------------------------------------ F: content sensitivity
  console.log('\nF. does the scanner inspect content? same size, different payload')
  {
    const dir = fresh(WORK, '.cm-f')
    const text = Buffer.alloc(65536, 0x41)
    const pe = Buffer.alloc(65536, 0)
    pe.write('MZ', 0, 'latin1')
    pe.writeUInt32LE(0x80, 0x3c)
    pe.write('PE\0\0', 0x80, 'latin1')
    ops('512 × 64 KiB plain text .txt', 512, () => {
      for (let i = 0; i < 512; i++) writeFileSync(join(dir, `t${i}.txt`), text)
    })
    ops('512 × 64 KiB PE-looking .exe', 512, () => {
      for (let i = 0; i < 512; i++) writeFileSync(join(dir, `e${i}.exe`), pe)
    })
    rmSync(dir, { recursive: true, force: true })
  }

  rmSync(a, { recursive: true, force: true })
  rmSync(b, { recursive: true, force: true })

  console.log('\nsummary (µs per operation)')
  for (const r of results) console.log(`  ${r.label.padEnd(44)} ${(r.perOp * 1000).toFixed(0).padStart(7)} µs`)
}

function readSyncCompat (fd, buf) {
  return readSync(fd, buf, 0, buf.length, 0)
}

main().catch((err) => {
  console.error(err)
  process.exitCode = 1
})
