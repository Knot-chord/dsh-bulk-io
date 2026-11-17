#!/usr/bin/env node
/**
 * Standalone CLI for dsh-bulk-io — the same engine the DSH tools use, so a plain
 * pwsh 7 command can hand a whole batch to a warm runtime:
 *
 *   node cli.mjs write  --root . --from files.json
 *   node cli.mjs read   --root . --include "src/**\/*.ts" --max-bytes 200000
 *   node cli.mjs copy   --dest D:\out --source .\src --source .\docs
 *   node cli.mjs delete --root . --include "tmp/**"          # to .dsh-trash
 *   node cli.mjs scan   --root . --include "**\/*.js"
 *   node cli.mjs doctor
 */

import { spawnSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir, availableParallelism } from 'node:os'
import { join, resolve } from 'node:path'
import { performance } from 'node:perf_hooks'
import * as engine from './engine.js'
import * as tuning from './tuning.js'
import { closePool } from './pool.js'
import { diagnostics } from './probe.js'

const USAGE = `dsh-bulk — bulk read/write for many small files

  write   --root DIR --from FILE.json [--atomic] [--dry-run]
  read    --root DIR (--paths a,b | --include GLOB... ) [--grep RE] [--max-files N] [--max-bytes N]
  copy    --dest DIR --source PATH... [--strategy auto|threads|robocopy|archive] [--threads N]
  move    --dest DIR --source PATH...
  sync    --dest DIR --source PATH...
  delete  --root DIR (--paths a,b | --include GLOB...) [--permanent | --quarantine]
  scan    --root DIR --include GLOB...
  doctor

Common: --exclude GLOB  --json  --dry-run  --concurrency N
`

function parseArgs (argv) {
  const options = { _: [], sources: [], include: [], exclude: [], paths: [] }
  const bucket = { '--source': 'sources', '--include': 'include', '--exclude': 'exclude', '--path': 'paths' }
  const flags = new Set(['json', 'dry-run', 'permanent', 'atomic', 'force', 'quarantine'])
  for (let i = 0; i < argv.length; i++) {
    const token = argv[i]
    if (!token.startsWith('--')) {
      options._.push(token)
      continue
    }
    const key = token.slice(2).toLowerCase()
    if (flags.has(key)) {
      options[key] = true
      continue
    }
    const value = argv[++i]
    if (bucket[token]) {
      for (const part of String(value ?? '').split(',')) if (part) options[bucket[token]].push(part)
    } else {
      options[key] = value
    }
  }
  return options
}

function readManifest (path) {
  const raw = path === '-'
    ? readFileSync(0, 'utf8')
    : readFileSync(resolve(path), 'utf8')
  const trimmed = raw.trim()
  if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
    const parsed = JSON.parse(trimmed)
    if (Array.isArray(parsed)) return parsed
    if (Array.isArray(parsed.files)) return parsed.files
    throw new Error('manifest JSON must be an array of files or {"files": [...]}')
  }
  return trimmed.split(/\r?\n/).filter(Boolean).map((line, index) => {
    try {
      return JSON.parse(line)
    } catch {
      throw new Error(`manifest line ${index + 1} is not valid JSON`)
    }
  })
}

function number (value, fallback) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function print (result, wantsJson) {
  if (wantsJson) {
    process.stdout.write(JSON.stringify(result, null, 2) + '\n')
    return
  }
  process.stdout.write((result.text || result.summary || '') + '\n')
  if (!result.ok) process.exitCode = 1
}

async function doctor () {
  const root = mkdtempSync(join(tmpdir(), 'dsh-bulk-io-doctor-'))
  const files = Array.from({ length: 600 }, (_, i) => ({ path: `d${i % 8}/f${i}.txt`, content: 'x'.repeat(2048) }))
  // Warm the pool first: a cold first batch measures worker startup, not the disk.
  engine.poolFor(0).warm()
  await engine.writeFiles({ root, files: files.slice(0, 40) })
  const t0 = performance.now()
  await engine.writeFiles({ root, files })
  const ms = performance.now() - t0
  const rate = Math.round(600 / (ms / 1000))
  rmSync(root, { recursive: true, force: true })

  const found = ['robocopy', 'tar'].map((name) => {
    const probe = spawnSync(name, ['/?'], { windowsHide: true })
    return `${name}: ${probe.error ? 'missing' : 'present'}`
  })

  const info = engine.engineInfo()
  const diag = diagnostics()
  const tune = tuning.describe()
  const lines = [
    `node              ${process.version}`,
    `cpus              ${availableParallelism()}`,
    `worker pool       ${info.workers} threads (size ${info.poolSize})`,
    `derived tuning    batch ${tune.batchSize} · delete units ${tune.deleteUnits} · chunk ${tune.chunkSize} · seed ${tune.filesPerSecond}/s${tune.measured ? ' (settled on measurement)' : ' (no sample yet)'}`,
    `small-file rate   ${rate} files/s (2 KiB × 600, warm pool)`,
    ...found,
    `zstd              ${await hasZstd() ? 'in-process' : 'absent (tar stream stays uncompressed)'}`,
    `on-access scanner ${diag.scanner ?? 'none detected'}`,
    `path filters      ${diag.filters.length ? diag.filters.map((f) => f.filter).join(', ') : 'none'}`,
    `copy-on-write     ${diag.cloneVolumes.length ? diag.cloneVolumes.map((v) => `${v.volume} ${v.supported ? 'yes' : 'no'}`).join(', ') : 'not probed'}`,
    '',
    rate < 1500
      ? `This machine is slow at small files${diag.scanner ? `, and ${diag.scanner} is on the path` : ''}.\nEvery create/open/rename pays a scan. Excluding the working tree from real-time\nscanning is worth several times; no tool can outrun it. The tools themselves\navoid per-file work wherever a directory-level operation is possible.`
      : 'Small-file rate looks healthy.',
  ]
  process.stdout.write(lines.join('\n') + '\n')
}

async function hasZstd () {
  const zlib = await import('node:zlib').catch(() => null)
  return typeof zlib?.createZstdCompress === 'function'
}

async function main () {
  const argv = process.argv.slice(2)
  if (argv.length === 0 || argv[0] === '--help' || argv[0] === '-h') {
    process.stdout.write(USAGE)
    return
  }
  const command = argv[0]
  const options = parseArgs(argv.slice(1))
  const wantsJson = options.json === true
  const dryRun = options['dry-run'] === true

  try {
    if (command === 'doctor') {
      await doctor()
      return
    }

    if (command === 'write') {
      const manifest = readManifest(options.from ?? options._[0] ?? '')
      const result = await engine.writeFiles({
        root: options.root ?? '.',
        files: manifest,
        atomic: options.atomic === true,
        dryRun,
        concurrency: number(options.concurrency, 0),
      })
      print(result, wantsJson)
      return
    }

    if (command === 'read') {
      const result = await engine.readFiles({
        root: options.root ?? '.',
        paths: options.paths,
        include: options.include,
        exclude: options.exclude,
        maxFiles: number(options['max-files'], 200),
        maxBytesPerFile: number(options['max-bytes'], 65536),
        maxTotalBytes: number(options['max-bytes-total'], 262144),
        binary: options.binary ?? 'skip',
        headLines: number(options.head, 0),
        tailLines: number(options.tail, 0),
        grep: options.grep ?? '',
        concurrency: number(options.concurrency, 0),
      })
      print(result, wantsJson)
      return
    }

    if (command === 'copy' || command === 'move' || command === 'sync') {
      const result = await engine.transferFiles({
        op: command,
        sources: options.sources,
        dest: options.dest ?? '',
        strategy: options.strategy ?? 'auto',
        threads: number(options.threads, 32),
        overwrite: options.overwrite !== 'false',
        dryRun,
        exclude: options.exclude,
        concurrency: number(options.concurrency, 0),
      })
      print(result, wantsJson)
      return
    }

    if (command === 'delete') {
      const result = await engine.deleteFiles({
        root: options.root ?? '.',
        paths: options.paths,
        include: options.include,
        exclude: options.exclude,
        permanent: options.permanent === true,
        quarantine: options.quarantine === true,
        dryRun,
        maxFiles: number(options['max-files'], 20_000),
        force: options.force === true,
        concurrency: number(options.concurrency, 0),
      })
      print(result, wantsJson)
      return
    }

    if (command === 'scan') {
      const result = await engine.scanTree({
        root: options.root ?? '.',
        include: options.include,
        exclude: options.exclude,
        maxDepth: number(options['max-depth'], -1),
        maxFiles: number(options['max-files'], 200_000),
      })
      if (wantsJson) {
        process.stdout.write(JSON.stringify(result, null, 2) + '\n')
      } else {
        process.stdout.write(`${result.files.length} files${result.truncated ? ' (truncated)' : ''}\n`)
        for (const file of result.files.slice(0, 500)) process.stdout.write(`${String(file.size).padStart(10)}  ${file.path}\n`)
      }
      return
    }

    process.stderr.write(`unknown command: ${command}\n\n${USAGE}`)
    process.exitCode = 2
  } finally {
    closePool()
  }
}

main().catch((err) => {
  process.stderr.write(`dsh-bulk-io: ${err.message}\n`)
  closePool()
  process.exitCode = 1
})

export { readManifest }

