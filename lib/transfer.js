/**
 * Bulk transfer strategies.
 *
 * Three mature native paths do the heavy lifting, because the fastest correct
 * code is usually already installed:
 *
 *  - rename:      same-volume move. O(1) metadata change, no data touched.
 *  - robocopy:    Windows' own multi-threaded copier (/MT). It is the reference
 *                 implementation for trees with attributes, and its measured
 *                 small-file throughput (~3,200 files/s on a USB SSD) is what a
 *                 hand-written copier has to beat.
 *  - tar | zstd:  one stream instead of N round trips. Wins on high-latency
 *                 links (WAN, VPN, SMB over the internet) and when the
 *                 destination counts bytes. Compression runs in-process with
 *                 Node's zstd so no zstd.exe is required.
 */

import { spawn } from 'node:child_process'
import { mkdirSync } from 'node:fs'
import { zlib } from './zlib.js'

export function isUNC (path) {
  return /^\\\\[^\\]/.test(path) || /^\/\/[^/]/.test(path)
}

export function volumeOf (path) {
  const match = /^([A-Za-z]:\\|\\\\[^\\]+\\[^\\]+\\|\/)/.exec(path)
  return match ? match[1].toLowerCase() : path.toLowerCase()
}

export function sameVolume (a, b) {
  return volumeOf(a) === volumeOf(b)
}

/**
 * Kill a child when the caller's signal aborts.
 *
 * Both strategies can run for minutes, and a cancel that leaves the process
 * running is a cancel that only takes effect... never. The detach function must
 * run on every exit path or a long-lived process accumulates listeners.
 */
function killOnAbort (child, signal) {
  if (!signal) return () => {}
  const onAbort = () => {
    try { child.kill('SIGTERM') } catch { /* already gone */ }
  }
  if (signal.aborted) onAbort()
  else signal.addEventListener('abort', onAbort, { once: true })
  return () => signal.removeEventListener?.('abort', onAbort)
}

function spawnAsync (command, args, { stdin = 'ignore', stdout = 'ignore', signal } = {}) {
  return new Promise((resolve, reject) => {
    let child
    try {
      child = spawn(command, args, { windowsHide: true, stdio: [stdin, stdout, 'pipe'] })
    } catch (err) {
      reject(err)
      return
    }
    let stderr = ''
    child.stderr?.on('data', (chunk) => {
      if (stderr.length < 16384) stderr += chunk.toString()
    })
    const detach = killOnAbort(child, signal)
    child.on('error', (err) => {
      detach()
      err.code = err.code || 'ESPAWN'
      reject(err)
    })
    child.on('close', (code) => {
      detach()
      resolve({ code, stderr })
    })
  })
}

/** Robocopy counts success in its exit code: anything below 8 worked. */
export function robocopySucceeded (code) {
  return Number.isInteger(code) && code >= 0 && code < 8
}

export async function robocopy ({
  src,
  dst,
  move = false,
  sync = false,
  threads = 32,
  dryRun = false,
  exclude = [],
  signal,
}) {
  mkdirSync(dst, { recursive: true })
  const args = [
    src,
    dst,
    '/E',
    `/MT:${Math.max(1, Math.min(128, threads))}`,
    '/NFL',
    '/NDL',
    '/NP',
    '/NJH',
    '/NJS',
    '/R:1',
    '/W:1',
  ]
  if (move) args.push('/MOV')
  if (sync) args.push('/XO')
  if (dryRun) args.push('/L')
  for (const pattern of exclude ?? []) args.push('/XF', pattern)

  signal?.throwIfAborted?.()
  const result = await spawnAsync('robocopy', args, { signal })
  if (!robocopySucceeded(result.code)) {
    const err = new Error(`robocopy exited with code ${result.code}: ${result.stderr.trim().slice(0, 400)}`)
    err.code = 'EROBOLCOPY'
    throw err
  }
  return { code: result.code }
}

/**
 * Stream one directory tree through a single compressed pipe into another.
 * `tar` is present on Windows 10+, macOS and most Linux images.
 */
export async function archivePipe ({ src, dst, level = 3, signal }) {
  mkdirSync(dst, { recursive: true })
  signal?.throwIfAborted?.()

  const list = spawn('tar', ['-cf', '-', '-C', src, '.'], {
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const extract = spawn('tar', ['-xf', '-', '-C', dst], {
    windowsHide: true,
    stdio: ['pipe', 'ignore', 'pipe'],
  })

  const detachList = killOnAbort(list, signal)
  const detachExtract = killOnAbort(extract, signal)

  let listError = ''
  let extractError = ''
  list.stderr.on('data', (chunk) => { if (listError.length < 8192) listError += chunk.toString() })
  extract.stderr.on('data', (chunk) => { if (extractError.length < 8192) extractError += chunk.toString() })

  const compressor = zlib ? zlib.createZstdCompress({ params: { [zlib.constants.ZSTD_c_compressionLevel]: level } }) : null

  const closed = new Promise((resolve, reject) => {
    let listCode = null
    let extractCode = null
    let settled = false
    const done = () => {
      if (settled || listCode === null || extractCode === null) return
      settled = true
      if (listCode === 0 && extractCode === 0) resolve({ listCode, extractCode })
      else {
        const err = new Error(`tar pipeline failed (list ${listCode}, extract ${extractCode}): ${(listError + ' ' + extractError).trim().slice(0, 400)}`)
        err.code = 'ETAR'
        reject(err)
      }
    }
    list.on('error', reject)
    extract.on('error', reject)
    list.on('close', (code) => { detachList(); listCode = code; done() })
    extract.on('close', (code) => { detachExtract(); extractCode = code; done() })
  })

  if (compressor) {
    list.stdout.pipe(compressor).pipe(extract.stdin)
  } else {
    list.stdout.pipe(extract.stdin)
  }
  compressor?.on('error', (err) => extract.stdin.destroy(err))
  extract.stdin.on('error', () => { /* both sides report through close codes */ })

  return closed
}
