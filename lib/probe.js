/**
 * Capability probing: everything the tools need to pick the right strategy
 * without asking the user to configure anything.
 *
 * All of it is best effort and cached for the process. A probe that fails
 * degrades to the portable path; it never fails a tool call.
 *
 * - `filters()`      which filesystem filter drivers are loaded (Windows). This
 *                    is what turns "this batch feels slow" into "sysdiag
 *                    (火绒) is on the path", which is the only actionable thing
 *                    an agent can tell a user.
 * - `volumeOf()`     the volume a path lives on, including UNC and device paths.
 * - `topology()`     same-volume / cross-volume / network / device, which decides
 *                    rename vs copy vs stream.
 * - `cloneSupport()` whether a volume supports copy-on-write clones (ReFS,
 *                    APFS, btrfs, XFS). Measured once per volume with a 64 KiB
 *                    probe, because a failed FICLONE costs an ioctl per file.
 */

import { execFileSync } from 'node:child_process'
import { constants as FS, copyFileSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, parse } from 'node:path'

const CACHE = {
  filters: undefined,
  clone: new Map(),
  volumes: new Map(),
}

/**
 * Which on-access scanners are loaded on this machine.
 *
 * `fltmc` is the obvious tool, but it refuses to run when its parent has no
 * console, so a plugin cannot call it. `sc query` answers the same question for
 * a known name — the minifilter's service is the filter's name — in about 10 ms
 * and works from any process. A product that is installed but stopped is not a
 * cost, so only RUNNING counts.
 */
const SCANNERS = [
  ['sysdiag', '火绒 Huorong'],
  ['WdFilter', 'Microsoft Defender'],
  ['360FsFlt', '360 安全卫士'],
  ['QQPCTry', 'QQ 电脑管家'],
  ['QQPCRealTimeSpeedup', 'QQ 电脑管家'],
  ['klif', 'Kaspersky'],
  ['eamon', 'ESET'],
  ['SymEFA', 'Symantec'],
  ['mfehidk', 'McAfee'],
  ['bdfilefilter', '百度卫士'],
  ['cfr', 'Comodo'],
]

export function filters () {
  if (CACHE.filters !== undefined) return CACHE.filters
  if (process.platform !== 'win32') {
    CACHE.filters = []
    return CACHE.filters
  }
  const found = []
  for (const [service, product] of SCANNERS) {
    if (serviceState(service) !== 'RUNNING') continue
    found.push({ filter: service, product })
  }
  CACHE.filters = found
  return found
}

function serviceState (name) {
  try {
    const output = execFileSync('sc', ['query', name], {
      windowsHide: true,
      timeout: 3000,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    })
    const state = /STATE\s*:\s*\d+\s+(\w+)/.exec(output)
    return state ? state[1].toUpperCase() : 'UNKNOWN'
  } catch {
    return 'ABSENT'
  }
}

/** A product name string for diagnostics, or '' when nothing notable is loaded. */
export function scannerName () {
  const list = filters()
  if (list.length === 0) return ''
  return [...new Set(list.map((entry) => entry.product))].join(' + ')
}

export function isUNC (path) {
  return /^\\\\[^\\]/.test(path) || /^\/\/[^/]/.test(path)
}

/** `C:`, `\\server\share`, or `/`. */
export function volumeRoot (path) {
  const text = String(path)
  const hit = CACHE.volumes.get(text)
  if (hit) return hit
  let root
  if (isUNC(text)) {
    const parts = text.replace(/\//g, '\\').split('\\').filter(Boolean)
    root = '\\\\' + parts.slice(0, 2).join('\\') + '\\'
  } else {
    root = parse(text).root || '/'
  }
  CACHE.volumes.set(text, root)
  return root
}

/**
 * The transfer topology, which is what actually selects a strategy:
 *   same-volume  rename is free
 *   same-device  different volume letters on one physical disk: still a copy,
 *                but renames across them are cheap enough to try
 *   network      one stream beats N round trips when latency is high
 *   removable    sync/avoid buffering
 */
export function topology (src, dst) {
  if (isUNC(dst) || isUNC(src)) return 'network'
  const a = volumeRoot(src)
  const b = volumeRoot(dst)
  if (a.toLowerCase() === b.toLowerCase()) return 'same-volume'
  return 'cross-volume'
}

/**
 * Does this volume support copy-on-write clones? Probed once with a 64 KiB file
 * and `COPYFILE_FICLONE_FORCE`, because the non-forcing flag silently falls back
 * and we need to know which one we are paying for.
 */
export function cloneSupport (volumePath) {
  const key = volumeRoot(volumePath).toLowerCase()
  const cached = CACHE.clone.get(key)
  if (cached !== undefined) return cached
  let supported = false
  try {
    const dir = mkdtempSync(join(volumeRoot(volumePath), process.platform === 'win32' ? 'dsh-clone-' : '.dsh-clone-'))
    try {
      const src = join(dir, 'a.bin')
      const dst = join(dir, 'b.bin')
      writeFileSync(src, Buffer.alloc(65536, 1))
      copyFileSync(src, dst, FS.COPYFILE_FICLONE_FORCE)
      supported = true
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  } catch {
    supported = false
  }
  CACHE.clone.set(key, supported)
  return supported
}

/** For the doctor command and the tool's slow-path note. */
export function diagnostics () {
  return {
    platform: process.platform,
    scanner: scannerName() || null,
    filters: filters(),
    cloneVolumes: [...CACHE.clone.entries()].map(([volume, supported]) => ({ volume, supported })),
    tempVolume: volumeRoot(tmpdir()),
  }
}
