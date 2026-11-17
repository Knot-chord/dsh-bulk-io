/** Human-readable formatting shared by every tool result. */

export function fmtBytes (n) {
  if (!Number.isFinite(n) || n < 0) return String(n)
  if (n < 1024) return n + ' B'
  if (n < 1024 * 1024) return fmt(n / 1024) + ' KiB'
  if (n < 1024 * 1024 * 1024) return fmt(n / 1024 / 1024) + ' MiB'
  return fmt(n / 1024 / 1024 / 1024) + ' GiB'
}

function fmt (n) {
  return n >= 100 ? n.toFixed(0) : n >= 10 ? n.toFixed(1) : n.toFixed(2)
}

export function fmtMs (ms) {
  if (!Number.isFinite(ms) || ms < 0) return '?'
  if (ms < 1) return '<1 ms'
  if (ms < 1000) return Math.round(ms) + ' ms'
  if (ms < 60_000) return (ms / 1000).toFixed(2) + ' s'
  return (ms / 60_000).toFixed(2) + ' min'
}

export function fmtRate (bytes, ms) {
  if (!Number.isFinite(ms) || ms <= 0) return '?'
  return fmtBytes(Math.round(bytes / (ms / 1000))) + '/s'
}

/** Seconds a user actually budgets for: one thousand files at this rate. */
export function perThousand (files, ms) {
  if (!Number.isFinite(files) || files <= 0 || !Number.isFinite(ms) || ms <= 0) return ''
  const per = (ms / files) * 1000
  if (per < 1000) return `${per < 10 ? per.toFixed(1) : Math.round(per)} ms per 1,000 files`
  return `${(per / 1000).toFixed(2)} s per 1,000 files`
}

/** "1 file" / "300 files" — a headline that says "1 paths" reads like a bug. */
export function plural (n, one, many = one + 's') {
  return `${fmtCount(n)} ${n === 1 ? one : many}`
}

export function fmtCount (n) {
  return Number(n).toLocaleString('en-US')
}

/** `12,345 files · 890.8 MiB · 3.20 s · 278 MiB/s · 3,858 files/s` */
export function summaryLine ({ files, bytes, ms }) {
  const parts = []
  if (Number.isFinite(files)) parts.push(fmtCount(files) + (files === 1 ? ' file' : ' files'))
  if (Number.isFinite(bytes)) parts.push(fmtBytes(bytes))
  if (Number.isFinite(ms)) parts.push(fmtMs(ms))
  if (Number.isFinite(bytes) && Number.isFinite(ms) && ms > 0) parts.push(fmtRate(bytes, ms))
  return parts.join(' · ')
}

/**
 * A batch that lands far below a clean disk's small-file rate is nearly always
 * paying per-file on-access scanning, not doing I/O. Naming the cause beats
 * letting the agent conclude that the tool is slow and start improvising.
 */
export function slowHint (files, ms, scanner = '') {
  if (!Number.isFinite(files) || !Number.isFinite(ms) || files < 200 || ms <= 0) return ''
  if (files / (ms / 1000) >= 1500) return ''
  const budget = perThousand(files, ms)
  const who = scanner ? ` ${scanner} is on the path.` : ''
  return `note: ${budget}. The cost is per-file primitives, not bytes.${who} Fewer file operations (rename a directory, or one archive stream) beat more concurrency, which is already scaled to the cores.`
}
