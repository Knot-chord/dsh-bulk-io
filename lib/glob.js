/**
 * A small glob compiler shared by the Host plugin and its worker threads.
 *
 * Supported syntax (gitignore-flavoured, which is what people expect when they
 * type a pattern at an agent):
 *   *        any run of characters inside one path segment
 *   **       any run of characters, crossing segment boundaries
 *   **\/     zero or more leading segments
 *   ?        exactly one character inside a segment
 *   [abc]    one character out of the set, [a-z] ranges, [!abc] negation
 *   {a,b}    one alternative (one level, no nesting)
 *   path/    a directory prefix, treated as path/**
 *    !pat    negation (only honoured by matcher())
 *   *.ts     a pattern with no slash matches at any depth, like gitignore
 *
 * Matching is case-insensitive, because Windows is, and a model writing
 * `*.TS` should not silently miss its files.
 */

const CACHE = new Map()
const CACHE_MAX = 4096

export function toPosix (value) {
  return String(value).replace(/\\/g, '/')
}

export function compile (pattern) {
  const key = String(pattern)
  const hit = CACHE.get(key)
  if (hit) return hit
  const re = build(key)
  if (CACHE.size >= CACHE_MAX) CACHE.clear()
  CACHE.set(key, re)
  return re
}

function escapeLiteral (ch) {
  return ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function build (pattern) {
  let p = toPosix(pattern).trim().replace(/^\.\//, '')
  if (p === '') return /^$/
  if (!p.includes('/')) p = '**/' + p
  if (p.endsWith('/')) p += '**'

  let out = '^'
  for (let i = 0; i < p.length;) {
    const ch = p[i]
    if (ch === '*') {
      if (p[i + 1] === '*') {
        if (p[i + 2] === '/') {
          out += '(?:.*/)?'
          i += 3
        } else {
          out += '.*'
          i += 2
        }
        continue
      }
      out += '[^/]*'
      i += 1
      continue
    }
    if (ch === '?') {
      out += '[^/]'
      i += 1
      continue
    }
    if (ch === '[') {
      const end = p.indexOf(']', i + 1)
      if (end === -1) {
        out += '\\['
        i += 1
        continue
      }
      let body = p.slice(i + 1, end)
      if (body.startsWith('!')) body = '^' + body.slice(1)
      out += '[' + body.replace(/\\/g, '\\\\') + ']'
      i = end + 1
      continue
    }
    if (ch === '{') {
      const end = p.indexOf('}', i + 1)
      if (end === -1) {
        out += '\\{'
        i += 1
        continue
      }
      const alternatives = p.slice(i + 1, end).split(',').map(escapeLiteral)
      out += '(?:' + alternatives.join('|') + ')'
      i = end + 1
      continue
    }
    if (ch === '/') {
      out += '/'
      i += 1
      continue
    }
    out += escapeLiteral(ch)
    i += 1
  }
  out += '$'
  return new RegExp(out, 'i')
}

/**
 * Build one predicate from a pattern list. Negated patterns always win, so
 * `**\/*.ts, !vendor/**` means "TypeScript files that are not vendored".
 * An empty include list means "everything that is not excluded".
 */
export function matcher (patterns) {
  const include = []
  const exclude = []
  for (const raw of patterns ?? []) {
    const text = String(raw).trim()
    if (text === '') continue
    if (text.startsWith('!')) exclude.push(compile(text.slice(1)))
    else include.push(compile(text))
  }
  if (include.length === 0 && exclude.length === 0) return null
  return (rel) => {
    for (const re of exclude) if (re.test(rel)) return false
    if (include.length === 0) return true
    for (const re of include) if (re.test(rel)) return true
    return false
  }
}
