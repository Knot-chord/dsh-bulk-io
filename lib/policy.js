/**
 * The security boundary, expressed the same way DeepSeek Harness expresses it.
 *
 * Harness already owns file policy: `ctx.sandboxPolicy.resolve({ session })`
 * returns the effective `mode`, the `workspaceRoot` that `workspace-write`
 * means, and any `additionalWritableRoots` (configured plus this session's
 * registrations). A plugin that invents its own policy would let an agent do
 * through `bulk_write` what `write` refuses. So this module reads the Host's
 * policy and enforces exactly it, with one extra structural guard the Host does
 * not need because it writes one file at a time: resolving reparse points, so a
 * junction inside the workspace cannot be used to write outside it.
 *
 * Nothing here is configurable. There is no second source of truth.
 */

import { realpathSync } from 'node:fs'
import { basename, dirname, resolve, sep } from 'node:path'

/** Where a target sits relative to the caller's writable boundary. */
export const BOUNDARY = {
  workspace: 'workspace',
  granted: 'granted',
  outside: 'outside',
}

function norm (path) {
  return String(path).replace(/[\\/]+$/, '').toLowerCase()
}

/** Textual containment, used only where the filesystem cannot add a link. */
export function under (root, target) {
  const r = norm(resolve(root))
  const t = norm(resolve(target))
  if (t === r) return true
  return t.startsWith(r.endsWith(sep) || r.endsWith('/') ? r : r + sep)
}

function realOrNull (path) {
  try {
    return realpathSync.native ? realpathSync.native(path) : realpathSync(path)
  } catch {
    return null
  }
}

/**
 * Resolve as much of a path as exists, then re-attach the missing tail. A tail
 * that does not exist cannot be a link, so this is exact for our purpose: the
 * part that can redirect is always the part that was resolved.
 */
function realDeep (path) {
  const direct = realOrNull(path)
  if (direct) return direct
  const parent = dirname(path)
  if (parent === path) return null
  const up = realDeep(parent)
  if (!up) return null
  return resolve(up, basename(path))
}

/**
 * Resolve the policy that applies to one tool call.
 *
 * `agent.session` is used when the runtime exposes it; otherwise the deployment
 * default applies, which is what `resolve()` returns for an agentless call.
 * A policy that cannot be read is treated as the deployment default rather than
 * as "unrestricted", because failing open on a permission check is worse than
 * being conservative.
 */
export function resolvePolicy (ctx, agent) {
  const service = ctx?.get?.('sandboxPolicy')
  if (!service) {
    // No policy service means "cannot read the policy", not "no policy".
    // Answering danger-full-access here would quietly turn a missing API into
    // unrestricted writes; assuming the workspace still holds is the same
    // conservative answer the catch below gives.
    return {
      mode: 'workspace-write',
      workspaceRoot: process.cwd(),
      additionalWritableRoots: [],
      source: 'no-sandbox-policy-service (assumed workspace-write)',
    }
  }
  const session = agent?.session
  try {
    const policy = service.resolve(session ? { session } : {})
    return {
      mode: policy.mode,
      workspaceRoot: policy.workspaceRoot,
      additionalWritableRoots: [...(policy.additionalWritableRoots ?? [])],
      sessionId: policy.sessionId,
      source: 'sandboxPolicy.resolve',
    }
  } catch {
    return {
      mode: service.defaultMode ?? 'workspace-write',
      workspaceRoot: service.workspaceRoot ?? process.cwd(),
      additionalWritableRoots: [...(service.additionalWritableRoots ?? [])],
      source: 'sandboxPolicy.defaults',
    }
  }
}

export function boundaryOf (policy, target) {
  if (policy.mode === 'danger-full-access') return BOUNDARY.workspace
  if (under(policy.workspaceRoot, target)) return BOUNDARY.workspace
  for (const root of policy.additionalWritableRoots ?? []) {
    if (under(root, target)) return BOUNDARY.granted
  }
  return BOUNDARY.outside
}

export function policyError (message) {
  return Object.assign(new Error(message), { code: 'EPOLICY' })
}

/**
 * Prove that a path is writable under this policy.
 *
 * The path is resolved through the real filesystem whenever it exists, and its
 * parent is resolved when it does not (a path that does not exist cannot be a
 * link). Textual containment alone is not enough: a junction inside the
 * workspace resolves outside while still looking inside, and that case was a
 * real bug during development.
 */
export function proveWritable (policy, target) {
  if (policy.mode === 'read-only') {
    throw policyError(`file policy is read-only: this session cannot modify ${target}`)
  }
  if (policy.mode === 'danger-full-access') return target

  const real = realDeep(target)
  if (real && boundaryOf(policy, real) !== BOUNDARY.outside) return target
  if (real) {
    throw policyError(
      `file policy is workspace-write: ${target} resolves to ${real}, which is outside ` +
      `${policy.workspaceRoot}. A junction cannot be used to write past the boundary.`,
    )
  }
  throw policyError(
    `file policy is workspace-write: ${target} is outside ${policy.workspaceRoot}` +
    ((policy.additionalWritableRoots ?? []).length ? ' and outside every granted root' : '') +
    '. Raise the session permission to danger-full-access to allow it.',
  )
}

/**
 * Guard a write plan with one check per distinct directory instead of one per
 * file. A batch of 100,000 files usually lives in a few hundred directories, so
 * the guard stays O(dirs) while still resolving every directory it will touch.
 *
 * Beyond `maxDirs` the remaining directories are not proven, and the result says
 * `partial: true` so the caller can split the batch. Writes create new files
 * rather than destroying existing ones, so a bounded check is acceptable here;
 * `bulk_delete` stays strict per target.
 */
export function proveWritableDirs (policy, rootAbs, relPaths, { maxDirs = 20_000 } = {}) {
  if (policy.mode === 'danger-full-access') return { checked: 0, partial: false, mode: policy.mode }
  proveWritable(policy, rootAbs)
  if (policy.mode === 'read-only') return { checked: 0, partial: false, mode: policy.mode }

  const dirs = new Set()
  for (const rel of relPaths) {
    if (typeof rel !== 'string' || rel === '') continue
    dirs.add(resolve(rootAbs, dirname(rel)))
    if (dirs.size >= maxDirs) break
  }
  const partial = dirs.size >= maxDirs
  for (const dir of dirs) proveWritable(policy, dir)
  return { checked: dirs.size, partial, mode: policy.mode }
}

/** A one-line description for tool results, so the model sees the boundary. */
export function describePolicy (policy) {
  if (policy.mode === 'danger-full-access') return 'danger-full-access'
  if (policy.mode === 'read-only') return 'read-only (no writes)'
  const extra = (policy.additionalWritableRoots ?? []).length
  return `workspace-write @ ${policy.workspaceRoot}${extra ? ` +${extra} granted root(s)` : ''}`
}
