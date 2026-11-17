/**
 * Functional tests for the bulk-io engine. Run with: npm test
 * Everything happens in a temp directory that is removed afterwards.
 */

import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtempSync, readFileSync, existsSync, mkdirSync, readdirSync, writeFileSync, rmSync, symlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import * as engine from '../lib/engine.js'
import { closePool } from '../lib/pool.js'
import { cancelJob, describeJob, getJob, listJobs, runAsJob, waitForJob } from '../lib/jobs.js'
import { matcher } from '../lib/glob.js'

const root = mkdtempSync(join(tmpdir(), 'bulk-io-test-'))

test.after(() => {
  closePool()
  rmSync(root, { recursive: true, force: true })
})

test('glob: bare patterns match at any depth, ** crosses segments, ! negates', () => {
  assert.equal(matcher(['*.ts'])('a/b/c.ts'), true)
  assert.equal(matcher(['*.ts'])('a/b/c.js'), false)
  assert.equal(matcher(['src/**/*.ts'])('src/c.ts'), true)
  assert.equal(matcher(['src/**/*.ts'])('src/a/b.ts'), true)
  assert.equal(matcher(['src/**/*.ts'])('lib/b.ts'), false)
  assert.equal(matcher(['**/*.ts', '!vendor/**'])('vendor/x.ts'), false)
  assert.equal(matcher(['**/*.ts', '!vendor/**'])('src/x.ts'), true)
  assert.equal(matcher(['{a,b}.txt'])('a.txt'), true)
  assert.equal(matcher(['{a,b}.txt'])('c.txt'), false)
})

test('bulk_write writes a batch and bulk_read reads it back', async () => {
  const dir = join(root, 'rt')
  const files = Array.from({ length: 250 }, (_, i) => ({
    path: `pkg${i % 7}/sub${i % 3}/f${i}.txt`,
    content: `file ${i}\nsecond line ${i}\n`,
  }))
  const wrote = await engine.writeFiles({ root: dir, files })
  assert.equal(wrote.ok, true)
  assert.equal(wrote.files, 250)
  assert.equal(existsSync(join(dir, 'pkg1/sub1/f1.txt')), true)
  assert.equal(readFileSync(join(dir, 'pkg0/sub0/f0.txt'), 'utf8'), 'file 0\nsecond line 0\n')

  const read = await engine.readFiles({ root: dir, include: ['**/*.txt'], maxFiles: 500, maxTotalBytes: 2_000_000 })
  assert.equal(read.files, 250)
  assert.match(read.text, /=== pkg0\/sub0\/f0\.txt/)
  assert.match(read.text, /second line 249/)
})

test('bulk_write reports per-file failures without aborting the batch', async () => {
  const dir = join(root, 'fail')
  const result = await engine.writeFiles({
    root: dir,
    files: [
      { path: 'ok.txt', content: 'fine' },
      { path: 'as-directory/x', content: 'x' },
    ],
  })
  // The portable failure: a path segment that is already a file.
  await engine.writeFiles({ root: dir, files: [{ path: 'blocker', content: 'x' }] })
  const second = await engine.writeFiles({
    root: dir,
    files: [
      { path: 'still-ok.txt', content: 'fine' },
      { path: 'blocker/under.txt', content: 'x' },
    ],
  })
  assert.equal(existsSync(join(dir, 'still-ok.txt')), true)
  assert.equal(second.ok, false)
  assert.equal(second.failed.length, 1)
  assert.equal(second.failed[0].path, 'blocker/under.txt')
  assert.equal(result.ok, true)
})

test('bulk_read bounds output, filters with grep, and marks truncation', async () => {
  const dir = join(root, 'grep')
  await engine.writeFiles({
    root: dir,
    files: [
      { path: 'a.log', content: 'alpha\nneedle one\nomega\n' },
      { path: 'b.log', content: 'beta\nnothing\n' },
      { path: 'c.log', content: 'x'.repeat(5000) },
    ],
  })

  const filtered = await engine.readFiles({ root: dir, include: ['*.log'], grep: 'needle' })
  assert.equal(filtered.files, 1)
  assert.match(filtered.text, /needle one/)
  assert.doesNotMatch(filtered.text, /beta/)

  const capped = await engine.readFiles({ root: dir, include: ['*.log'], maxBytesPerFile: 100 })
  assert.match(capped.text, /first 100 B/)

  const tiny = await engine.readFiles({ root: dir, include: ['*.log'], maxTotalBytes: 30 })
  assert.ok(tiny.dropped > 0)
})

test('bulk_read skips binaries unless asked', async () => {
  const dir = join(root, 'bin')
  await engine.writeFiles({
    root: dir,
    files: [
      { path: 'data.bin', content: Buffer.from([0, 1, 2, 3, 0, 4]).toString('base64'), base64: true },
      { path: 'text.txt', content: 'hello' },
    ],
  })
  const skipped = await engine.readFiles({ root: dir, include: ['*'] })
  assert.match(skipped.text, /binary .*skipped/)
  assert.match(skipped.text, /hello/)

  const encoded = await engine.readFiles({ root: dir, include: ['*.bin'], binary: 'base64' })
  assert.match(encoded.text, /AAECAwAE/)
})

test('bulk_transfer copies a tree and moves it back by rename', async () => {
  const src = join(root, 'copy-src')
  await engine.writeFiles({
    root: src,
    files: Array.from({ length: 60 }, (_, i) => ({ path: `d${i % 4}/f${i}.txt`, content: 'x'.repeat(128) })),
  })

  const dest = join(root, 'copy-dest')
  const copied = await engine.transferFiles({ op: 'copy', sources: [src], dest })
  assert.equal(copied.ok, true)
  assert.equal(copied.strategy, 'threads')
  assert.equal(copied.files, 60)
  assert.equal(readFileSync(join(dest, 'copy-src/d1/f1.txt'), 'utf8'), 'x'.repeat(128))

  const movedDest = join(root, 'move-dest')
  const moved = await engine.transferFiles({ op: 'move', sources: [join(dest, 'copy-src')], dest: movedDest })
  assert.equal(moved.strategy, 'rename')
  assert.equal(existsSync(join(dest, 'copy-src')), false)
  assert.equal(readFileSync(join(movedDest, 'copy-src/d2/f2.txt'), 'utf8'), 'x'.repeat(128))
})

test('bulk_transfer expands globs and keeps relative paths', async () => {
  const src = join(root, 'glob-src')
  await engine.writeFiles({
    root: src,
    files: [
      { path: 'deep/a/keep.ts', content: 'ts' },
      { path: 'deep/b/skip.js', content: 'js' },
    ],
  })
  const dest = join(root, 'glob-dest')
  const result = await engine.transferFiles({ op: 'copy', sources: [join(src, 'deep/**/*.ts')], dest })
  assert.equal(result.files, 1)
  assert.equal(existsSync(join(dest, 'a/keep.ts')), true)
  assert.equal(existsSync(join(dest, 'b/skip.js')), false)
})

test('bulk_delete parks files in the trash and the move can be undone', async () => {
  const dir = join(root, 'trash')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 20 }, (_, i) => ({ path: `tmp/f${i}.txt`, content: 'gone soon' })),
  })

  const removed = await engine.deleteFiles({ root: dir, include: ['tmp/**'] })
  assert.equal(removed.mode, 'trash')
  assert.equal(removed.files, 20)
  assert.equal(existsSync(join(dir, 'tmp/f0.txt')), false)
  assert.ok(removed.trashPath.includes('.dsh-trash'))
  assert.equal(existsSync(join(removed.trashPath, 'tmp/f0.txt')), true)
  assert.equal(existsSync(join(removed.trashPath, 'manifest.json')), true)

  const restored = await engine.transferFiles({
    op: 'move',
    sources: [join(removed.trashPath, 'tmp')],
    dest: dir,
  })
  assert.equal(restored.ok, true)
  assert.equal(existsSync(join(dir, 'tmp/f0.txt')), true)
})

test('bulk_delete refuses targets outside root and the root itself', async () => {
  const dir = join(root, 'guard')
  mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, 'keep.txt'), 'keep')
  await assert.rejects(
    () => engine.deleteFiles({ root: dir, paths: ['../outside.txt'], permanent: true }),
    /outside root/,
  )
  await assert.rejects(
    () => engine.deleteFiles({ root: dir, paths: ['.'], permanent: true }),
    /root itself/,
  )
  assert.equal(existsSync(join(dir, 'keep.txt')), true)
})

test('scan never descends into the trash directory', async () => {
  const dir = join(root, 'scan')
  await engine.writeFiles({ root: dir, files: [{ path: 'a.txt', content: 'a' }] })
  await engine.writeFiles({ root: dir, files: [{ path: '.dsh-trash/old/a.txt', content: 'a' }] })
  const scan = await engine.scanTree({ root: dir, include: ['**/*.txt'] })
  assert.deepEqual(scan.files.map((f) => f.path), ['a.txt'])
})

test('quarantine renames in place and frees the path immediately', async () => {
  const dir = join(root, 'quarantine')
  await engine.writeFiles({ root: dir, files: [{ path: 'big/a.txt', content: 'a' }] })
  const result = await engine.deleteFiles({ root: dir, paths: ['big'], quarantine: true })
  assert.equal(result.mode, 'quarantine')
  assert.equal(result.files, 1)
  assert.equal(existsSync(join(dir, 'big')), false)
  assert.equal(existsSync(join(dir, 'big/a.txt')), false)
  assert.ok(readdirSync(dir).some((name) => name.startsWith('big.old-')))
})

function makeJunction (linkPath, targetPath) {
  if (process.platform !== 'win32') return false
  const result = spawnSync('cmd', ['/c', 'mklink', '/J', linkPath, targetPath], { windowsHide: true })
  return result.status === 0 && result.error === undefined
}

test('bulk_delete refuses a path that escapes root through a junction', async (t) => {
  const base = join(root, 'junction-escape')
  const outside = join(root, 'junction-outside')
  mkdirSync(join(base, 'parent'), { recursive: true })
  mkdirSync(outside, { recursive: true })
  writeFileSync(join(outside, 'keep.txt'), 'important')
  if (!makeJunction(join(base, 'parent', 'link'), outside)) {
    t.skip('junctions unavailable')
    return
  }
  assert.equal(existsSync(join(base, 'parent/link/keep.txt')), true)

  await assert.rejects(
    () => engine.deleteFiles({ root: base, paths: ['parent/link/keep.txt'], permanent: true }),
    /resolves to|outside root|cannot prove/,
  )
  assert.equal(existsSync(join(outside, 'keep.txt')), true, 'file outside root must survive')

  // A glob scan must not walk through the link either.
  const scan = await engine.scanTree({ root: base, include: ['**/*.txt'] })
  assert.deepEqual(scan.files.map((f) => f.path), [])
})

test('deleting a tree removes the junction entry, never its target', async (t) => {
  const base = join(root, 'junction-delete')
  const outside = join(root, 'junction-target')
  mkdirSync(base, { recursive: true })
  mkdirSync(outside, { recursive: true })
  writeFileSync(join(outside, 'keep.txt'), 'important')
  if (!makeJunction(join(base, 'link'), outside)) {
    t.skip('junctions unavailable')
    return
  }
  const result = await engine.deleteFiles({ root: base, paths: ['link'], permanent: true })
  assert.equal(result.ok, true)
  assert.equal(existsSync(join(base, 'link')), false)
  assert.equal(existsSync(join(outside, 'keep.txt')), true, 'junction target must survive')
})

/* --------------------------------------------------------------- file policy */

test('a workspace root written another way still contains its own files', async () => {
  // The root is not always spelled the way the filesystem does: a hosted runner's
  // TEMP is C:\Users\RUNNER~1\... while realpath answers C:\Users\runneradmin\...
  // A junction reproduces the same shape here: root given as the alias, target
  // resolved to the real directory. Comparing a raw root against a resolved
  // target refuses the workspace's own files, which is what turned CI red.
  const real = join(root, 'alias-real')
  const alias = join(root, 'alias-link')
  mkdirSync(real, { recursive: true })
  symlinkSync(real, alias, 'junction')

  const policy = workspacePolicy(alias)
  const written = await engine.writeFiles({
    root: join(alias, 'deep', 'nested'),
    files: [{ path: 'a.txt', content: 'a' }],
    policy,
  })
  assert.equal(written.ok, true)
  assert.equal(existsSync(join(real, 'deep', 'nested', 'a.txt')), true)
})

function workspacePolicy (workspaceRoot, mode = 'workspace-write') {
  return { mode, workspaceRoot, additionalWritableRoots: [], source: 'test' }
}

test('workspace-write policy allows the workspace and refuses everywhere else', async () => {
  const ws = join(root, 'policy-ws')
  const outside = join(root, 'policy-outside')
  mkdirSync(ws, { recursive: true })
  mkdirSync(outside, { recursive: true })
  const policy = workspacePolicy(ws)

  const inside = await engine.writeFiles({ root: join(ws, 'deep/nested'), files: [{ path: 'a.txt', content: 'a' }], policy })
  assert.equal(inside.ok, true)

  await assert.rejects(
    () => engine.writeFiles({ root: outside, files: [{ path: 'b.txt', content: 'b' }], policy }),
    /outside .*policy-ws|workspace-write/,
  )
  assert.equal(existsSync(join(outside, 'b.txt')), false)

  await assert.rejects(
    () => engine.transferFiles({ op: 'copy', sources: [join(ws, 'deep')], dest: outside, policy }),
    /workspace-write|outside/,
  )

  await assert.rejects(
    () => engine.deleteFiles({ root: outside, include: ['**/*.txt'], policy }),
    /workspace-write|outside/,
  )
})

test('read-only policy refuses every mutation', async () => {
  const dir = join(root, 'policy-read-only')
  mkdirSync(dir, { recursive: true })
  const policy = workspacePolicy(dir, 'read-only')
  await assert.rejects(
    () => engine.writeFiles({ root: dir, files: [{ path: 'x.txt', content: 'x' }], policy }),
    /read-only/,
  )
  await assert.rejects(
    () => engine.deleteFiles({ root: dir, include: ['**/*'], policy }),
    /read-only/,
  )
  const read = await engine.readFiles({ root: dir, include: ['**/*'] })
  assert.equal(read.files, 0)
})

test('danger-full-access policy keeps writes allowed everywhere', async () => {
  const outside = join(root, 'policy-open')
  const policy = workspacePolicy(join(root, 'some-workspace'), 'danger-full-access')
  const result = await engine.writeFiles({ root: outside, files: [{ path: 'ok.txt', content: 'ok' }], policy })
  assert.equal(result.ok, true)
  assert.equal(existsSync(join(outside, 'ok.txt')), true)
})

test('workspace-write refuses a write that reaches outside through a junction', async (t) => {
  const ws = join(root, 'policy-link-ws')
  const outside = join(root, 'policy-link-outside')
  mkdirSync(ws, { recursive: true })
  mkdirSync(outside, { recursive: true })
  if (!makeJunction(join(ws, 'link'), outside)) {
    t.skip('junctions unavailable')
    return
  }
  const policy = workspacePolicy(ws)
  await assert.rejects(
    () => engine.writeFiles({ root: ws, files: [{ path: 'link/escaped.txt', content: 'x' }], policy }),
    /workspace-write|outside/,
  )
  assert.equal(existsSync(join(outside, 'escaped.txt')), false)
})

/* ------------------------------------------------------------------ scan stats */

test('bulk_scan aggregates a tree instead of listing it', async () => {  const dir = join(root, 'stats')
  await engine.writeFiles({
    root: dir,
    files: [
      { path: 'a/one.ts', content: 'x'.repeat(100) },
      { path: 'a/two.ts', content: 'x'.repeat(200) },
      { path: 'b/three.js', content: 'x'.repeat(300) },
      { path: 'b/readme.md', content: '' },
    ],
  })
  const stats = await engine.scanStats({ root: dir, topN: 3 })
  assert.equal(stats.files, 4)
  assert.equal(stats.bytes, 600)
  assert.equal(stats.byExtension.ts.count, 2)
  assert.equal(stats.byExtension.js.bytes, 300)
  assert.equal(stats.largest[0].path, 'b/three.js')
  assert.equal(stats.largest[0].size, 300)
  assert.match(stats.text, /4 files/)

  const counted = await engine.scanStats({ root: dir, withSize: false, topN: 0 })
  assert.equal(counted.files, 4)
  assert.equal(counted.bytes, 0)
})

test('permanent delete of a directory expands into subtrees and reports progress', async () => {
  const dir = join(root, 'expand')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 80 }, (_, i) => ({ path: `big/sub${i % 8}/f${i}.txt`, content: 'x' })),
  })
  const events = []
  const result = await engine.deleteFiles({
    root: dir,
    paths: ['big'],
    permanent: true,
    onProgress: (snapshot) => events.push(snapshot),
  })
  assert.equal(result.ok, true)
  assert.equal(existsSync(join(dir, 'big')), false)
  assert.ok(events.length >= 1, 'progress must fire during a directory delete')
  assert.ok(events.some((event) => event.files > 0))
})

test('permanent delete descends a single-entry chain before splitting', async () => {
  // Shape: wrap / src / d0..d19 — the target itself has one entry. A planner
  // that treats a single-entry directory as a unit deletes the whole thing on
  // one worker, which is both slow and silent; descending first is what makes
  // it twenty units across sixteen slices.
  const dir = join(root, 'chain')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 100 }, (_, i) => ({ path: `wrap/src/d${i % 20}/f${i}.txt`, content: 'x' })),
  })
  const events = []
  const result = await engine.deleteFiles({
    root: dir,
    paths: ['wrap'],
    permanent: true,
    onProgress: (snapshot) => events.push(snapshot),
  })
  assert.equal(result.ok, true)
  assert.equal(existsSync(join(dir, 'wrap')), false)
  const deleting = events.filter((event) => event.phase === 'deleting')
  assert.ok(deleting.length >= 2, `expected several delete batches, saw ${deleting.length}`)
  // The denominator is the 100 files, not the 21 directories that contain them.
  assert.equal(events[0].totalFiles, 100)
  assert.equal(result.files, 100)
})

test('permanent delete counts files, not pieces of work', async () => {
  const dir = join(root, 'weighted')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 120 }, (_, i) => ({ path: `big/s${i % 6}/f${i}.txt`, content: 'x' })),
  })
  const events = []
  const result = await engine.deleteFiles({
    root: dir,
    paths: ['big'],
    permanent: true,
    onProgress: (snapshot) => events.push(snapshot),
  })
  assert.equal(result.ok, true)
  assert.equal(result.files, 120, 'progress must be in files, not subtrees')
  assert.equal(events[0].totalFiles, 120)
  assert.equal(Math.max(...events.map((event) => event.files)), 120)
})

test('a delete job settles shortly after cancel', async () => {
  const dir = join(root, 'cancel-delete')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 600 }, (_, i) => ({ path: `big/s${i % 20}/f${i}.txt`, content: 'x' })),
  })
  const job = runAsJob('test', 'cancel delete', (onProgress, signal) => engine.deleteFiles({
    root: dir,
    paths: ['big'],
    permanent: true,
    onProgress,
    signal,
  }))
  await new Promise((resolve) => setTimeout(resolve, 15))
  cancelJob(job.id)
  // settleOnly: a snapshot as soon as anything changes would be useless here —
  // the assertion is about the job actually reaching a terminal state.
  const settled = await waitForJob(job.id, 20_000, { settleOnly: true })
  assert.ok(['cancelled', 'done'].includes(settled.state), `unexpected state ${settled.state}`)
})

test('waitForJob returns when progress advances instead of waiting the full budget', async () => {
  const job = runAsJob('test', 'slow progress', async (onProgress) => {
    for (let i = 1; i <= 20; i++) {
      await new Promise((resolve) => setTimeout(resolve, 80))
      onProgress({ files: i * 5, phase: 'working' })
    }
    return { ok: true, files: 100, text: 'done' }
  })
  const started = Date.now()
  const snapshot = await waitForJob(job.id, 10_000, { minWaitMs: 300 })
  const elapsed = Date.now() - started
  assert.ok(elapsed < 3000, `poll returned after ${elapsed} ms, expected well under the 10 s budget`)
  assert.ok(snapshot.files > 0)
})

test('the transfer estimate counts files only, not directories', async () => {
  const dir = join(root, 'estimate')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 12 }, (_, i) => ({ path: `a/b/f${i}.txt`, content: 'x' })),
  })
  const filesOnly = engine.estimateEntries([dir], { filesOnly: true })
  const everything = engine.estimateEntries([dir])
  assert.equal(filesOnly, 12)
  assert.ok(everything > filesOnly, `directories must not inflate the file total (${everything} vs ${filesOnly})`)
})

test('a same-volume move is recognised before anything is counted', async () => {
  assert.equal(engine.willRename('move', ['D:/a/b'], 'D:/a/c'), true)
  assert.equal(engine.willRename('copy', ['D:/a/b'], 'D:/a/c'), false)
  assert.equal(engine.willRename('move', ['D:/a'], 'C:/a'), false)
  assert.equal(engine.willRename('move', [], 'C:/a'), false)
})

test('a tree can be rebuilt in the same place after it is deleted', async () => {
  // The worker caches created directories. A delete removes them; without cache
  // invalidation the next write into the same path fails with ENOENT for every
  // file while the cache still claims the directory exists.
  const dir = join(root, 'rebuild')
  const files = Array.from({ length: 40 }, (_, i) => ({ path: `d${i % 4}/f${i}.txt`, content: 'first' }))
  const first = await engine.writeFiles({ root: dir, files })
  assert.equal(first.ok, true)

  const removed = await engine.deleteFiles({ root: dir, paths: ['d0', 'd1'], permanent: true })
  assert.equal(removed.ok, true)

  const second = await engine.writeFiles({ root: dir, files })
  assert.equal(second.ok, true, `rebuild failed: ${JSON.stringify(second.failed?.[0] ?? {})}`)
  assert.equal(second.files, 40)
  assert.equal(existsSync(join(dir, 'd0/f0.txt')), true)
})

test('the delete planner proves its own plan, and the check rejects bad ones', async () => {
  const dir = join(root, 'verify-plan')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 80 }, (_, i) => ({ path: `wrap/src/d${i % 8}/f${i}.txt`, content: 'x' })),
  })

  const plan = await engine.planDeletePaths({ root: dir, paths: ['wrap'] })
  assert.deepEqual(engine.verifyPlan(plan), [])
  assert.ok(plan.total > 0)

  const overlapping = engine.verifyPlan({
    leaves: [{ path: join(root, 'a', 'b'), weight: 1 }, { path: join(root, 'a', 'b', 'c'), weight: 1 }],
    roots: [],
    total: 2,
  })
  assert.ok(overlapping.some((problem) => problem.includes('overlapping')), JSON.stringify(overlapping))

  const mismatched = engine.verifyPlan({ leaves: [{ path: '/x', weight: 1 }], roots: [], total: 5 })
  assert.ok(mismatched.some((problem) => problem.includes('total')))

  const duplicated = engine.verifyPlan({
    leaves: [{ path: '/y', weight: 1 }, { path: '/y', weight: 1 }],
    roots: [],
    total: 2,
  })
  assert.ok(duplicated.some((problem) => problem.includes('duplicate')))
})

test('a paced run stops its timers when it finishes', async () => {
  // setInterval returns a Timeout, which has no .clear(); a `timer?.clear?.()`
  // cleanup is a no-op that leaves the progress timer firing for the life of
  // the process.
  const src = join(root, 'timers', 'src')
  const dst = join(root, 'timers', 'dst')
  await engine.writeFiles({
    root: src,
    files: Array.from({ length: 400 }, (_, i) => ({ path: `d${i % 8}/f${i}.txt`, content: 'x'.repeat(200) })),
  })

  let calls = 0
  await engine.transferFiles({ op: 'copy', sources: [src], dest: dst, onProgress: () => { calls += 1 } })
  assert.ok(calls > 0, 'the transfer never reported progress')

  const settledAt = calls
  await new Promise((resolve) => setTimeout(resolve, 1500))
  assert.equal(calls, settledAt, 'progress kept firing after the transfer finished')
})

/* ------------------------------------------------------------- progress */

test('a background job reports progress, then exposes its own result', async () => {
  const dir = join(root, 'job-src')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 120 }, (_, i) => ({ path: `d${i % 4}/f${i}.txt`, content: 'x'.repeat(64) })),
  })

  const job = runAsJob('test', 'copy a small tree', (onProgress, signal) => engine.transferFiles({
    op: 'copy',
    sources: [dir],
    dest: join(root, 'job-out'),
    onProgress,
    signal,
  }))

  const settled = await waitForJob(job.id, 30_000)
  assert.equal(settled.state, 'done')
  assert.equal(settled.files, 120)
  assert.ok(settled.elapsedMs >= 0)
  assert.match(describeJob(settled), /^\[done\]/)
  assert.equal(getJob(job.id).result.ok, true)
  assert.equal(existsSync(join(root, 'job-out', 'job-src', 'd1', 'f1.txt')), true)
})

test('a background job can be cancelled', async () => {
  const dir = join(root, 'cancel-src')
  await engine.writeFiles({
    root: dir,
    files: Array.from({ length: 400 }, (_, i) => ({ path: `d${i % 8}/f${i}.txt`, content: 'x'.repeat(64) })),
  })
  const job = runAsJob('test', 'cancellable copy', (onProgress, signal) => engine.transferFiles({
    op: 'copy',
    sources: [dir],
    dest: join(root, 'cancel-out'),
    onProgress,
    signal,
  }))
  cancelJob(job.id)
  const settled = await waitForJob(job.id, 30_000)
  assert.equal(settled.state, 'cancelled')
  assert.equal(listJobs(5).some((entry) => entry.id === job.id), true)
})
