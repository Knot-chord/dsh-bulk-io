# 大量小文件读写优化

Five tools that make an agent fast on many small files: one call instead of one call
per file, without widening what the session is allowed to do.

## The tools

**bulk_write** takes an array of `{ path, content, base64?, append? }` and writes
them all: directories created once, per-file failures collected instead of
aborting, `atomic` writes through a rename, `dryRun` reports the plan.

**bulk_read** takes a path list or `include`/`exclude` globs and returns the files
inside a bounded output budget. Every block header states whether the file was
truncated, so the agent always knows what it did not see.

**bulk_scan** summarises a tree — count, bytes, extension histogram, largest
members — in one pass with bounded memory, which is what makes a 200,000-file
directory answerable at all.

**bulk_transfer** copies, moves or syncs. A move inside one volume is a rename;
a whole-tree copy can use `robocopy /MT` or one `tar | zstd` stream; everything
else runs as parallel native copies, with copy-on-write clones where the volume
supports them.

**bulk_delete** moves targets into `<root>/.dsh-trash/<timestamp>/` with a
manifest and states the exact call that restores them. Passing a **directory**
costs one rename no matter how many files are inside. `quarantine: true` renames
entries to `<name>.old-<stamp>` in place and returns immediately.

## The boundary is the Host's, not a second one

Every call resolves `ctx.sandboxPolicy.resolve({ session })` and enforces that
mode: `read-only` refuses all mutation, `workspace-write` refuses anything outside
the workspace root and its granted roots, `danger-full-access` allows the rest.
The tools are a narrowing of what the session may do, never a widening.

On top of that, paths are **resolved** rather than compared as strings, because a
junction inside the workspace can point outside it — a real bug found while
testing, in both the write and the delete paths. `bulk_delete` additionally
refuses a target it cannot prove inside its root, the root itself, and any volume
root, and never follows a link into its target.

## Where the time goes

Measured on Windows 11 / NTFS with an on-access scanner active: `stat` 100–140 µs,
read-open 127–146 µs, unlink 243 µs, **write-open 440–630 µs**, rename ~700 µs,
one directory rename ~2 ms regardless of file count, 1 MiB write ~1 ms.

So below ~64 KiB the cost is flat and per-file — an empty file costs the same as
a 64 KiB one — and a directory rename is O(1). Moving 1,500 files one at a time
took 1,045 ms; renaming the directory took 2 ms. That single fact drives the
whole design: the cheapest file operation is the one that does not happen.

## Zero configuration

Concurrency defaults to 8, the peak of the measured scaling curve (4→6,591,
8→7,247, 16→6,331 files/s). Volume topology, copy-on-write support and the active
scanner are probed and cached at runtime; the policy comes from the Host. A
degraded environment — no worker threads, no `tar`, no `robocopy`, unreadable
policy — falls back to a portable path rather than failing the call.

## Also usable from pwsh 7

`lib/cli.mjs` and `bin/dsh-bulk.ps1` run the same engine. One spawn costs 170 ms
(`node -e`) to 490 ms (`pwsh -Command`), so a per-call interpreter is only worth
it for one large batch; inside DSH the tools are in-process and pay nothing.
