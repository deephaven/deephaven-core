---
name: deephaven-documentation-needs-check
description: >
  Analyze everything in main since the latest release tag and determine what needs documentation
  for the upcoming release. Diffs main against the most recent vX.Y tag, filters out internal-only
  changes (tests, build, refactoring, style, docs-only), and triages every user-facing change —
  new public APIs, changed behavior, new configuration properties, new or changed gRPC services,
  Python binding additions, new extensions or integrations — into a prioritized report of what
  needs documenting and where. **Use this skill when:** preparing release documentation, asking
  "what needs docs for the next release?", triaging documentation work for an upcoming release,
  or reviewing what's undocumented since the last release. **Do NOT use for:** reviewing existing
  documentation (use deephaven-docs-review-full), writing documentation, checking a single PR
  (just read the PR diff), or Enterprise/deephaven-ent code.
allowed-tools: Read, Grep, Glob, Bash(git diff *), Bash(git log *), Bash(git show *), Bash(git tag *), Bash(git fetch *), Bash(git grep *), Bash(find *), Bash(wc *)
---

# Release documentation needs check

Analyze everything that has landed on `upstream/main` since the latest release and determine
what needs documentation for the upcoming release. All commands use `upstream/main` (the fetched
remote ref) rather than the local `main` branch, so the triage is always against the latest
upstream state regardless of the local checkout.

## 1. Find the latest release tag and build the change set

Release tags follow the pattern `vX.Y` (e.g., `v42.5`, `v41.10`).

### Ensure local state is current

Before anything else, fetch tags and `main` from upstream so the triage reflects what has
actually shipped and landed, not a stale local snapshot:

```bash
git fetch upstream --tags
git fetch upstream main
```

If `upstream` is not configured or the fetch fails, stop and tell the user — an outdated local
checkout can produce an empty or incomplete report.

**Use `upstream/main` — not bare `main` — in every command below.** The local `main` branch may
be behind `upstream/main` after a fetch, so bare `main` can silently omit newly merged features
and read outdated documentation.

### Find the latest release tag

Use `--sort=-v:refname` for correct version ordering (lexical sorting treats `v42.9` as newer
than `v42.10`):

```bash
git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+$' | head -1
```

Then get the full picture of what's changed since that tag:

```bash
# Total commit count for context
git log <tag>..upstream/main --oneline --first-parent | wc -l

# All changed files, grouped
git diff --name-status <tag>..upstream/main

# All first-parent commits (one per PR, including squash-merged and rebased PRs)
git log <tag>..upstream/main --oneline --first-parent
```

Use `--first-parent` — not `--merges` — so squash-merged PRs, rebased PRs, and direct commits
are included. `--merges` would silently skip any PR that was squash-merged or rebased onto main.

The commit titles are the primary triage input — each represents a PR or direct commit that
landed, and the Conventional Commits prefix (`feat:`, `fix:`, `perf:`, `chore:`, `docs:`,
`test:`, `refactor:`) tells you its intent. The `--name-status` diff tells you what files
actually changed.

### Filter out changes already shipped via cherry-picks

Minor releases cherry-pick fixes from `main` onto a release branch (see `RELEASE.md`). These
cherry-picks create new SHAs on the release branch, so the original commits on `main` still
appear in `<tag>..upstream/main` even though their changes already shipped. This applies to fixes
cherry-picked into the selected tag itself as well as any later minor releases.

Check for patch-equivalent commits by comparing diff content (not SHAs) between `main` commits
and what shipped in each release. Use `--sort=v:refname` for correct version ordering — lexical
comparison treats `v42.9` as newer than `v42.10`:

```bash
# List all release tags in the same major series, version-sorted
git tag --sort=v:refname | grep -E '^v<major>\.[0-9]+$'
```

For each commit in `<tag>..upstream/main`, get its first-parent diff (`git diff <sha>^..<sha>`) — not
`git show`, which produces an empty combined diff for clean merges and would falsely match other
empty diffs. Only compare commits whose first-parent diff is non-empty; a commit with an empty
first-parent diff has no changes to exclude. If a non-empty first-parent diff matches the
first-parent diff of any commit reachable from a release tag (including the selected base tag
and any later minor releases), exclude it from the triage — it was already released and any
documentation for it belongs to that earlier release.

## 2. First pass: triage by commit prefix

Sort every first-parent commit into one of three buckets based on its prefix and title. This
is a preliminary sort — **not a final exclusion**. Every bucket, including "likely no docs,"
gets diff inspection in step 3 before anything is definitively excluded.

### Likely needs documentation

- **`feat:`** — New features almost always need docs. Read the title to understand what was added.
- **`fix:`** with user-visible behavior change — A fix that changes documented behavior or error
  conditions needs a doc update. A fix that makes code match existing docs does not.
- **`perf:`** with new configuration knobs — Performance work that adds user-facing config
  properties or changes defaults needs docs. Pure internal speedups do not.

### Likely does NOT need documentation

These are *preliminary* classifications. Each still gets diff inspection in step 3 — a prefix
alone cannot rule out documentation needs. For example, a `chore:` dependency upgrade can change
deployment requirements, and a `refactor:` can rename a public API.

- **`docs:`** — Already documentation changes.
- **`chore:`** — Build, dependency, CI, tooling changes — but inspect the diff for user-facing
  side effects (runtime/dependency changes that affect deployment, new system requirements).
- **`test:`** — Test-only changes.
- **`refactor:`** — Internal restructuring — but inspect the diff to confirm no public API
  surface changed.
- **`fix:`** where code now matches existing docs — Bug fix that corrects behavior to what docs
  already say.
- **`perf:`** with no new config or changed defaults — Pure internal optimization.

### Needs closer inspection

- **`feat:`** that might be internal-only (new test infrastructure, build tooling).
- **`fix:`** where it's unclear if docs described the buggy or correct behavior.
- Any commit whose title doesn't clearly indicate scope.

## 3. Second pass: analyze user-facing changes in detail

For every commit in **all three buckets** — including "likely does NOT need documentation" —
examine the actual diff to determine the documentation impact. The prefix-based triage is a
sorting heuristic, not a filter. A commit must be confirmed internal-only by its diff before
it can be excluded from the report.

### What makes a change user-facing

| Change type | Where to look | Documentation impact |
|---|---|---|
| **New or changed public Java API** | `engine/api/`, `engine/table/`, `table-api/`, `qst/`, classes without `@InternalUse` or package-private | New methods, changed signatures, changed semantics, new classes |
| **New or changed Python API** | `py/server/deephaven/`, public functions/classes in `__init__.py` or exported modules | New functions, changed parameters, changed behavior |
| **New or changed Groovy API** | `Integrations/src/.../groovy/`, server console wiring | New bindings, changed script-session behavior |
| **New or changed gRPC services** | `proto/proto-backplane-grpc/`, `server/src/.../server/` | New RPCs, changed request/response shapes, new services |
| **New or changed configuration properties** | `props/`, `Configuration/`, `*Configuration.java`, system properties parsed from `-D` flags | New properties, changed defaults, removed properties |
| **New or changed server behavior** | `server/`, `server-jetty-app/`, authentication/authorization changes | Startup behavior, auth flows, deployment changes |
| **New or changed extensions/integrations** | `extensions/*/`, new connectors (Parquet, Kafka, Iceberg, S3, CSV, JSON, JDBC, Arrow) | New data source support, changed import/export behavior |
| **New or changed client APIs** | `java-client/`, `py/client/`, `cpp-client/`, `R/`, `web/client-api/` | New client methods, changed connection behavior |
| **New or changed table operations** | `engine/table/impl/` (joins, aggregations, filters, update-by, sort) | New operations, changed operation semantics |
| **New or changed SQL support** | `engine/sql/`, `sql/` | New SQL syntax, changed query behavior |
| **UI changes** | `web/client-ui/`, `web/client-api/`, plugin UI | New UI features, changed workflows |
| **New or changed Docker/deployment** | `docker/`, `server-jetty-app/`, Gradle application tasks | Changed deployment steps, new images, new startup flags |

### What makes a change internal-only

| Change type | Where to look | Why no docs |
|---|---|---|
| **Test-only changes** | `**/src/test/**`, `*Test.java`, `*TestBase.java` | Not user-facing |
| **Build/CI changes** | `buildSrc/`, `*.gradle`, `.github/workflows/`, `settings.gradle` | Build infrastructure |
| **Code style/formatting** | Spotless changes, whitespace, import ordering | No behavioral change |
| **Internal refactoring** | Package-private classes, `@InternalUse` classes, unchanged public API | No user-visible change |
| **Performance-only** | Same API, same behavior, faster execution | No docs unless new config knobs or changed defaults |
| **Dependency updates** | `gradle/libs.versions.toml`, version bumps | Unless a user-facing dependency changed its API |
| **Liveness/ref-counting internals** | `engine/liveness/`, `ReferenceCounted` changes | Engine internal |
| **UpdateGraph internals** | Notification ordering, cycle mechanics | Engine internal unless new user-facing config |
| **Documentation-only changes** | `docs/`, `.devin/rules/`, `.claude/skills/` | Already documentation or tooling |

### For each user-facing change, determine:

**Public API changes:**
- Is the class/method/function public? Check access modifiers, `__init__.py` exports, `@InternalUse`.
- Is it new, modified, or removed?
  - New: needs documentation of what it does, parameters, return type, and at least one example.
  - Modified signature: needs updated parameter docs.
  - Modified behavior (same signature): needs updated description.
  - Removed/deprecated: needs deprecation notice or migration guidance.
- Is it a breaking change? Renamed method, removed parameter, or changed default needs a migration note.

**Configuration changes:**
- New property: needs documentation of name, type, default, and what it controls.
- Changed default: needs updated docs showing the new default and rationale.
- Removed property: needs migration guidance.

**Behavioral changes (even with identical API surface):**
- Changed error messages or error conditions
- Changed ordering guarantees
- Changed null/empty handling
- New warnings or logging visible to users
- Changed default thread counts, batch sizes, or timeouts

## 4. Check what's already documented

For each change that needs docs, check whether documentation on `upstream/main` already covers
it or needs updating. Search documentation **at the `upstream/main` revision**, not the working
tree — running this skill from a documentation branch or an older checkout would otherwise
produce wrong results (marking unmerged doc work as "already documented," or missing docs that
are already on `upstream/main`).

```bash
# Search for existing mentions of the feature/class/method in docs on upstream/main
git grep -l "featureName\|ClassName\|method_name" upstream/main -- 'docs/**/*.md'

# Read a specific doc page at the upstream/main revision
git show upstream/main:docs/python/path/to/page.md
```

Also check if any `docs:` commits in the same release window already documented the feature.
A `feat:` commit followed by a `docs:` commit covering the same feature can be marked as
already handled.

Classify each item:
- **Already documented** — a `docs:` commit in this release window covers it, or existing docs
  on `upstream/main` already describe the new behavior accurately.
- **Needs a new page** — entirely new feature or concept with no existing coverage.
- **Needs updates to existing pages** — changed behavior, new parameters, new options on an
  existing feature.
- **Needs updates to examples** — existing examples are now wrong or incomplete.
- **Covered by generated API reference** — Javadoc/PyDoc changes that the generated reference
  will pick up automatically; no prose doc changes needed unless the feature is complex enough
  to warrant a how-to or concept guide.

## 5. Write the report

Structure the output as:

```
## Release documentation triage: <tag>..upstream/main

**Release tag:** <tag>
**Commits since release (first-parent):** <count>

### Documentation items (action needed)

Group by priority. For each item:

#### [High | Medium | Low] — <short description>

- **PR(s):** <commit title(s) and PR number(s)>
- **What changed:** <specific description of the user-facing change>
- **Where in docs:** <existing page to update, or "new page needed" with suggested location>
- **What to document:** <specific content needed — parameters, examples, migration notes>

Priority guide:
- **High:** Breaking changes, new major features, removed/deprecated APIs, changed defaults
- **Medium:** New non-breaking APIs, new configuration properties, new extensions
- **Low:** Minor behavioral refinements, new optional parameters, edge-case fixes

### Already documented in this release window

- <commit/PR>: <what it documented>

### No documentation needed

Summary of the internal-only changes, grouped by type (tests, build, refactoring, etc.),
so the reader can verify nothing was miscategorized.
```

Keep the report concrete and actionable. For each documentation item, name the specific page
and section in `docs/` that needs updating, or describe where a new page should go. Reference
the actual PR titles and file paths so someone can act on the report without re-investigating.

When the change set is large (100+ commits), organize the "action needed" section by area
(engine, Python API, clients, extensions, server/deployment, configuration) within each
priority level, so related items are adjacent.
