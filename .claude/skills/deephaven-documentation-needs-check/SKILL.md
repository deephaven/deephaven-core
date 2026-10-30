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
allowed-tools: Read, Grep, Glob, Bash(git diff *), Bash(git log *), Bash(git show *), Bash(git tag *), Bash(find *), Bash(wc *)
---

# Release documentation needs check

Analyze everything that has landed on `main` since the latest release and determine what needs
documentation for the upcoming release.

## 1. Find the latest release tag and build the change set

Release tags follow the pattern `vX.Y` (e.g., `v42.5`, `v41.10`). Find the latest one:

```bash
git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+$' | head -1
```

Then get the full picture of what's changed since that tag:

```bash
# Total commit count for context
git log <tag>..main --oneline | wc -l

# All changed files, grouped
git diff --name-status <tag>..main

# All merge commits (typically one per PR) with their titles
git log <tag>..main --oneline --merges
```

The merge commit titles are the primary triage input — each represents a PR that landed, and
the Conventional Commits prefix (`feat:`, `fix:`, `perf:`, `chore:`, `docs:`, `test:`, `refactor:`)
tells you its intent. The `--name-status` diff tells you what files actually changed.

## 2. First pass: triage by commit prefix

Sort every merge commit into one of three buckets based on its prefix and title. This is a
fast first pass — the detailed analysis in step 3 refines it.

### Likely needs documentation

- **`feat:`** — New features almost always need docs. Read the title to understand what was added.
- **`fix:`** with user-visible behavior change — A fix that changes documented behavior or error
  conditions needs a doc update. A fix that makes code match existing docs does not.
- **`perf:`** with new configuration knobs — Performance work that adds user-facing config
  properties or changes defaults needs docs. Pure internal speedups do not.

### Likely does NOT need documentation

- **`docs:`** — Already documentation changes.
- **`chore:`** — Build, dependency, CI, tooling changes.
- **`test:`** — Test-only changes.
- **`refactor:`** — Internal restructuring with no API change.
- **`fix:`** where code now matches existing docs — Bug fix that corrects behavior to what docs
  already say.
- **`perf:`** with no new config or changed defaults — Pure internal optimization.

### Needs closer inspection

- **`feat:`** that might be internal-only (new test infrastructure, build tooling).
- **`fix:`** where it's unclear if docs described the buggy or correct behavior.
- Any commit whose title doesn't clearly indicate scope.

## 3. Second pass: analyze user-facing changes in detail

For every commit in the "likely needs documentation" and "needs closer inspection" buckets,
examine the actual diff to determine the documentation impact.

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

For each change that needs docs, check whether documentation already covers it or needs
updating:

```bash
# Search for existing mentions of the feature/class/method in docs
grep -r "featureName\|ClassName\|method_name" docs/ --include="*.md" -l
```

Also check if any `docs:` commits in the same release window already documented the feature.
A `feat:` commit followed by a `docs:` commit covering the same feature can be marked as
already handled.

Classify each item:
- **Already documented** — a `docs:` commit in this release window covers it, or existing docs
  already describe the new behavior accurately.
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
## Release documentation triage: <tag>..main

**Release tag:** <tag>
**Commits since release:** <count>
**Merge commits (PRs):** <count>

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
