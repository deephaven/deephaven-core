---
name: deephaven-release-notes
description: >
  Generate release notes for a new Deephaven Community Core version by analyzing all changes
  between two release tags. Produces a structured draft organized by change type (new features,
  improvements, breaking changes, bug fixes) and product area, with reasoning notes and an
  omissions log so a human reviewer can verify coverage. **Use this skill when:** a new Deephaven
  version is being prepared or has just been released and someone needs release notes drafted,
  or when someone asks what changed between two versions. **Do NOT use for:** reviewing
  documentation quality (use deephaven-docs-review-full), researching how a single feature works
  (use deephaven-core-research), or generating changelogs for non-Deephaven projects.
allowed-tools: Read, Grep, Glob, Bash(git *), Bash(gh *), Bash(wc *), Bash(sort *), Bash(uniq *), Bash(grep *), Bash(awk *), Bash(sed *), Bash(head *), Bash(tail *), Bash(cat *), Bash(cut *), Bash(tr *), Skill
---

# Deephaven release notes generation

This skill produces a release notes draft for a Deephaven Community Core version by examining
all relevant sources of change between two release tags. The output is a structured document
ready for human review and editing before publication.

## 0. Determine the version range

Ask the user (or infer from context) which version to generate notes for. You need two tags:

- **From tag**: the previous release (e.g. `v41.10` for a major, `v42.4` for a patch)
- **To tag**: the new release (e.g. `v42.0`, `v42.5`)

If only the new version is given, determine the previous version from `git tag --sort=-v:refname`.
For a major release (e.g. `v42.0`), the "from" tag is the last patch of the previous major
(e.g. `v41.10`). For a patch release (e.g. `v42.5`), the "from" tag is the preceding patch
(e.g. `v42.4`).

Confirm the range before proceeding:
```bash
git log --oneline --first-parent <from>..<to> | wc -l
```

## 1. Gather all changes

Collect data from multiple sources. Run these in parallel where possible.

### 1a. Commit log (primary source)

```bash
git log --first-parent --format="%H %s" <from>..<to>
```

Parse each commit's conventional-commit prefix (`feat`, `fix`, `perf`, `refactor`, `docs`,
`chore`, `ci`, `build`, `test`, `style`, `revert`) and scope (e.g. `csharp/client`, `gradle`).
The `!` suffix or `feat!:`/`fix!:` prefix marks a breaking change.

### 1b. PRs labeled ReleaseNotesNeeded

```bash
gh pr list --state merged --label "ReleaseNotesNeeded" --json number,title,body,labels,mergedAt \
  --limit 500 --jq '.[] | select(.mergedAt >= "<from-date>" and .mergedAt <= "<to-date>")'
```

Get the merge dates of the from/to tags with:
```bash
git log -1 --format="%aI" <from>
git log -1 --format="%aI" <to>
```

These PRs are the ones the team has already flagged as noteworthy. Cross-reference them against
the commit log to ensure none are missed.

### 1c. PRs labeled "breaking"

```bash
gh pr list --state merged --label "breaking" --json number,title,body,labels,mergedAt \
  --limit 100 --jq '.[] | select(.mergedAt >= "<from-date>" and .mergedAt <= "<to-date>")'
```

### 1d. Breaking commits (conventional-commit `!` marker)

From the commit log gathered in 1a, extract any commit whose subject contains `!:` (e.g.
`feat!:`, `fix!:`, `refactor!:`). Cross-reference with 1c to build a complete breaking-changes
list.

### 1e. Web UI version bumps

Look for commits like `Update web version v1.X.Y` or `chore: Update web version`. Note the
web-client-ui version bumps — major UI changes often come from the separate `deephaven/web-client-ui`
repo and are summarized in these version-bump PRs. Read the PR body for details:

```bash
gh pr view <number> --json body -q .body
```

### 1f. Dependency bumps (context only)

Commits prefixed `chore(gradle): bump` are dependency updates. These rarely merit individual
release notes entries, but note any that are security-related, involve major version bumps of
significant dependencies (Arrow, Parquet, Kafka, Iceberg, gRPC, Netty, Protobuf), or that the
team flagged with `ReleaseNotesNeeded`.

## 2. Classify and group changes

### Classification by type

Assign each noteworthy change to exactly one of these categories, in priority order:

1. **Breaking changes** — any change that alters existing behavior, removes or renames a public
   API, changes a default, or requires user action on upgrade. Source: `!` commits, `breaking`
   label, and PR bodies that mention migration or behavioral changes.

2. **New features** — new user-facing capabilities. Source: `feat:` commits, `ReleaseNotesNeeded`
   PRs that describe new functionality.

3. **Improvements** — enhancements to existing features, performance gains, better error messages,
   configuration additions. Source: `perf:` commits, `refactor:` commits with user-visible impact,
   `feat:` commits that enhance rather than create.

4. **Bug fixes** — corrections to incorrect behavior. Source: `fix:` commits.

### Classification by product area

Within each type, group entries by area:

- **Engine** — core table operations, update graph, aggregations, joins, sorts, filters, formulas
- **Parquet** — Parquet reading/writing, codec mapping, predicate pushdown
- **Kafka** — Kafka streaming integration
- **Iceberg** — Apache Iceberg table support
- **S3 / Cloud storage** — S3, GCS, Azure connectors
- **Barrage / Streaming** — the Barrage streaming protocol, subscriptions, Arrow Flight
- **Server** — gRPC services, session management, authentication, authorization, plugins
- **Python** — `deephaven` Python package (server-side), UDFs, Python integration
- **Python client** — `pydeephaven` client library
- **Java client** — Java client SDK
- **C++ client** — C++ client, including R and Python ticking bindings
- **C# client** — C# client library
- **JavaScript / Web** — JS API, web IDE, GWT client
- **SQL** — SQL front-end
- **Configuration** — server configuration, properties
- **Performance** — cross-cutting performance improvements
- **Documentation** — doc changes (usually omitted from release notes unless significant)
- **Build / CI** — build system changes (usually omitted unless user-facing)
- **Dependencies** — dependency updates (usually omitted unless security-related or significant)

### What to include vs. omit

**Include:**
- All breaking changes, no matter how small
- All `feat:` changes with user-visible impact
- All `fix:` changes that affect user-observable behavior
- `perf:` changes with measurable impact
- `refactor:` changes that alter public API surface
- Significant dependency updates (security fixes, major version bumps of core deps)
- Web UI version bumps (summarize highlights)

**Omit (but log in the omissions section):**
- `chore:` commits (routine dependency bumps, CI config, code formatting)
- `docs:` commits (unless they document a new feature introduced in this release)
- `test:` commits (internal test changes)
- `ci:` / `build:` commits (unless they change what users build or deploy)
- `style:` commits
- Internal refactors with no user-visible change
- Gradle plugin bumps, GitHub Actions bumps

**Judgment calls** — changes that could go either way. Log these in the omissions section with
your reasoning so the reviewer can override:
- Refactors that clean up but don't change public API
- Performance improvements without benchmarks
- Error message improvements
- Internal configuration additions
- Test infrastructure changes

## 3. Research notable changes

For each change classified as a new feature or breaking change, and for any improvement that
seems significant, gather enough context to write a clear entry:

1. **Read the PR body** (`gh pr view <number> --json body`) for the author's description of
   what changed and why.
2. **Read the commit diff summary** (`git show --stat <hash>`) to understand scope.
3. **For features**: identify the user-facing API or behavior change. What can users do now that
   they couldn't before? What method/class/parameter was added?
4. **For breaking changes**: identify what was the old behavior, what is the new behavior, and
   what users need to do differently.
5. **For bug fixes**: identify what was broken and how it manifested to users.

Use `deephaven-core-research` (invoke via Skill) for any change that requires deeper code
understanding — for example, tracing a new feature through the engine to understand its
user-facing impact.

## 4. Write the release notes draft

### Format

Follow the established Deephaven release notes blog post structure:

```markdown
# Release notes for Deephaven Community Core version X.Y

<One-sentence tagline summarizing the 2-3 headline items>

Deephaven Community Core version X.Y is now available. Here's what's new.

## New features

### <Headline feature 1>

<1-3 paragraphs describing the feature, what it enables for users, and how to use it.
Include a short code example if the feature has a clear API entry point. Link to relevant
documentation pages where they exist.>

### <Headline feature 2>

<Same format as above.>

### Additional new features

- <One-sentence description of minor feature>
- ...

## Improvements

### <Area name> (e.g. "Parquet", "Performance", "Server")

- <One-sentence description of improvement>
- ...

### <Another area>

- ...

## Breaking changes

### General

- <Description of what changed: old behavior -> new behavior.>
- ...

### Server-side

- ...

### Client-side

- ...

## Bug fixes

### <Area name>

- <One-sentence description: what was broken, now fixed.>
- ...
```

### Writing style

- **Tone**: Direct, reader-addressed. "You can now..." / "Deephaven now supports..."
- **Lead with the user benefit**, not the implementation detail.
- **Headline features** (2-4 per major release) get their own H3 with prose paragraphs.
  Choose features that are most visible or impactful to users.
- **Minor features** are single-sentence bullets under "Additional new features."
- **Improvements and bug fixes** are single-sentence bullets grouped by area.
- **Breaking changes** state both old and new behavior concisely. No migration guide needed in
  the notes themselves, but link to one if it exists.
- **Do not cite PR numbers or issue numbers** in the prose (the blog posts don't). Links to
  documentation and Javadoc are preferred over links to PRs.
- **Code examples** are welcome for headline features. Keep them short (5-15 lines).
- **No dependency bump entries** unless security-related or the team flagged them.

## 5. Compile the reasoning and omissions log

After the draft, include two additional sections that are for the human reviewer, not for
publication:

### Reasoning notes

For each entry in the draft, briefly note:
- The source (commit hash, PR number)
- Why it was categorized as it was (feature vs. improvement vs. fix)
- Any judgment calls made (e.g. "classified as improvement rather than feature because it
  enhances existing parquet reading, doesn't add a new capability")
- Confidence level: **high** (clear from commit/PR), **medium** (inferred from code/context),
  **low** (uncertain — reviewer should verify)

### Omissions log

List every change that was examined and deliberately excluded, with the reason:

```markdown
## Omissions log

### Omitted — routine maintenance
- `chore(gradle): bump guava from 33.5.0-jre to 33.6.0-jre` (#7918) — routine dep bump
- `chore(gradle): bump org.seleniumhq.selenium:selenium-remote-driver` (#8158) — test dep
- ...

### Omitted — internal only
- `refactor: DH-22467: Eliminate trove dependencies` (#8038) — internal refactor, no public
  API change
- ...

### Omitted — documentation only
- `docs: DOC-1381: Uris and authentication` (#8030) — doc update, not a feature change
- ...

### Judgment calls — reviewer should verify
- `refactor: DH-22563: Expose auth transformations to plugins` (#8018) — classified as
  improvement; could be a feature if plugin authors need to know about it. **Medium confidence.**
- ...
```

The omissions log is critical: the human reviewer may deem something important that the agent
passed over. Every commit in the range should appear either in the draft or in the omissions
log. Verify this by counting:

```bash
# Total commits in range
git log --first-parent --oneline <from>..<to> | wc -l

# Should equal: (entries in draft) + (entries in omissions log)
```

## 6. Final checklist

Before delivering the draft, verify:

- [ ] Every `feat!:` and `fix!:` commit appears in Breaking changes
- [ ] Every PR labeled `breaking` appears in Breaking changes
- [ ] Every PR labeled `ReleaseNotesNeeded` appears in the draft (not just the omissions log)
- [ ] Web UI version bumps are summarized
- [ ] No PR numbers appear in the publishable prose (they go in the reasoning notes only)
- [ ] The total of draft entries + omission entries equals the total commit count
- [ ] Headline features (H3 sections) are the 2-4 most impactful user-facing changes
- [ ] Breaking changes state both old and new behavior
- [ ] Bug fix descriptions say what was broken, not just what was changed
- [ ] Areas with no entries are omitted (don't include empty sections)
- [ ] The tagline reflects the actual headline features

Deliver the full output: the publishable draft, then the reasoning notes, then the omissions log.
