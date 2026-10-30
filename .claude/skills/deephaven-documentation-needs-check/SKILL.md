---
name: deephaven-documentation-needs-check
description: >
  Analyze new or changed code in deephaven-core and determine whether it needs documentation
  updates. Examines the diff (staged, unstaged, or between branches) for user-facing changes —
  new public APIs, changed behavior, new configuration properties, new or changed gRPC services,
  Python binding additions, new extensions or integrations — and produces a verdict
  (DocumentationNeeded / NoDocumentationNeeded) with a summary of what needs documenting and
  where. **Use this skill when:** someone asks "does this need docs?", "what documentation does
  this change need?", "check if this PR needs documentation," or when triaging whether to apply
  the DocumentationNeeded or NoDocumentationNeeded label to a PR. **Do NOT use for:** reviewing
  existing documentation (use deephaven-docs-review-full), writing documentation, or
  Enterprise/deephaven-ent code.
allowed-tools: Read, Grep, Glob, Bash(git diff *), Bash(git log *), Bash(git show *), Bash(find *)
---

# Documentation needs check

Analyze code changes and determine whether they require documentation updates, what kind, and
where those updates belong.

## 1. Identify the change set

Determine what code changed. Use the approach that fits the request:

```bash
# If reviewing a branch against main
git diff --name-status main...HEAD

# If reviewing staged changes
git diff --name-status --cached

# If reviewing unstaged changes
git diff --name-status

# If given a specific commit range
git diff --name-status <base>..<head>
```

If the user specifies a PR number, diff range, or branch, use that. If nothing is specified,
default to `main...HEAD`.

## 2. Classify each changed file

Sort every changed file into one of these buckets. A file can belong to more than one.

### User-facing (documentation likely needed)

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

### Internal-only (documentation usually NOT needed)

| Change type | Where to look | Why no docs |
|---|---|---|
| **Test-only changes** | `**/src/test/**`, `*Test.java`, `*TestBase.java` | Not user-facing |
| **Build/CI changes** | `buildSrc/`, `*.gradle`, `.github/workflows/`, `settings.gradle` | Build infrastructure |
| **Code style/formatting** | Spotless changes, whitespace, import ordering | No behavioral change |
| **Internal refactoring** | Package-private classes, `@InternalUse` classes, unchanged public API | No user-visible change |
| **Performance-only** | Same API, same behavior, faster execution | Usually no docs unless user-visible (new config knobs, changed defaults) |
| **Dependency updates** | `gradle/libs.versions.toml`, version bumps | Unless a user-facing dependency changed its API |
| **Liveness/ref-counting internals** | `engine/liveness/`, `ReferenceCounted` changes | Engine internal |
| **UpdateGraph internals** | Notification ordering, cycle mechanics | Engine internal unless new user-facing config |
| **Documentation-only changes** | `docs/`, `.devin/rules/` | Already documentation |

## 3. Analyze each user-facing change in detail

For each file or group of files in the user-facing bucket, determine exactly what changed and
whether it affects what a user sees, writes, or configures.

### Public API changes

Read the diff and answer:

- **Is the class/method/function public?** Check access modifiers, `__init__.py` exports, `@InternalUse` annotations.
- **Is it new, modified, or removed?**
  - New: needs documentation of what it does, its parameters, return type, and at least one example.
  - Modified signature: needs updated parameter docs if parameters changed.
  - Modified behavior (same signature): needs updated description of what it does or when to use it.
  - Removed/deprecated: needs deprecation notice or migration guidance.
- **Is it a breaking change?** A renamed method, removed parameter, or changed default is a breaking change that needs a migration note.

### Configuration changes

- **New property:** needs documentation of its name, type, default, and what it controls.
- **Changed default:** needs updated docs showing the new default and why it changed.
- **Removed property:** needs migration guidance.

### gRPC/protocol changes

- **New RPC or message type:** needs client-side documentation for every language binding.
- **Changed field:** needs updated API docs for affected clients.

### Behavioral changes

Even when the API surface is identical, a behavioral change can need docs:

- Changed error messages or error conditions
- Changed ordering guarantees
- Changed null/empty handling
- New warnings or logging visible to users
- Changed default thread counts, batch sizes, or timeouts

## 4. Check existing documentation coverage

For each change that needs docs, check whether documentation already exists and what needs
updating:

```bash
# Search for existing mentions of the class/method/feature in docs
grep -r "featureName\|ClassName\|method_name" docs/ --include="*.md" -l
```

Note whether the change:
- Needs a **new page** (entirely new feature or concept)
- Needs **updates to existing pages** (changed behavior, new parameters)
- Needs **updates to API reference** (Javadoc/PyDoc changes handled automatically vs. prose docs)
- Needs **updates to examples** (existing examples now wrong or incomplete)

## 5. Produce the verdict

### Verdict: DocumentationNeeded

Use when ANY of the following are true:

- A new public API was added (Java, Python, Groovy, gRPC, client)
- An existing public API's behavior, parameters, or return type changed
- A new configuration property was added or an existing default changed
- A new extension, connector, or integration was added
- A user-facing error condition, error message, or workflow changed
- A new server feature, deployment option, or startup flag was added
- An existing documented feature was removed or deprecated

### Verdict: NoDocumentationNeeded

Use when ALL of the following are true:

- No public API surface changed (or changes are internal/package-private only)
- No user-visible behavior changed
- No configuration properties were added, removed, or changed
- The change is purely internal: tests, build, refactoring, performance, style
- Documentation-only changes (already docs)

### Edge cases

- **Javadoc/PyDoc-only changes**: If the code change only updates Javadoc or Python docstrings
  and the generated API reference will pick them up automatically, this counts as
  NoDocumentationNeeded for *prose* docs — but note that the generated reference will update.
- **New test helpers or test infrastructure**: NoDocumentationNeeded unless they're intended for
  user consumption (e.g., `JAVA_PUBLIC_TESTING` project type).
- **Bug fixes**: NoDocumentationNeeded if the docs already describe the *correct* behavior and the
  fix makes the code match. DocumentationNeeded if the docs described the *buggy* behavior.

## 6. Write the report

Structure the output as:

```
## Verdict: [DocumentationNeeded | NoDocumentationNeeded]

### Summary
One paragraph explaining the overall documentation impact.

### Changes requiring documentation (if DocumentationNeeded)

For each change:
- **What changed**: [description]
- **Where in docs**: [existing page to update, or "new page needed"]
- **What to document**: [specific content needed]
- **Priority**: [High — breaking/new feature | Medium — enhancement | Low — minor addition]

### Changes NOT requiring documentation (if any user-facing files were in the diff)
- [file/change]: [reason no docs needed]

### Suggested PR label
`DocumentationNeeded` or `NoDocumentationNeeded`
```

Keep the report concrete. Don't say "update the docs for X" — say which page, which section,
and what the new content should cover. Reference specific files in the diff and specific pages
in `docs/` so the author can act on the report without re-investigating.
