# AGENTS.md

This file provides guidance for coding agents (including Claude Code and GitHub Copilot) when working with this repository.

## Overview

Deephaven Community Core is a real-time, time-series, column-oriented analytics engine with
relational database features. Queries operate seamlessly over both historical and live (ticking)
data. The core engine is Java; it is consumed from Python, Groovy, and a set of client APIs
(Java, Python, C++, JS, Go, R) that communicate over gRPC + Apache Arrow Flight + the Barrage
streaming protocol.

This is a large Gradle multi-project monorepo (200+ subprojects). Java 11–25 is required;
the build uses Gradle toolchain auto-provisioning to fetch the right JDK.

## Build & Run

The build is driven by `./gradlew` from the repo root. Subprojects are addressed by Gradle path
(e.g. `:engine-table`, `:server-jetty-app`, `:extensions-parquet-table`).

```bash
# Run the server (Python flavor) — serves the web IDE at https://localhost:10000/ide
./gradlew server-jetty-app:run

# Run the Groovy server instead
./gradlew server-jetty-app:run -Pgroovy

# Attach a debugger on port 5005 (combine with other flags)
./gradlew server-jetty-app:run -Pgroovy -Pdebug

# Build the Python wheel
./gradlew py-server:assemble
```

The PSK auth key is printed to the server log on startup; override with `-Dauthentication.psk=<key>`.

## Testing

Most tests — including the engine suite — use **JUnit 4**, where **`@Category` annotations** route
tests to different Gradle test tasks. (The repo also contains other frameworks: many newer
client/extension modules use **JUnit 5** (Jupiter) via `useJUnitPlatform()`, and some legacy modules
such as `Plot` still use **JUnit 3** / `junit.framework.TestCase`.) The category-based routing below
applies to the JUnit 4 tests.
The default `test` task **excludes** the three categorized types (`ParallelTest`, `SerialTest`,
`OutOfBandTest`); each type has its own task (`testParallel`, `testSerial`, `testOutOfBand`) that
runs *only* that category. These categorized tasks are **not** wired into `check` — run them
explicitly by name. CI runs them nightly: the root `nightly` task and
`.github/workflows/nightly-check-ci.yml` invoke `check`, `testParallel`, `testSerial`, and
`testOutOfBand` directly.

```bash
# Run the (uncategorized) tests for a module
./gradlew :engine-table:test

# Run a single test class or method (standard Gradle filtering)
./gradlew :engine-table:test --tests "io.deephaven.engine.table.impl.SomeTest"
./gradlew :engine-table:test --tests "*SomeTest.someMethod"

# Run the categorized test suites (invoke each task explicitly; CI runs them nightly)
./gradlew :engine-table:testParallel          # @Category(ParallelTest.class)
./gradlew :engine-table:testSerial            # @Category(SerialTest.class) — runs single-forked
./gradlew :engine-table:testOutOfBand         # @Category(OutOfBandTest.class)

# Re-run tests even if cached/unchanged
./gradlew :module:test -PforceTest=true

# Faster engine tests with reduced data sizes
./gradlew :module:test -PshortTests=true
```

Test JVMs run with assertions enabled and use `dh-tests.prop` as the configuration root.
`@Category(ParallelTest.class)` opts a test into parallel execution.

## Lint / Format / Verification tasks

Code style is enforced by **Spotless** (Google-derived style, see `style/`); generated checked-in
code is exempt.

```bash
./gradlew spotlessApply        # auto-format (run before committing)
./gradlew spotlessCheck        # verify formatting

./gradlew quick                # lifecycle task: fast subset of check (compile + spotlessCheck)
./gradlew spotlessCheck quick  # what the "quick CI" runs
./gradlew check                # full verification (slow); CI runs with --continue
```

When opening a PR, mirror the CI commands locally: `./gradlew spotlessCheck quick` first, then
`./gradlew check` for the affected modules.

## Conventions

- **Commit messages** follow Conventional Commits, prefixed with the issue ID, e.g.
  `feat: DH-22670: Add codec-mapping ability` or `fix: DH-22921: ...` (enforced by
  `conventional-pr-check` CI and `cog.toml`).
- **PR labels**: every PR needs `ReleaseNotesNeeded`/`NoReleaseNotesNeeded` and
  `DocumentationNeeded`/`NoDocumentationNeeded`.
- Each Gradle subproject must declare `io.deephaven.project.ProjectType` in its
  `gradle.properties`. The type (e.g. `JAVA_PUBLIC`, `JAVA_LOCAL`, `JAVA_APPLICATION`,
  `JAVA_PUBLIC_TESTING`) selects a convention plugin from `buildSrc/` that wires up
  publishing, testing, licensing, and dependency resolution. When creating a subproject, set this
  property and add it to `settings.gradle`.
- `.devin/rules` is the **documentation** style guide (for docs prose, not code).

## Architecture

The system is layered to decouple query *syntax* from *execution*, *serialization*, and
*client binding*. Understanding these layers and how they connect is the key to navigating the code.

### Core data model (`engine/`)

- **`Table`** (`io.deephaven.engine.table.Table`, in `engine/api`, impl in `engine/table`):
  a columnar, typed, dynamically-updatable dataset. `BaseTable` is the abstract base for impls.
- **`ColumnSource<T>`**: per-column data accessor by row key. Tracks current *and* previous
  values (the "prev" mechanism) for change detection. Data is read in bulk as **`Chunk`s**
  (`engine/chunk`) — fixed-size, mostly zero-copy array windows — for efficient iteration.
- **`RowSet`** (`engine/rowset`): a compressed, possibly non-contiguous set of row keys.
  **`TrackingRowSet`** additionally retains a previous-cycle snapshot.
- **`TableDefinition`**: the schema (column name → type).
- **`TableUpdate`**: emitted each cycle, describing added / removed / modified `RowSet`s, row
  shifts (`RowSetShiftData`), and a **`ModifiedColumnSet`** bitset of which columns changed.

### Update propagation — the DAG (`engine/updategraph`)

Dependent tables form a DAG driven by the **`UpdateGraph`**. Tables are **`DynamicNode`s**;
they register **`TableUpdateListener`s** on upstream tables and receive `TableUpdate`s via
`onUpdate()` each cycle.

- A **`LogicalClock`** sequences cycle phases (Idle → Updating) so all listener callbacks within a
  cycle observe a consistent snapshot.
- **`NotificationQueue`** serializes listener execution; **`MergedListener`** coordinates nodes
  with multiple parents.
- **Liveness / reference counting** (`engine/liveness`: `LivenessNode`, `LivenessReferent`,
  `ReferenceCounted`) keeps upstream dependencies alive through the weak-reference listener chain
  and ensures timely cleanup. New `Table`-producing or `Listener` code typically must participate
  in liveness scoping.
- **Locking**: operations annotated `@ConcurrentMethod` bypass the update lock; others run under
  the graph's shared/exclusive lock within a cycle. `UpdateGraph.serialTableOperationsSafe()`
  gates thread-safety assumptions.
- **Attributes** (`AttributeMap`): semantic metadata propagated through operations
  (e.g. `BlinkTable`, `AddOnly`/`AppendOnly`, `InputTable` markers) used as hints/optimizations.

### Query layers (`table-api`, `qst`, `engine/table/impl`)

- **`table-api`** (`io.deephaven.api`): provider-agnostic fluent `TableOperations` interface
  (filter, join, update, aggregations, sort, …) plus declarative types like `Filter` and `Selectable`.
- **`qst`** (`io.deephaven.qst`, "query snapshot table"): an **immutable query syntax tree**.
  `TableSpec` represents a query graph; `TableCreator` replays a spec against a fluent backend.
  This makes queries serializable — the basis for remote/gRPC execution.
- **`engine/table/impl`**: concrete execution. Logical operations become graph nodes
  (`SelectOrUpdateListener`, `SortListener`, the `By` aggregation suite, join helpers like
  `CrossJoinHelper`/`AsOfJoinHelper`, filter execution).

### Server & client integration (`server/`, `py/`, `java-client/`, `proto/`)

- **`server`** exposes tables over **gRPC + Arrow Flight**; ticking data streams via the
  **Barrage** protocol (flatbuffer messages). See `server/src/.../arrow` for Flight handlers and
  `BarrageMessageWriter`. **`SessionState`** (`io.deephaven.server.session`) manages per-client
  object scopes and ticket resolution.
- gRPC/protobuf definitions live in `proto/`; Barrage flatbuffers and Flight bindings in
  `extensions/barrage` and `extensions/flight-sql`.
- **`py/server`** binds Python to the Java engine via **jpy**; Python authors queries that execute
  in-JVM. `py/client` is the pure-Python gRPC client (`pydeephaven`).
- **`java-client/`** holds the Java client (`session`, `flight`, `barrage`) — note these use
  **Dagger** for dependency injection (the `*-dagger` subprojects), as does the server and
  `plugin/dagger`.
- **DI**: the server and clients are wired with Dagger; look for `@Module`/`@Component` and the
  `*-dagger` subprojects when tracing how components are assembled.

### Other major areas

- **`extensions/`**: pluggable data integrations — `parquet`, `kafka`, `csv`, `iceberg`, `s3`,
  `jdbc`, `json`, `arrow`, `barrage`, `protobuf`, `suanshu` (math), etc.
- **`plugin/`**: server-side plugin system (object types, figures, hierarchical/partitioned tables).
- **`web/`**: the web IDE and JS client API (`web-client-ui`, `web-client-api`).
- **`Util`, `Configuration`, `IO`, `Base`, `log-factory`**: foundational utilities, the
  `Configuration` property system, and the logging framework.
- **`engine/sql`** and **`sql/`**: SQL front-end over the table engine.
