---
name: deephaven-core-research
description: Research the deephaven-core (Community) codebase to understand implementations, architecture, and feature behavior. Use when the user asks about how a feature works, where code lives, why something was implemented a certain way, or needs to trace data/control flow through the deephaven-core repo specifically.
allowed-tools: Read, Grep, Glob, Bash(git log *), Bash(git blame *), Bash(git show *)
---

Perform deep research on the Core codebase to develop SME-level understanding.

## 1. Clarify and plan

Ask the user what they want to research. Classify the goal and plan your approach:

| Research type | Focus areas | Depth target |
|--------------|-------------|--------------|
| **Implementation lookup** | Steps 2-4, then 6 | Find the code, understand its contract and behavior |
| **Architecture question** | Steps 2-3, then 5 | Map module boundaries, dependencies, and extension points |
| **Feature trace** | Steps 2-4 in sequence | Follow data/control flow end-to-end, understand each layer |
| **Historical context** | Steps 2-3, then 7 | Understand why code exists and how it evolved |

## 2. Build conceptual understanding first

Before diving into code, understand WHAT you're researching:

1. **Search for documentation.** Look for README files, package-info.java, or Javadoc in the relevant module.
2. **Identify the core abstractions.** What are the key interfaces and classes? What problem do they solve?
3. **Understand the domain vocabulary.** Key terms in Core:
   - **UpdateGraph** — orchestrates live table updates, manages dependency tracking and notification ordering
   - **TableUpdateListener** — reactive mechanism for propagating live table updates downstream (see `engine/api/`)
   - **RowSet** — efficient representation of row keys; tracks which rows exist in a table
   - **ColumnSource** — provides typed access to column data; the fundamental data access abstraction
   - **ScriptSession** — manages user script execution in Python or Groovy
   - **Barrage** — streaming protocol for efficient table data transfer between client and server

4. **Read the interfaces before implementations.** Interfaces define contracts; implementations are details.

## 3. Search strategically

Start broad, then narrow. Iterate until you find the right code.

**Broad search first (Glob for filenames, Grep for content):**
```
Glob pattern="**/*UpdateListener*.java"
Grep pattern="notifyChanges|propagate.*update" path="engine/" -i
```

**Exact symbol search when you know names:**
```
Grep pattern="TableUpdateListener" output_mode="files_with_matches"
Grep pattern="implements UpdateGraph"
```

**If search returns nothing useful:**
- Try synonyms or related terms
- Search for error messages or log strings
- Check the Quick Reference table in the appendix for module entry points
- Ask the user for more context

**If search returns too much:**
- Add class/method name constraints
- Filter by module path
- Look for test files first (they show expected usage)

## 4. Trace code systematically

**For each significant class/method, understand:**
1. **Contract** — What does it promise? Read the interface and Javadoc.
2. **Callers** — Who uses this? Search for the method name.
3. **Callees** — What does it depend on? Read the implementation.
4. **Tests** — What behavior is expected? Check `*Test.java` files.
5. **Configuration** — Is behavior configurable? Look for `*Configuration.java` or properties.

**Key file types to examine:**
- **Interfaces** — define contracts and extension points
- **`*Configuration.java`** — configuration schemas and defaults
- **`*Service.java`, `*Handler.java`** — core business logic
- **`*Test.java`** — expected behavior and edge cases
- **`*.gradle`** — module dependencies

**When to go deeper:**
- The code references unfamiliar classes → trace those too.
- The behavior seems configurable → find where config is loaded.
- The code delegates to another module → follow the delegation.

**When to stop:**
- You've reached well-understood infrastructure (logging, basic collections).
- You've found the authoritative implementation (not just a wrapper).
- You can explain the behavior without hand-waving.

## 5. Map architecture

For architecture questions, build a mental model:

1. **Module boundaries** — What does each module own? Check `*.gradle` for dependencies.
2. **Extension points** — Where can behavior be customized? Look for interfaces with multiple implementations.
3. **Data flow** — How does data move between modules?
4. **Configuration flow** — Where are settings defined, stored, and consumed?

**Common architectural patterns:**
```
Configuration flow:
props/**/*.prop files or environment → Configuration/ utilities → Runtime component

gRPC service flow:
proto/proto-backplane-grpc/*.proto → generated Java → server/src/.../server/**/*ServiceGrpcImpl.java → java-client/ or py/client/

Table update flow:
Source data change → UpdateGraph cycle → TableUpdateListener.onUpdate → Downstream table update → Barrage → UI refresh
```

## 6. Trace features end-to-end

For feature traces, follow data/control through all layers:

1. **Entry point** — gRPC service (`server/`), UI action (`web/`), or Python/Groovy API (`py/`)
2. **Service layer** — `*ServiceGrpcImpl.java` in `server/src/main/java/io/deephaven/server/**/` (nested, e.g. `table/ops/TableServiceGrpcImpl.java`, `table/inputtables/InputTableServiceGrpcImpl.java`)
3. **Domain logic** — Core classes in `engine/table/` or `extensions/`
4. **Data access** — `ColumnSource`, `RowSet` in `engine/api/`

**Example trace (client executes a table operation via gRPC):**
```
User calls table.where("Price > 100") in py/client/pydeephaven
  → Client sends gRPC request to server
  → server/src/.../table/ops/TableServiceGrpcImpl receives request
  → engine/table/impl/QueryTable.where() evaluates WhereFilter against ColumnSource data
  → If the source or filters are refreshing, a WhereListener is created and the result table is registered with UpdateGraph for live updates; a fully static where() has no listener
  → Barrage streams result back to client
```

**Iterate until you can explain:**
- What triggers this feature?
- What data flows through it?
- What can go wrong? (check error handling)
- How is it configured?

## 7. Research history when needed

For historical context or debugging:

// turbo
```bash
git log --oneline -n 20 -- {path}
```

// turbo
```bash
git blame {file} | head -50
```

// turbo
```bash
git log -S "{symbol}" --oneline -n 10
```

**Useful patterns:**
- `git log --grep="fix" -- {file}` — find bug fixes
- `git log --grep="{feature}" --oneline` — feature history
- `git show {commit}` — understand a specific change

**What to look for:**
- Why was this code added? (commit message)
- What problem did it solve? (linked issues)
- Has it been refactored? (multiple significant commits)

## 8. Validate your understanding

Before reporting, verify your mental model:

1. **Can you explain it simply?** If not, you don't understand it yet.
2. **Do the tests confirm your understanding?** Read test cases for expected behavior.
3. **Are there edge cases you haven't considered?** Check error handling paths.
4. **Would your explanation survive code review?** Be precise, not hand-wavy.

## 9. Synthesize findings

Report comprehensively:

- **Summary** — Concise answer to the research question. Lead with the insight.
- **Key files** — List with `@path:line` format. Prioritize by importance.
- **Call chain** — For feature traces, document the complete flow.
- **Architecture diagram** — For architecture questions, describe module relationships.
- **Conceptual model** — Explain the abstractions and how they relate.
- **Gaps** — What couldn't be determined? What needs SME input?
- **Related areas** — What connected code might be worth exploring next?

**Quality bar:** Could someone unfamiliar with the codebase read your synthesis and understand the feature/architecture well enough to modify it?

---

# Appendix: Module Reference

## Quick reference

| Research goal | Start here |
|--------------|-------------|
| Table operations | `engine/table/src/main/java/io/deephaven/engine/table/impl/` |
| Table API/interfaces | `engine/api/src/main/java/io/deephaven/engine/table/` |
| Live updates | `engine/updategraph/src/main/java/io/deephaven/engine/updategraph/` |
| Row/column data | `engine/chunk/`, `engine/rowset/`, `engine/vector/` |
| Server/gRPC | `server/src/main/java/io/deephaven/server/` |
| gRPC definitions | `proto/proto-backplane-grpc/` |
| Web UI | `web/client-ui/`, `web/client-api/` |
| Python integration | `py/server/`, `Integrations/` |
| Data connectors | `extensions/` (parquet, kafka, iceberg, arrow, etc.) |
| Authentication | `authentication/`, `authorization/` |
| Configuration | `Configuration/`, `props/` |

## Module layouts

- **Top-level**: `{module-name}/build.gradle` (e.g., `server/build.gradle`)
- **Nested**: `{parent}/{sub-module}/build.gradle`
  - `engine/` — core table engine modules
  - `extensions/` — data format and connector modules
  - `server/` — gRPC server modules
  - `web/` — web client modules
  - `py/` — Python integration modules

## Engine modules

### engine/api/ — Table interfaces and contracts
- **Entry points**: `engine/api/src/main/java/io/deephaven/engine/table/`
- **Key classes**: `Table`, `TableDefinition`, `ColumnSource`, `RowSet`, `TableUpdateListener`, `TableUpdate`

### engine/table/ — Table implementations
- **Entry points**: `engine/table/src/main/java/io/deephaven/engine/table/impl/`
- **Key classes**: `QueryTable`, `BaseTable`, `WhereListener`, `SelectColumn`, `ColumnSource` implementations
- **Subpackages**: `select/`, `sources/`, `join/`, `by/`, `sort/`

### engine/updategraph/ — Live update orchestration
- **Entry points**: `engine/updategraph/src/main/java/io/deephaven/engine/updategraph/`
- **Key classes**: `UpdateGraph`, `NotificationQueue`, `LogicalClock`, `UpdateSourceRegistrar`

### engine/chunk/ — Bulk data access
- **Entry points**: `engine/chunk/src/main/java/io/deephaven/chunk/`
- **Key classes**: `Chunk`, `WritableChunk`, `ChunkType` — efficient bulk data transfer

### engine/rowset/ — Row key management
- **Entry points**: `engine/rowset/src/main/java/io/deephaven/engine/rowset/`
- **Key classes**: `RowSet`, `WritableRowSet`, `RowSetFactory`, `RowSequence`

## Server modules

### server/ — gRPC server implementation
- **Entry points**: `server/src/main/java/io/deephaven/server/`
- **Key classes**: `*ServiceGrpcImpl` (TableServiceGrpcImpl, SessionServiceGrpcImpl, etc.)
- **Subpackages**: `session/`, `table/`, `arrow/`, `barrage/`

### proto/proto-backplane-grpc/ — gRPC protocol definitions
- **Entry points**: `proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/`
- `.proto` files define all client-server communication

## Extensions

### extensions/parquet/ — Parquet file support
- **Entry points**: `extensions/parquet/table/src/main/java/io/deephaven/parquet/table/`
- **Key classes**: `ParquetTools`, `ParquetTableWriter`

### extensions/kafka/ — Kafka streaming
- **Entry points**: `extensions/kafka/src/main/java/io/deephaven/kafka/`
- **Key classes**: `KafkaTools`, `KafkaStreamPublisher` (in `ingest/` subpackage)

### extensions/iceberg/ — Iceberg table support
- **Entry points**: `extensions/iceberg/src/main/java/io/deephaven/iceberg/`

### extensions/barrage/ — Streaming protocol
- **Entry points**: `extensions/barrage/src/main/java/io/deephaven/extensions/barrage/`
- **Key classes**: `BarrageMessageWriter`, `BarrageOptions`, `BarrageSubscriptionOptions`

### extensions/arrow/ — Arrow integration
- **Entry points**: `extensions/arrow/src/main/java/io/deephaven/extensions/arrow/`

## Web and clients

### web/client-api/ — GWT JavaScript API
- **Entry points**: `web/client-api/src/main/java/io/deephaven/web/client/api/`
- **Key classes**: `JsTable`, `CoreClient`, `WorkerConnection`

### web/client-ui/ — React IDE (links to external repo)
- Points to `deephaven/web-client-ui` repository

### java-client/ — Java client SDK
- **Entry points**: `java-client/session/src/main/java/io/deephaven/client/`
- **Key classes**: `Session`, `TableHandle`, `FlightSession`

### py/client/ — Python client (pydeephaven)
- **Entry points**: `py/client/pydeephaven/`

### py/server/ — Python server integration (deephaven package)
- **Entry points**: `py/server/deephaven/`
- **Key modules**: `table.py`, `dtypes.py`, `time.py`, `plot/`

## Infrastructure

### Configuration/ — Configuration utilities
- **Entry points**: `Configuration/src/main/java/io/deephaven/configuration/`

### authentication/ — Authentication handlers
- **Entry points**: `authentication/src/main/java/io/deephaven/auth/`
- **Example providers**: `authentication/example-providers/`

### authorization/ — Authorization framework
- **Entry points**: `authorization/src/main/java/io/deephaven/auth/`

## Specialized modules

### Plot/ — Charting
- **Entry points**: `Plot/src/main/java/io/deephaven/plot/`
- **Key classes**: `Figure`, `Axes`, `Series`

### Integrations/ — Python integration
- **Entry points**: `Integrations/src/main/java/io/deephaven/integrations/`
- **Subpackages**: `python/` (no `groovy/` subpackage — Groovy session support lives in `engine/table/src/main/java/io/deephaven/engine/util/GroovyDeephavenSession.java` and the server console wiring under `server/src/main/java/io/deephaven/server/console/groovy/`)

### ModelFarm/ — Model execution
- **Entry points**: `ModelFarm/src/main/java/io/deephaven/modelfarm/`

## Common patterns

**Configuration flow:**
```
props/**/*.prop or environment → Configuration/ → Runtime component
```

**gRPC service flow:**
```
proto/proto-backplane-grpc/*.proto → generated Java → server/src/.../server/**/*ServiceGrpcImpl.java → java-client/ or py/client/
```

**Table update flow:**
```
Source change → PeriodicUpdateGraph's timer starts the next cycle (or, for an EventDrivenUpdateGraph, an explicit UpdateGraph.requestRefresh call triggers one) → Notification cycle → TableUpdateListener.onUpdate → Downstream tables → Barrage → Client
```
