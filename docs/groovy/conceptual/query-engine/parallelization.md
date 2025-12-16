---
title: Parallelization
sidebar_label: Parallelization
---

Deephaven parallelizes table operations like [`select`](../../reference/table-operations/select/select.md), [`update`](../../reference/table-operations/select/update.md), and [`where`](../../reference/table-operations/filter/where.md) automatically, using multiple CPU cores to improve data processing performance. Understanding how parallelization works is essential for writing efficient queries and ensuring correct results.

> [!IMPORTANT] > **Breaking change in Deephaven 0.41+**: Queries now run in parallel by default. Non-thread-safe code will produce incorrect results.
>
> **Quick check**: Does your code use global variables, depend on row order, or call non-thread-safe functions? If yes, the [crash course guide](../../tutorials/crash-course/parallelization.md) provides practical examples.

This guide provides comprehensive coverage of how parallelization works and when to control it. For a practical overview with examples, see the [crash course guide](../../tutorials/crash-course/parallelization.md).

## Deephaven does parallelization for you

Deephaven automatically uses all available CPU cores to parallelize query processing. Parallelization is handled by the engine without requiring explicit configuration.

Parallelization occurs at two levels:

### Parallelizing multiple tables in the DAG

When you have independent operations on the same source table, Deephaven processes them simultaneously. Consider this example:

```groovy skip-test
// Retrieve a live table
marketData = getMarketFeed()

// Run independent transformations
withMetrics = marketData.update("Value = Price * Volume")
highVolume = marketData.where("Volume > 1000000")
recentTrades = marketData.where("Timestamp > now() - 60 * SECOND")

// Combine results
analysis = merge(withMetrics, highVolume, recentTrades)
```

When new data arrives in `marketData`, Deephaven processes the three transformations (`withMetrics`, `highVolume`, `recentTrades`) simultaneously because they are independent. The `analysis` table updates only after all three transformations complete.

The [update graph (DAG)](../dag.md) tracks dependencies between tables and automatically parallelizes independent operations.

### Parallelizing individual table calculations

Deephaven also parallelizes work within a single table operation. When you run `source.update("Total = Price * Quantity")`, Deephaven:

1. Splits the `Price` and `Quantity` columns into chunks (typically 4096 rows each)
2. Assigns chunks to available CPU cores
3. Each core calculates `Total` for its chunk independently
4. Combines results into the final `Total` column

Deephaven is a column-oriented engine that processes entire columns rather than individual rows, enabling efficient parallelization.

**What gets parallelized**:

- Column calculations in [`update`](../../reference/table-operations/select/update.md), [`select`](../../reference/table-operations/select/select.md), [`view`](../../reference/table-operations/select/view.md), and [`updateView`](../../reference/table-operations/select/update-view.md).
- Filters in [`where`](../../reference/table-operations/filter/where.md) clauses.
- Aggregations and group-by operations.
- Join operations.

**What doesn't get parallelized**:

- Operations marked with `.withSerial()` (you control this).
- Operations waiting for dependencies (automatic in the update graph).

## Controlling parallelization

When automatic parallelization isn't appropriate for your code, Deephaven provides mechanisms to control execution order and concurrency. Three concepts work together:

### Selectable

A [`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) is a column expression object that represents a formula to be evaluated in [`select`](../../reference/table-operations/select/select.md) or [`update`](../../reference/table-operations/select/update.md) operations.

When you write `.update("A = i * 2")`, Deephaven converts this string into a Selectable internally. You can create Selectables explicitly to apply concurrency controls:

```groovy order=source,result
// Create a Selectable from a formula string
col = Selectable.of(ColumnName.of("A"), RawString.of("i * 2"))

// Use it in table operations
source = emptyTable(10)
result = source.update([col])
```

Creating explicit Selectables allows you to apply `.withSerial()` or use barriers to control when and how the formula executes.

### Serial execution

Serial execution forces rows to be processed one at a time, in order, on a single thread. This prevents race conditions when your code has dependencies between rows or accesses shared state.

Applied using `.withSerial()` on a Selectable or Filter:

```groovy syntax
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString

col = Selectable.of(ColumnName.of("ID"), RawString.of("getNextId()")).withSerial()
```

### Barriers

Barriers create explicit ordering dependencies between operations. They ensure one operation completes before another begins:

1. Operation A **declares** a barrier ("I'll signal when I'm done").
2. Operation B **respects** the barrier ("I'll wait for A's signal").
3. Deephaven ensures A completes before B starts.

Barriers coordinate multiple operations that share state or have ordering requirements.

## Query phases

Query operations execute in two phases: initialization and updates.

### Query initialization

When a table is first created, the initial state is computed using the available data. This happens when you call operations like [`.where()`](../../reference/table-operations/filter/where.md), [`.update()`](../../reference/table-operations/select/update.md), or [`.naturalJoin()`](../../reference/table-operations/join/natural-join.md).

For [refreshing](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/BaseTable.html#isRefreshing()) tables, Deephaven also adds a node to the [update graph](../dag.md) to track dependencies.

**How parallelization works during initialization**: Deephaven splits the data into chunks and processes them on multiple cores simultaneously. Each core works on its assigned chunk independently.

### Query updates

After initialization, a table is updated as input data changes. This only applies to refreshing tables that receive new or modified data.

**How parallelization works during updates**: Deephaven parallelizes in two ways:

1. **Within operations**: Each operation splits its work into chunks processed on multiple cores.
2. **Across operations**: Independent operations in the [update graph](../dag.md) run simultaneously on different cores.

## Thread pools

Deephaven uses two thread pools to manage parallelization. By default, both thread pools are configured to use all available CPU cores.

### Operation Initialization Thread Pool

Handles parallel processing during query initialization (when you first create a table operation).

**Configuration**: `OperationInitializationThreadPool.threads`

- Default: `-1` (use all available cores)
- Set to a specific number to limit parallelism during initialization

**When it's used**:

- Initial calculation of [`update`](../../reference/table-operations/select/update.md), [`select`](../../reference/table-operations/select/select.md), [`where`](../../reference/table-operations/filter/where.md), etc.
- Processing existing data when creating derived tables.
- Join operations on static tables.

### Update Graph Processor Thread Pool

Handles parallel processing during update cycles (when live tables receive new data).

**Configuration**: `PeriodicUpdateGraph.updateThreads`

- Default: `-1` (use all available cores)
- Set to a specific number to limit parallelism during updates

**When it's used**:

- Processing new/modified rows in refreshing tables
- Propagating changes through the update graph
- Parallel execution of independent DAG nodes

Both thread pools use [Runtime.availableProcessors()](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/Runtime.html#availableProcessors()) to determine the number of available cores at startup.

## Controlling concurrency

Deephaven parallelizes operations by default. Concurrency control mechanisms are available for operations that require specific execution ordering or single-threaded processing.

### Key terms

Three concepts work together to control parallelization:

- **Selectable**: A column expression object used in `select` or `update` operations. Created using `Selectable.of()`.
- **Serial**: A property applied to a Selectable or Filter that forces in-order row evaluation. Applied using `.withSerial()`.
- **Barrier**: An explicit ordering mechanism that controls when Selectables or Filters can begin evaluation. One Selectable declares a barrier, and another respects it.

You can control ordering in two ways:

1. **Mark a Selectable as serial** - Ensures rows are evaluated in order; may also create implicit barriers between serial Selectables (config-dependent).
2. **Use explicit barriers** - Provides fine-grained control over which Selectables must complete before others begin.

### Parallelization (default)

Deephaven automatically parallelizes operations if they are stateless.

**Stateless operations**:

- Don't depend on mutable external inputs (no global variables).
- Don't depend on row processing order.
- Always produce the same output for the same input.
- Are thread-safe.

**Examples of stateless operations**:

```groovy order=source1,result1,source2,result2,source3,result3,source4,result4
// Pure column arithmetic
source1 = emptyTable(10).update("Price = i * 10.0", "Quantity = i")
result1 = source1.update("Total = Price * Quantity")

// String manipulation
source2 = emptyTable(10).update("FirstName = `First` + i", "LastName = `Last` + i")
result2 = source2.update("FullName = FirstName + ' ' + LastName")

// Conditional logic
source3 = emptyTable(10).update("Age = i + 18")
result3 = source3.where("Age > 21")

// Built-in functions
source4 = emptyTable(10).update("X = i * 2.0")
result4 = source4.update("Squared = sqrt(X)")
```

> [!WARNING] > **Breaking change in Deephaven 0.41+**
>
> **Deephaven 0.40 and earlier**: Assumed all formulas were stateful (serial) by default
>
> **Deephaven 0.41 and later**: Assumes all formulas are stateless (parallel) by default
>
> If your formula uses global state or depends on row order, you **must** mark it with `.withSerial()` or it will produce incorrect results.

You can change the default behavior using configuration properties:

- For [`select`](../../reference/table-operations/select/select.md) and [`update`](../../reference/table-operations/select/update.md): set `QueryTable.statelessSelectByDefault`.
- For filters: set `QueryTable.statelessFiltersByDefault`.

### Serialization

Serialization forces single-threaded, in-order execution for operations that are not thread-safe.

**When serialization is required**:

- The formula uses or modifies global variables
- The formula calls non-thread-safe external functions
- The formula depends on rows being processed in a specific order
- Race conditions produce incorrect results

> [!NOTE]
> Most queries don't need serial execution. Use `.withSerial()` only when parallelization causes incorrect results.

#### How serialization works

Marking an operation as serial tells Deephaven:

- Process rows one at a time, in order
- Don't parallelize this operation across CPU cores
- Ensure thread-safe execution for stateful code

The [`ConcurrencyControl`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html) interface provides the `.withSerial()` method for [`Filter`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) ([`where`](../../reference/table-operations/filter/where.md) clause) and [`Selectable`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) ([`update`](../../reference/table-operations/select/update.md) and [`select`](../../reference/table-operations/select/select.md) operations).

> [!IMPORTANT] > `ConcurrencyControl` cannot be applied to Selectables passed to `view` or `updateView`. These operations compute results on demand and cannot enforce ordering constraints. Use `select` or `update` instead when serial evaluation is needed.

#### Example: Global state requires serialization

This example demonstrates why some code needs serialization. A function maintains global state:

```groovy order=t
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)
t = emptyTable(1_000_000).update("A = counter.getAndIncrement()", "B = counter.getAndIncrement()")
```

Without serialization, parallel execution could cause race conditions where multiple threads read and update `counter` simultaneously, producing incorrect results.

#### Using `.withSerial()` for Selectables

To force serial execution for a column calculation, create a `Selectable` object and apply `.withSerial()`:

```groovy order=result
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Force serial execution - rows processed one at a time, in order
col = Selectable.of(ColumnName.of("ID"), RawString.of("counter.getAndIncrement()")).withSerial()
result = emptyTable(1_000_000).update([col])
```

When a Selectable is serial:

- Every row is evaluated in order (row 0, then row 1, then row 2, etc.).
- Only one thread processes the column at a time.
- Global state updates happen sequentially without race conditions.

#### Using `.withSerial()` for Filters

Serial filters are needed when filter evaluation has stateful side effects. String-based filters in [`where()`](../../reference/table-operations/filter/where.md) are parallelized by default, so construct Filter objects explicitly:

<!-- TODO: Fix Groovy syntax - MissingMethodException: No signature of method: static io.deephaven.api.filter.Filter.isNull() is applicable for argument types: (String). Need correct import/method call syntax for Groovy. -->

```groovy order=result
import static io.deephaven.api.filter.Filter.isNull
import static io.deephaven.api.filter.Filter.not

// Create filters with serial evaluation
filter1 = isNull("X").withSerial()
filter2 = not(isNull("Y")).withSerial()

result = emptyTable(1000)
    .update("X = i % 10 == 0 ? null : i", "Y = i % 5 == 0 ? null : i")
    .where(filter1, filter2)
```

When a Filter is serial:

- Every input row is evaluated in order.
- Filter cannot be reordered with respect to other Filters.
- Stateful side effects happen sequentially.

### Barriers

Barriers control the execution order between different operations. Use barriers when operation A must complete before operation B starts.

**When you need barriers**:

- One operation depends on side effects from another operation.
- Multiple operations share resources that require coordinated access.
- Fine-grained control over execution order is necessary.

#### How barriers work

A [`Barrier`](https://docs.deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.Barrier.html) is an object that creates ordering dependencies:

1. One operation **declares** the barrier ("I'll signal when I'm done").
2. Another operation **respects** the barrier ("I'll wait for the signal").
3. Deephaven ensures the declaring operation completes before the respecting operation starts.

#### Barriers for Selectables

To ensure that all rows of column `A` are evaluated before any rows of column `B` begin evaluation:

<!-- TODO: Fix Groovy syntax - Unable to resolve class ConcurrencyControl.Barrier. Inner class instantiation 'new ConcurrencyControl.Barrier()' fails in Groovy. Need correct syntax for accessing inner Barrier class. -->

```groovy order=t
import io.deephaven.api.ConcurrencyControl
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Create a barrier
barrier = new ConcurrencyControl.Barrier()

// Column A declares the barrier ("I'll signal when done")
colA = Selectable.of(ColumnName.of("A"), RawString.of("counter.getAndIncrement()"))
    .withDeclaredBarriers(barrier)

// Column B respects the barrier ("I'll wait for A's signal")
colB = Selectable.of(ColumnName.of("B"), RawString.of("counter.getAndIncrement()"))
    .withRespectedBarriers(barrier)

t = emptyTable(1_000_000).update([colA, colB])
```

With this barrier:

- Column `A` processes all 1,000,000 rows completely.
- Only after `A` finishes does column `B` start processing.
- Both columns can still be parallelized internally (unless also marked serial).

#### Barriers for Filters

Barriers control evaluation order between filters when one depends on another's side effects:

<!-- TODO: Fix Groovy syntax - Unable to resolve class ConcurrencyControl.Barrier. Inner class instantiation fails. Need correct Groovy syntax. -->

```groovy order=result
import static io.deephaven.api.filter.Filter.isNull
import io.deephaven.api.ConcurrencyControl

// Create a barrier
barrier = new ConcurrencyControl.Barrier()

// Filter1 declares the barrier
filter1 = isNull("X").withDeclaredBarriers(barrier)

// Filter2 respects the barrier and won't start until filter1 completes
filter2 = isNull("Y").withRespectedBarriers(barrier)

result = emptyTable(1000)
    .update("X = i % 10 == 0 ? null : i", "Y = i % 5 == 0 ? null : i")
    .where(filter1, filter2)
```

#### Implicit barriers

Serial Selectables can create implicit barriers between each other, controlled by the `QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS` configuration property:

**Stateful mode (default)**:

- Serial Selectables act as barriers to other serial Selectables.
- Prevents concurrent execution of serial operations.
- Provides automatic coordination for shared state.

**Stateless mode**:

- Serial Selectables only enforce in-order row evaluation within themselves.
- No automatic barriers between serial operations.
- Use explicit barriers for cross-operation ordering.

Most users can rely on the default stateful behavior.

### Choosing the right approach

Use this decision guide to pick the right concurrency control method:

#### When to use parallelization (default)

**Use default parallel execution when**:

- The formula only uses values from the current row
- The formula has no side effects (doesn't modify global state)
- The formula doesn't depend on row processing order
- The formula is thread-safe

**Examples**:

```groovy order=source1,result1,source2,result2,source3,result3,source4,result4
// These all parallelize safely by default
source1 = emptyTable(10).update("Price = i * 10.0", "Quantity = i")
result1 = source1.update("Total = Price * Quantity")

source2 = emptyTable(10).update("FirstName = `First` + i", "LastName = `Last` + i")
result2 = source2.update("FullName = FirstName + ' ' + LastName")

source3 = emptyTable(10).update("Age = i + 18")
result3 = source3.where("Age > 21")

source4 = emptyTable(10).update("Value = i * 50")
result4 = source4.update("Category = Value > 100 ? `High` : `Low`")
```

#### When to use `.withSerial()`

**Use `.withSerial()` when**:

- Rows must be processed in order within a single operation.
- The formula updates global state sequentially.
- The formula depends on row evaluation order.
- A single Filter or Selectable has order-dependent logic.

**Examples**:

- Sequential numbering with a counter.
- Processing events in chronological sequence.
- Cumulative calculations within one column.
- File I/O or logging operations.

**Code example**:

```groovy order=source,result
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString
import java.util.concurrent.atomic.AtomicInteger

// Global state requires serial execution
counter = new AtomicInteger(0)

col = Selectable.of(ColumnName.of("ID"), RawString.of("counter.getAndIncrement()")).withSerial()
source = emptyTable(10)
result = source.update([col])
```

#### When to use barriers

**Use explicit barriers when**:

- You need to control ordering between different operations.
- Operation A must finish before operation B starts.
- Multiple Filters or Selectables have dependencies.
- One operation populates state that another consumes.

**Examples**:

- Filter A populates a cache that Filter B reads from.
- Column A initializes a resource that Column B uses.
- Sequential operations with cross-dependencies.

**Code example**:

<!-- TODO: Fix Groovy syntax - Unable to resolve class ConcurrencyControl.Barrier. Inner class instantiation fails. Need correct Groovy syntax. -->

```groovy order=source,result
import io.deephaven.api.ConcurrencyControl
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString

cache = [:]

def initCache(key) {
    cache[key] = "Value_${key}"
    return key
}

def useCache(key) {
    return cache.get(key, "Not found")
}

barrier = new ConcurrencyControl.Barrier()

// A must complete before B starts
colA = Selectable.of(ColumnName.of("A"), RawString.of("initCache(Key)")).withDeclaredBarriers(barrier)
colB = Selectable.of(ColumnName.of("B"), RawString.of("useCache(Key)")).withRespectedBarriers(barrier)

source = emptyTable(10).update("Key = i")
result = source.update([colA, colB])
```

#### Quick reference table

| Scenario                             | Solution                      | Why                                 |
| ------------------------------------ | ----------------------------- | ----------------------------------- |
| Pure column math                     | Default (parallel)            | Thread-safe, no shared state        |
| Global counter                       | `.withSerial()`               | Needs sequential row processing     |
| Column A must finish before Column B | Barriers                      | Controls cross-operation ordering   |
| File I/O or logging                  | `.withSerial()`               | Serialize access to shared resource |
| Multiple operations sharing state    | Barriers or implicit barriers | Coordinates access to shared state  |
| Non-thread-safe library              | `.withSerial()`               | Forces single-threaded access       |

## Summary

Deephaven automatically parallelizes queries using all available CPU cores. Understanding parallelization concepts is essential for writing efficient, correct queries.

**Key concepts**:

- Parallelization is automatic for stateless operations in Deephaven 0.41+
- Use `.withSerial()` for operations with global state, order dependencies, or non-thread-safe code
- Use barriers to control ordering between different operations
- Stateless operations provide better performance and safety
- Thread pools use all available cores by default

For practical examples, see the [parallelization crash course](../../tutorials/crash-course/parallelization.md).

### Related documentation

- [Deephaven’s Directed-Acyclic-Graph (DAG)](../dag.md)
- [Multithreading: Synchronization, locks, and snapshots](./engine-locking.md)
- [ConcurrencyControl API (Javadoc)](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
