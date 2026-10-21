---
title: Vectorization and the recipe paradigm
---

Deephaven's query engine uses vectorized operations and a declarative "recipe" paradigm to achieve high performance on both static and real-time data. This guide explains the technical foundations of this approach and why it matters for your queries.

This guide starts with the recipe model. Instead of writing step-by-step instructions that process data one element at a time, you describe the result you want, like a recipe that describes the finished dish. The engine works out how to compute that result and keeps it up to date as data changes. The guide then covers:

- How the engine runs recipes over chunks of data in compiled code.
- What that means for memory use.
- Common patterns, when loops are still the right tool, and performance best practices.

## The paradigm shift: Imperative vs declarative

### Traditional programming: Imperative, one value at a time

In traditional programming, you write **imperative** code that processes one data element at a time. Each instruction operates on a single value:

```groovy skip-test
// Traditional Groovy: Imperative, one value at a time
def data = [1, 2, 3, 4, 5]
def results = []
for (value in data) {  // One iteration at a time
    def result = value * value  // One calculation
    results.add(result)  // One append
}
```

This approach:

- Executes instructions sequentially.
- Processes one data element per instruction.
- Requires explicit loops for multiple elements.
- Creates intermediate objects for each value.

### Deephaven: Declarative and chunked

Deephaven uses a **declarative** approach. You describe the columns you want, and the engine computes them in chunks. A chunk is a contiguous block of typed values from one column, such as 4,096 doubles. The engine applies each formula to a whole chunk in compiled code.

For primitive columns, the engine reads the values as primitives. It doesn't box them: boxing wraps each value in an object, such as an `Integer`.

This page uses _vectorization_ to mean processing a column in chunks with compiled code, rather than one boxed value at a time. It is related to, but not the same as, [CPU-level vectorization](#cpu-level-vectorization).

```groovy order=result test-set=groovy-chunked-example
// Deephaven: Declarative, chunked
result = emptyTable(5).update("X = i + 1", "XSquared = X * X")
```

See [`emptyTable`](../reference/table-operations/create/emptyTable.md) and [`update`](../reference/table-operations/select/update.md) for more details.

`i` is a [special variable](../reference/query-language/variables/special-variables.md) that holds each row's index.

This approach:

- Specifies **what** to compute, not **how**.
- Processes data in optimized chunks (vectorization).
- Avoids intermediate objects.

## The recipe paradigm: How it works

### Recipes are specifications

When you write a Deephaven query:

```groovy ticking-table order=null test-set=groovy-recipe-spec
t1 = timeTable("PT1s").update("X = i")
t2 = t1.update("Y = X * 2")
```

See [`timeTable`](../reference/table-operations/create/timeTable.md) for more details.

You're creating a **specification** (recipe) that says "`Y` should always equal `X` times 2". The engine computes `Y` for the existing rows when the query runs and keeps it up to date as `t1` changes. You don't write the loop or the update logic.

### Dependency tracking

For the `t1` and `t2` query in [Recipes are specifications](#recipes-are-specifications), where `t2` adds `Y = X * 2` to `t1`, the engine builds a [**Directed Acyclic Graph (DAG)**](./dag.md) of dependencies:

```
t1 (source) → t2 (derived)
   ↓             ↓
   X      →      Y = X * 2
```

A table ticks when an update cycle adds, removes, or modifies its rows. The [table update model](./table-update-model.md) describes the full cycle. Each time `t1` ticks, this happens:

1. New rows arrive in `t1`.
2. `t1` notifies `t2`, which registered as a [listener](../how-to-guides/table-listeners-groovy.md) when it was created.
3. The engine computes `Y` for the new rows.
4. The engine propagates the updates through the [DAG](./dag.md).

### Update propagation example

This example adds a [cumulative sum](../reference/table-operations/update-by-operations/cum-sum.md) to a ticking table:

```groovy ticking-table order=null test-set=groovy-update-propagation
import static io.deephaven.api.updateby.UpdateByOperation.CumSum

// Create a ticking table (adds row every second)
source = timeTable("PT1s").update("X = ii", "XSquared = X * X")

// Add a cumulative sum - updates automatically!
result = source.updateBy(CumSum("SumX = X"))
```

See [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) and [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) for more details.

Watch the `source` and `result` tables in the UI. Every second:

- A new row arrives in `source`.
- The engine computes `XSquared` for the new row.
- The engine updates `SumX` for the new row.

**Write the recipe once, and it keeps the result up to date for as long as the table is live**.

### Why recipes enable real-time

Real-time processing is where the recipe paradigm shines: it makes keeping results current straightforward. Compare a loop with a recipe, both reading the ticking `source` table from the [update propagation example](#update-propagation-example).

#### Loop approach (doesn't work for real-time)

```groovy skip-test
// ❌ This only runs once!
results = []
source.columnIterator("X").withCloseable { iter ->
    iter.forEachRemaining { x -> results.add(x * 2) }
}
// What happens when new data arrives? Nothing!
```

Imperative loops can't keep up with ticking data without significant additional infrastructure. A loop executes once and stops. You need to build your own subscription and recomputation logic.

#### Recipe approach (recomputes as data ticks)

```groovy ticking-table order=null test-set=groovy-update-propagation
// ✓ This updates automatically!
result = source.update("Y = X * 2")
// New data arrives? Y is computed for new rows automatically!
```

### Incremental computation

The engine is smart about updates. For many operations, including [`update`](../reference/table-operations/select/update.md), it doesn't recompute the whole table on each update. It processes only the rows that changed.

In the [update propagation example](#update-propagation-example), when new rows arrive in `source`:

- The engine processes only the new rows.
- It evaluates every formula for those rows.
- It appends the results to the output columns.
- **Nothing else is recomputed**.

For modified rows, the engine recomputes only the affected rows and passes the changes to downstream tables through the [DAG](./dag.md).

What counts as an affected row depends on the operation. A [cumulative sum](../reference/table-operations/update-by-operations/cum-sum.md) recomputes every row after a modified row. A [filter](../how-to-guides/use-filters.md) that depends on another ticking table, such as [`whereIn`](../reference/table-operations/filter/where-in.md), can re-evaluate every row when that table changes.

### Real-world example: Chained time calculations

This example chains three tables, each built with [`update`](../reference/table-operations/select/update.md) on the one before, to show that a change propagates through the whole chain:

```groovy ticking-table order=null test-set=groovy-time-operations
// Create a table that ticks every second -- add a column that is nanos since the epoch
t1 = timeTable("PT1s").update("TsEpochNs = epochNanos(Timestamp)")

// Create a new table that adds a Java instant column from the TsEpochNs column
// epochNanosToInstant is a Java function from DateTimeUtils
t2 = t1.update("TS2 = epochNanosToInstant(TsEpochNs)")

// Do some time operations
t3 = t2.update(
    "TS3 = epochNanosToInstant(TsEpochNs + 2 * SECOND)",
    "TS4 = Timestamp + 'PT2s'",
    "D3 = TS3 - Timestamp",
    "D4 = TS4 - Timestamp"
)
```

This example illustrates several key concepts:

- **Declarative recipes** — Each [`update`](../reference/table-operations/select/update.md) specifies what to compute, not how to loop.
- **Automatic propagation** — All three tables (`t1`, `t2`, `t3`) update every second.
- **Chained operations** — Tables build on each other through the [DAG](./dag.md).
- **Real-time execution** — Each new row gets all of these columns computed as it arrives.

`D3` and `D4` both equal 2,000,000,000 nanoseconds (2 seconds), because `TS3` and `TS4` are two ways to add 2 seconds to `Timestamp`.

### Real-world example: Live rolling averages

This example keeps a 10-row [rolling average](../reference/table-operations/update-by-operations/rolling-avg.md) price for each symbol in a ticking table of simulated trades:

```groovy ticking-table order=null test-set=groovy-live-agg
import static io.deephaven.api.updateby.UpdateByOperation.RollingAvg

// Streaming example data with rolling statistics
trades = timeTable("PT0.1s").update(
    "Symbol = (i % 3 == 0) ? `AAPL` : (i % 3 == 1) ? `GOOGL` : `MSFT`",
    "Price = 100 + randomGaussian(0, 5)",
    "Size = randomInt(1, 100)"
)

// Calculate 10-row rolling average - updates in real-time!
result = trades.updateBy(RollingAvg(10, "AvgPrice = Price"), "Symbol")
```

See [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md) for more details.

This query:

- Processes streaming trade data.
- Maintains separate [rolling averages](../reference/table-operations/update-by-operations/rolling-avg.md) per symbol.
- Updates automatically as new data arrives.
- Is **extremely difficult** to implement with loops.

## How the engine executes formulas

This section covers how Deephaven stores columns, how the engine runs a formula over chunks in compiled code, and how that relates to CPU-level vectorization.

### Why the recipe approach is faster

The recipe approach avoids the per-value overhead of Groovy loops for data-processing work:

1. **Vectorization** — The engine processes data in batches (chunks), paying per-call overhead once per chunk instead of once per value.
2. **No dynamic Groovy calls or boxing** — Computation stays in generated, type-specialized Java code.
3. **Better memory access** — Sequential columnar reads are cache-friendly.
4. **Parallelization** — The engine can [split work across cores](./query-engine/parallelization.md).

### Columnar storage

Deephaven stores data by column rather than by row.

**Row-oriented** (like lists of maps):

```
[[X: 1, Y: 2], [X: 3, Y: 4], [X: 5, Y: 6]]
```

- Interleaves column `X` values with `Y` values, so reading `X` skips over `Y`.
- Gives poor cache locality for column operations.
- Leaves little room for [Single Instruction, Multiple Data (SIMD)](#cpu-level-vectorization) instructions.

**Columnar** (like Deephaven):

```
X: [1, 3, 5]
Y: [2, 4, 6]
```

- Stores column `X` values next to each other in memory.
- Gives excellent cache locality.
- Gives the [just-in-time (JIT) compiler](#jit-compilation) a chance to use SIMD instructions.

### The chunk architecture

Deephaven moves data in `Chunk` objects, the contiguous blocks of typed values from one column described in [Deephaven: Declarative and chunked](#deephaven-declarative-and-chunked).

When you write:

```groovy skip-test
t.update("Y = X * 2")
```

The engine:

1. Reads column `X` in chunks (e.g., 4,096 values at a time).
2. Applies the formula to each value in the chunk, in compiled code.
3. Writes results to column `Y` in chunks.

Because each step works on a whole chunk, the engine pays the per-call overhead of reading, computing, and writing once per chunk rather than once per row.

### Query compilation

Under the hood, Deephaven:

1. **Parses** your query string into an Abstract Syntax Tree (AST).
2. **Analyzes** the AST to determine dependencies and types.
3. **Generates** Java code for the formula.
4. **Compiles** the generated code.
5. **Executes** the compiled code on chunks of data.

The one exception to these steps is a formula whose right-hand side is only the name of an existing column, such as `Y = X`. Deephaven treats it as a column reference, skips parsing and code generation, and compiles no code. Deephaven compiles every other expression, including `Y = X * 2`.

For example, if `X` is an `int` column, `"Y = X * 2"` becomes code shaped like this conceptual sketch. The real generated class has more parts, but the shape is the same: one pass over each chunk that applies the formula to every value.

```java
// Conceptual sketch of the generated code (simplified)
class GeneratedFormula {
    void fillChunk(IntChunk x, WritableIntChunk output) {
        for (int pos = 0; pos < x.size(); pos++) {
            output.set(pos, applyFormulaPerItem(x.get(pos)));
        }
    }

    int applyFormulaPerItem(int X) {
        return multiply(X, 2); // null-aware multiplication from the query library
    }
}
```

This compiled code:

- Makes no dynamic Groovy calls and boxes no primitive values.
- Runs as Java Virtual Machine (JVM) bytecode that the JIT compiler can optimize.

### JIT compilation

The JVM uses JIT compilation to optimize hot code paths. For Deephaven queries:

1. **Initial execution** — The JVM runs the formula's compiled bytecode in its own bytecode interpreter, without dynamic Groovy calls.
2. **Profiling** — The JVM identifies hot methods.
3. **Compilation** — The JIT compiler compiles hot methods to native code.
4. **Optimization** — The compiler can apply optimizations such as loop unrolling and [SIMD instructions](#cpu-level-vectorization).

This means:

- The first chunks may run more slowly, before the JIT has compiled the formula's hot methods to native code.
- Later chunks run the optimized native code.
- Long-running queries benefit most.

### CPU-level vectorization

Modern CPUs have special instructions that operate on multiple data elements simultaneously. For example, instead of performing one addition at a time:

```
Regular: A + B = C (one addition)
```

With a SIMD instruction, a CPU can do:

```
SIMD: [A1, A2, A3, A4] + [B1, B2, B3, B4] = [C1, C2, C3, C4] (four additions in one operation)
```

Deephaven's engine doesn't emit these SIMD instructions itself. The JVM's JIT compiler can use them when it compiles hot loops, but whether it does is up to the JVM. Deephaven's columnar storage and its code specialized for each chunk type give the JIT tight loops over primitive values, which are the loops it can vectorize.

## Memory efficiency

### No intermediate objects

A Groovy loop or `collect` call creates one boxed object per value:

```groovy skip-test
// Creates 1,000,000 Long objects!
results = (0L..<1_000_000L).collect { it * it }
```

The recipe approach stores values in primitive arrays, without boxing each value:

```groovy order=null test-set=groovy-memory-no-objects
// No intermediate objects created for data!
result = emptyTable(1_000_000).update("XSquared = ii * ii")
```

[`ii`](../reference/query-language/variables/special-variables.md) is the same row index as `i`, as a `long`, which keeps `ii * ii` from overflowing an `int` at 1,000,000 rows.

### Column sharing

A `ColumnSource` is the object that holds a column's data, and a [`RowSet`](./table-update-model.md#sparse-row-sets) is the set of row keys a table contains. [`update`](../reference/table-operations/select/update.md) and [`where`](../reference/table-operations/filter/where.md) reuse the source table's `ColumnSource` objects instead of copying them:

```groovy order=t1,t2,t3 test-set=groovy-memory-efficient
t1 = emptyTable(1_000_000).update("X = i")

// t2 shares the X column with t1 - no copy!
t2 = t1.update("Y = X * 2")

// t3 also shares the X column - still no copy!
t3 = t1.where("X > 500000")
```

See [`where`](../reference/table-operations/filter/where.md) for more details.

- `t2` adds only the new `Y` column. It reuses `t1`'s [`RowSet`](./table-update-model.md#sparse-row-sets) because `t2` contains the same rows as `t1`.
- `t3` reuses `X` with a new, filtered [`RowSet`](./table-update-model.md#sparse-row-sets).

## Common patterns

### Pattern: Element-wise operations

This query computes the length of the hypotenuse for each row from two columns:

```groovy order=result test-set=groovy-element-wise
result = emptyTable(10).update(
    "X = i",
    "Y = i * 10",
    "Z = sqrt(X * X + Y * Y)"  // Pythagorean theorem
)
```

The engine computes `Z` the way [The chunk architecture](#the-chunk-architecture) describes: it reads `X` and `Y` in chunks, applies the compiled formula, and writes `Z` in chunks, with no Groovy calls and no intermediate objects.

### Pattern: Conditional operations

This query labels each row with a category chosen by a [ternary](../how-to-guides/ternary-if-how-to.md) expression:

```groovy order=result test-set=groovy-conditional
result = emptyTable(10).update(
    "X = i",
    "Category = (X < 3) ? `Small` : (X < 7) ? `Medium` : `Large`"
)
```

The [ternary operator](../how-to-guides/ternary-if-how-to.md) compiles to generated Java code, with no Groovy calls.

### Pattern: Cross-row operations

Cumulative and [rolling calculations](../how-to-guides/rolling-aggregations.md) depend on earlier rows. A formula can read a fixed offset such as [`X_[i-1]`](../reference/query-language/types/arrays.md), but it can't keep a running total or a window state correct as rows are added and modified.

[`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) keeps that state in the engine and updates it incrementally. The [update propagation example](#update-propagation-example) uses [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md), and the [live rolling averages example](#real-world-example-live-rolling-averages) uses [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md). Doing the same with loops would take much more code and would lose automatic real-time propagation.

## When to use loops

### Valid use case: Data extraction

This loop reads each row of a table and prints it in Groovy:

```groovy order=source test-set=groovy-valid-extract
source = emptyTable(5).update("X = i", "Y = X * 2")

// Extracting data to Groovy - loops are fine here
xIter = source.columnIterator("X")
yIter = source.columnIterator("Y")
try {
    while (xIter.hasNext()) {
        def x = xIter.next()
        def y = yIter.next()
        // Process in Groovy, make API calls, etc.
        println "X=${x}, Y=${y}"
    }
} finally {
    xIter.close()
    yIter.close()
}
```

This is **extraction**, not **transformation**. The data is leaving Deephaven.

### Valid use case: Control flow

This loop creates one filtered table per symbol:

```groovy order=null test-set=groovy-valid-control
source = emptyTable(100).update(
    "Symbol = (i % 3 == 0) ? `AAPL` : (i % 3 == 1) ? `GOOGL` : `MSFT`",
    "Price = 100 + randomGaussian(0, 5)"
)

// Using loops for control flow - fine!
tables = [:]
["AAPL", "GOOGL", "MSFT"].each { symbol ->
    tables[symbol] = source.where("Symbol = `${symbol}`")
}
```

You're using loops to **control** table creation, not to **transform** table data.

### Invalid use case: Column transformations

Don't compute column values in a loop. As the [loop approach](#loop-approach-doesnt-work-for-real-time) shows, the results end up in a Groovy list rather than a table, and the loop doesn't run again when data ticks. Use [`update`](../reference/table-operations/select/update.md) instead.

## Performance best practices

### 1. Prefer built-in expressions over Groovy closures

A compiled formula takes the same chunked path whether it is simple arithmetic or a chain of [ternaries](../how-to-guides/ternary-if-how-to.md). What costs more is [calling Groovy code from a formula](../how-to-guides/groovy-closures.md):

❌ **Slow** — Calls a closure for every row:

```groovy skip-test
groovyFunc = { int x -> x * 2 }

t.update("Y = groovyFunc(X)")  // Dynamic Groovy call per row - slower
```

✅ **Fast** — Stays in compiled code:

```groovy skip-test
t.update("Y = X * 2")  // Compiled code - fast!
```

See [User-defined functions](../how-to-guides/groovy-closures.md) for how to call closures from query strings when you need them.

### 2. Use appropriate operations

For [rolling](../how-to-guides/rolling-aggregations.md) and cumulative calculations, use [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md), as in [Pattern: Cross-row operations](#pattern-cross-row-operations).

For [aggregations](../how-to-guides/dedicated-aggregations.md), use dedicated methods such as [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md):

```groovy order=result test-set=groovy-best-practice-4
result = emptyTable(100)
    .update("Group = i % 5", "Value = randomDouble(0, 100)")
    .avgBy("Group")
```

### 3. Filter early

[Filter](../how-to-guides/use-filters.md) before you add computed columns, so later formulas run only on the rows you keep:

```groovy order=result test-set=groovy-best-practice-5
// Filter first to minimize data processed
result = emptyTable(1_000_000)
    .update("X = ii")
    .where("X > 900000")  // Filter early!
    .update("Y = X * X")  // Only processes ~100k rows
```

The [`where`](../reference/table-operations/filter/where.md) keeps 99,999 of the 1,000,000 rows, so the engine computes `Y` for only those rows.

## Key takeaways

1. **Think declaratively** — Specify what to compute, not how to iterate.
2. **Write recipes for real-time data** — Declarative queries update automatically.
3. **Let chunks do the work** — Chunked operations process many values per call.
4. **Prefer built-in expressions** — Formulas that don't call [closures](../how-to-guides/groovy-closures.md) stay in generated, type-specialized Java code.
5. **Use loops for extraction, not transformation** — Loop to get data out of a table, and compute column values with query operations.

**The paradigm shift:**

- **Old way:** "For each row, multiply `X` by 2 and store in `Y`".
- **Deephaven way:** "`Y` should always equal `X` times 2".

This shift unlocks:

- High performance through vectorization.
- Automatic real-time updates.
- Efficient memory usage.
- Cleaner, more maintainable code.

## Related documentation

- [Think like a Deephaven ninja](./ninja.md#replace-loops-with-table-operations)
- [Deephaven's design](./deephaven-design.md)
- [How Deephaven works: A mental model](./deephaven-mental-model.md)
- [Table update model](./table-update-model.md)
- [The live DAG](./dag.md)
- [Query engine parallelization](./query-engine/parallelization.md)
- [Crash course: Recipes, not loops!](../getting-started/crash-course/vectorization-vs-loops.md)
- [Table operations](../getting-started/crash-course/table-ops.md)
- [Dedicated aggregations](../how-to-guides/dedicated-aggregations.md)
- [Filter table data](../how-to-guides/use-filters.md)
- [Iterate over a column](../how-to-guides/extract-table-value.md#iterate-over-a-column)
- [Rolling aggregations](../how-to-guides/rolling-aggregations.md)
- [Ternary operator](../how-to-guides/ternary-if-how-to.md)
- [User-defined functions](../how-to-guides/groovy-closures.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [Arrays](../reference/query-language/types/arrays.md)
- [`avgBy`](../reference/table-operations/group-and-aggregate/avgBy.md)
- [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`epochNanos`](../reference/time/datetime/epochNanos.md)
- [`epochNanosToInstant`](../reference/time/datetime/epochNanosToInstant.md)
- [`RollingAvg`](../reference/table-operations/update-by-operations/rolling-avg.md)
- [`timeTable`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`whereIn`](../reference/table-operations/filter/where-in.md)
- [`Table` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
- [`UpdateByOperation` Javadoc](/core/javadoc/io/deephaven/api/updateby/UpdateByOperation.html)
