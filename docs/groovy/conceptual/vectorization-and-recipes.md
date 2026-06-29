---
title: Vectorization and the recipe paradigm
---

Deephaven's query engine uses vectorized operations and a declarative "recipe" paradigm to achieve high performance on both static and real-time data. This guide explains the technical foundations of this approach and why it matters for your queries.

**The recipe paradigm**: Instead of writing step-by-step instructions that process data one element at a time, you define what result you want — like a recipe that describes the finished dish. Deephaven's engine then figures out how to compute it efficiently, processing data in optimized batches.

## The paradigm shift: Imperative vs declarative

### Traditional programming: Imperative SISD

In traditional programming, you write **imperative** code that processes one data element at a time. This is Single Instruction, Single Data (SISD) — each instruction operates on a single value:

```groovy skip-test
// Traditional Groovy: Imperative, SISD
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

Deephaven uses a **declarative** approach. The engine processes data in optimized chunks, applying one operation across many values at once:

```groovy order=result test-set=groovy-simd-example
// Deephaven: Declarative, chunked
result = emptyTable(5).update("X = i + 1", "XSquared = X * X")
```

See [`emptyTable`](../reference/table-operations/create/emptyTable.md) and [`update`](../reference/table-operations/select/update.md) for more details.

This approach:

- Specifies **what** to compute, not **how**.
- Processes data in optimized chunks (vectorization).
- Avoids intermediate objects.

### Why the recipe approach is faster

The recipe approach avoids the overhead of interpreted loops for data-processing work:

1. **Vectorization** - Processes multiple values per CPU instruction.
2. **No interpreter overhead** - Computation stays in compiled code.
3. **Better memory access** - Sequential columnar reads are cache-friendly.
4. **Parallelization** - Engine can split work across cores.

## What is vectorization?

### CPU-level vectorization

Modern CPUs have special instructions that operate on multiple data elements simultaneously. For example, instead of adding two numbers at a time:

```
Regular: A + B = C (one addition)
```

Vectorized CPUs can do:

```
Chunked: [A1, A2, A3, A4] + [B1, B2, B3, B4] = [C1, C2, C3, C4] (processed as a batch)
```

### How Deephaven enables vectorization

Deephaven's engine is designed to enable CPU vectorization:

1. **Columnar storage** - Data for a column is stored contiguously in memory.
2. **Chunk-oriented processing** - Operations work on blocks of data at once.
3. **Type-specific operations** - Specialized code for each data type avoids type checks in inner loops.
4. **JIT compilation** - The JVM can optimize and vectorize hot code paths.

By structuring engine operations as chunk-oriented kernels, Deephaven allows the JVM's JIT compiler to vectorize computations where possible.

### The Chunk architecture

Deephaven moves data using a structure called a **Chunk**:

```
Chunk = contiguous block of typed data (e.g., 4096 doubles)
```

When you write:

```groovy skip-test
t.update("Y = X * 2")
```

The engine:

1. Reads column `X` in chunks (e.g., 4096 values at a time).
2. Applies the operation to each chunk (vectorized multiplication).
3. Writes results to column `Y` in chunks.

This approach:

- Amortizes memory access costs.
- Enables vectorization.
- Reduces per-element overhead.
- Works efficiently with CPU caches.

## The recipe paradigm: How it works

### Recipes are specifications

When you write a Deephaven query:

```groovy order=null test-set=groovy-recipe-spec
t1 = timeTable("PT1s").update("X = i")
t2 = t1.update("Y = X * 2")
```

See [`timeTable`](../reference/table-operations/create/timeTable.md) for more details.

You're creating a **specification** (recipe) that says "Y should always equal X times 2". You're **not** executing a loop or directly computing values.

### Lazy evaluation and dependency tracking

The engine builds a **Directed Acyclic Graph (DAG)** of dependencies:

```
t1 (source) → t2 (derived)
   ↓             ↓
   X      →      Y = X * 2
```

When data ticks:

1. New rows arrive in `t1`.
2. Engine detects that `t2` depends on `t1`.
3. Engine automatically computes `Y` for the new rows.
4. Updates propagate through the DAG.

This requires significant additional infrastructure with imperative loops — because loops execute once and stop, you need to build your own subscription and recomputation logic.

### Update propagation example

```groovy ticking-table order=null test-set=groovy-update-propagation
import static io.deephaven.api.updateby.UpdateByOperation.CumSum

// Create a ticking table (adds row every second)
source = timeTable("PT1s").update("X = i", "XSquared = X * X")

// Add a cumulative sum - updates automatically!
result = source.updateBy(CumSum("SumX = X"))
```

See [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) and [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) for more details.

Watch this table in the UI. Every second:

- A new row arrives in `source`.
- `XSquared` is computed for the new row.
- `SumX` is updated for the new row.

**Write the recipe once, and it runs forever**.

### Real-world example: Time operations

Here's a more complex example that demonstrates multiple concepts working together - time manipulation, chained operations, and Java function integration:

```groovy ticking-table order=null test-set=groovy-time-operations
// Create a table that ticks every second -- add a column that is nanos since the epoch
t1 = timeTable("PT1s").update("TsEpochNs = epochNanos(Timestamp)")

// Create a new table that adds a Java instant column from the TsEpochNs column
// epochNanosToInstant is a Java function from DateTimeUtils
t2 = t1.update("TS2 = epochNanosToInstant(TsEpochNs)")

// Do some time operations
t3 = t2.update(
    "TS3 = epochNanosToInstant(TsEpochNs + 2*SECOND)",
    "TS4 = Timestamp + 'PT2s'",
    "D3 = TS3-Timestamp",
    "D4 = TS4-Timestamp"
)
```

This example illustrates several key concepts:

- **Declarative recipes** - Each `.update()` specifies what to compute, not how to loop.
- **Automatic propagation** - All three tables (`t1`, `t2`, `t3`) update every second.
- **Chained operations** - Tables build on each other through the DAG.
- **Real-time execution** - New rows trigger automatic recomputation.
- **Java integration** - Using `epochNanosToInstant()` from [DateTimeUtils](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html).
- **Type conversions** - Converting between epoch nanos, Instants, and timestamps.

Every second, a new row arrives and all formulas execute automatically. The engine handles:

- Dependency tracking between `t1` → `t2` → `t3`.
- Type conversions and time arithmetic.
- Efficient execution of all operations.

### Query compilation

Under the hood, Deephaven:

1. **Parses** your query string into an Abstract Syntax Tree (AST).
2. **Analyzes** the AST to determine dependencies and types.
3. **Generates** optimized Java code (or uses pre-compiled classes for simple operations).
4. **Compiles** the generated code.
5. **Executes** the compiled code on chunks of data.

For example, `"Y = X * 2"` might become:

```java
// Generated Java code (simplified)
class GeneratedFormula {
    void apply(DoubleChunk input, WritableDoubleChunk output) {
        for (int i = 0; i < input.size(); i++) {
            output.set(i, input.get(i) * 2.0);
        }
    }
}
```

This compiled code:

- Has no interpreter overhead.
- Can be JIT-optimized by the JVM.
- Can be vectorized by the CPU.
- Runs at native speed.

## Real-time processing: The killer feature

### Why recipes enable real-time

The recipe paradigm makes real-time processing trivial. Compare:

#### Loop approach (doesn't work for real-time):

```groovy skip-test
// ❌ This only runs once!
results = []
source.columnIterator("X").forEachRemaining { x ->
    results.add(x * 2)
}
// What happens when new data arrives? Nothing!
```

#### Recipe approach (automatically handles updates):

```groovy skip-test
// ✓ This updates automatically!
result = source.update("Y = X * 2")
// New data arrives? Y is computed for new rows automatically!
```

### Incremental computation

The engine is smart about updates. It doesn't recompute everything - it only processes what changed:

```groovy ticking-table order=null test-set=groovy-incremental
// Ticking source with multiple operations
source = timeTable("PT1s").update("X = i", "Y = X * X", "Z = Y + 10", "W = Z * 2")
```

When a new row arrives:

- Only the new row is processed.
- All formulas are evaluated for that row.
- Results are appended to output columns.
- **Nothing else is recomputed**.

For updates or modifications:

- Only affected rows are recomputed.
- Dependencies are tracked automatically.
- Downstream tables update accordingly.

### Example: Live aggregations

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

This query:

- Processes streaming trade data.
- Maintains separate rolling averages per symbol.
- Updates automatically as new data arrives.
- Would be **extremely difficult** to implement with loops.

## Memory efficiency

### No intermediate objects

Loop approach creates objects:

```groovy skip-test
// Creates 1,000,000 Integer objects!
results = (0..<1_000_000).collect { it * it }
```

Recipe approach stays in native memory:

```groovy order=null test-set=groovy-memory-no-objects
// No intermediate objects created for data!
result = emptyTable(1_000_000).update("XSquared = i * i")
```

### Column sharing and copy-on-write

Deephaven uses smart memory management:

```groovy order=t1,t2,t3 test-set=groovy-memory-efficient
t1 = emptyTable(1_000_000).update("X = i")

// t2 shares the X column with t1 - no copy!
t2 = t1.update("Y = X * 2")

// t3 also shares the X column - still no copy!
t3 = t1.where("X > 500000")
```

See [`where`](../reference/table-operations/filter/where.md) for more details.

Deephaven tables can share their `RowSet` with other tables in the same update graph that contain the same row keys. This sharing avoids copying data unnecessarily.

### Columnar vs row-oriented storage

**Row-oriented** (like lists of maps):

```
[{X: 1, Y: 2}, {X: 3, Y: 4}, {X: 5, Y: 6}]
```

- Accessing column X requires skipping Y values.
- Poor cache locality for column operations.
- Can't vectorize efficiently.

**Columnar** (like Deephaven):

```
X: [1, 3, 5]
Y: [2, 4, 6]
```

- Column X is contiguous in memory.
- Excellent cache locality.
- Enables vectorization.

## Common patterns: Technical details

### Pattern: Element-wise operations

```groovy order=result test-set=groovy-element-wise
result = emptyTable(10).update(
    "X = i",
    "Y = i * 10",
    "Z = sqrt(X * X + Y * Y)"  // Pythagorean theorem
)
```

Engine execution:

1. Reads `X` and `Y` columns in chunks.
2. Applies vectorized operations chunk-by-chunk.
3. Writes results to `Z` column.
4. No interpreter overhead, no intermediate objects.

### Pattern: Conditional operations

```groovy order=result test-set=groovy-conditional
result = emptyTable(10).update(
    "X = i",
    "Category = (X < 3) ? `Small` : (X < 7) ? `Medium` : `Large`"
)
```

The ternary operator compiles to generated Java code with no interpreter overhead.

### Pattern: Cross-row operations

```groovy order=result test-set=groovy-cross-row
import static io.deephaven.api.updateby.UpdateByOperation.CumSum
import static io.deephaven.api.updateby.UpdateByOperation.RollingAvg

result = emptyTable(10)
    .update("X = i + 1")
    .updateBy([CumSum("CumSum = X"), RollingAvg(3, "RollingAvg = X")])
```

These operations:

- Maintain state efficiently.
- Update incrementally when data ticks.
- Would require significantly more code to implement with loops, and would lose automatic real-time propagation.
- Are highly optimized in the engine.

## When loops ARE appropriate

### Valid use case: Data extraction

```groovy skip-test
source = emptyTable(5).update("X = i", "Y = X * 2")

// Extracting data to Groovy - loops are fine here
iter = source.columnIterator("X")
while (iter.hasNext()) {
    x = iter.next()
    // Process in Groovy, make API calls, etc.
    println "X=${x}"
}
```

This is **extraction**, not **transformation**. The data is leaving Deephaven.

### Valid use case: Control flow

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

```groovy skip-test should-fail
// ❌ NEVER do this!
results = []
source.columnIterator("X").forEachRemaining { x ->
    results.add(x * source.getColumnSource("Y").get(/* ??? */))
}

// Now what? How do you get this back into a table?
// And what happens when data ticks?
```

Use `.update()` instead!

## Performance best practices

### 1. Let the engine vectorize

✅ **Good** - Vectorizable:

```groovy order=good test-set=groovy-best-practice-1
good = emptyTable(10).update(
    "X = i",
    "Y = X * 2 + 5"  // Simple arithmetic - vectorizes well
)
```

⚠️ **Careful** - Complex logic in query strings may not vectorize:

```groovy skip-test
// Complex inline logic - may not vectorize efficiently
careful = emptyTable(10).update(
    "X = i + 1",
    "Y = (int)(X > 5 ? X * X : X + 1)"  // Complex ternary chains
)
```

### 2. Minimize cross-language calls

❌ **Slow** - Calls closure for every row:

```groovy skip-test
def groovyFunc(x) {
    return x * 2
}

t.update("Y = groovyFunc(X)")  // Closure call per row - slow!
```

✅ **Fast** - Stays in compiled code:

```groovy skip-test
t.update("Y = X * 2")  // Compiled code - fast!
```

### 3. Use appropriate operations

For rolling calculations, use `updateBy`:

```groovy order=result test-set=groovy-best-practice-3
import static io.deephaven.api.updateby.UpdateByOperation.RollingAvg

result = emptyTable(100)
    .update("X = i")
    .updateBy(RollingAvg(10, "AvgX = X"))
```

For aggregations, use dedicated methods:

```groovy order=result test-set=groovy-best-practice-4
result = emptyTable(100)
    .update("Group = i % 5", "Value = randomDouble(0, 100)")
    .avgBy("Group")
```

### 4. Filter early

```groovy order=result test-set=groovy-best-practice-5
// Filter first to minimize data processed
result = emptyTable(1_000_000)
    .update("X = i")
    .where("X > 900000")  // Filter early!
    .update("Y = X * X")  // Only processes 100k rows
```

## Advanced: JVM and vectorization

### JIT compilation

The Java Virtual Machine (JVM) uses Just-In-Time (JIT) compilation to optimize hot code paths. For Deephaven queries:

1. **Initial execution** - Code is interpreted.
2. **Profiling** - JVM identifies hot methods.
3. **Compilation** - Hot methods are compiled to native code.
4. **Optimization** - Compiler applies vectorization, loop unrolling, etc.

This means:

- First execution may be slower (compilation overhead).
- Subsequent executions are much faster.
- Long-running queries benefit most.

## Key takeaways

1. **Think declaratively** - Specify what to compute, not how to iterate.
2. **Recipes enable real-time** - Declarative queries update automatically.
3. **Vectorization = performance** - Chunked operations process multiple elements at once.
4. **No interpreter overhead** - Computation stays in compiled code.
5. **Use loops for extraction, not transformation** - Get data out, don't transform inside loops.

**The paradigm shift:**

- **Old way:** "For each row, multiply X by 2 and store in Y".
- **Deephaven way:** "Y should always equal X times 2".

This shift unlocks:

- High performance through vectorization.
- Automatic real-time updates.
- Cleaner, more maintainable code.
- Efficient memory usage.

## Related documentation

- [Think like a Deephaven ninja](./ninja.md#looping-dont-do-it)
- [Deephaven's design](./deephaven-design.md)
- [Table update model](./table-update-model.md)
- [Query engine parallelization](./query-engine/parallelization.md)
