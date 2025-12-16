---
title: Vectorization and the recipe paradigm
---

Deephaven's query engine uses vectorized operations and a declarative "recipe" paradigm to achieve high performance on both static and real-time data. This guide explains the technical foundations of this approach and why it matters for your queries.

## The paradigm shift: Imperative vs declarative

### Traditional programming: Imperative SISD

In traditional programming languages like Python, C, or Java, you write **imperative** code that specifies step-by-step instructions. This is Single Instruction, Single Data (SISD) - one instruction processes one piece of data at a time:

```python skip-test
# Traditional Python: Imperative, SISD
data = [1, 2, 3, 4, 5]
results = []
for value in data:  # One iteration at a time
    result = value * value  # One calculation
    results.append(result)  # One append
```

This approach:

- Executes instructions sequentially.
- Processes one data element per instruction.
- Requires explicit loops for multiple elements.
- Creates intermediate Python objects for each value.

### Deephaven: Declarative SIMD

Deephaven uses a **declarative** approach based on Single Instruction, Multiple Data (SIMD) - one instruction processes multiple data elements simultaneously:

```python order=result test-set=simd-example
from deephaven import empty_table

# Deephaven: Declarative, SIMD-capable
result = empty_table(5).update(["X = i + 1", "XSquared = X * X"])
```

This approach:

- Specifies **what** to compute, not **how**.
- Processes data in optimized chunks (vectorization).
- Enables CPU-level SIMD instructions when available.
- Avoids intermediate Python objects.

### Performance comparison

Let's compare the approaches with timing:

```python order=:log,dh_result test-set=performance-comparison
import time
import numpy as np
from deephaven import empty_table
from deephaven.numpy import to_numpy

# Create test data
size = 1_000_000

# Time the loop approach (via NumPy for fair comparison)
start = time.time()
x_array = np.arange(size)
result_array = np.empty(size)
for i in range(size):
    result_array[i] = x_array[i] * x_array[i]
loop_time = time.time() - start
print(f"Loop approach: {loop_time:.4f} seconds")

# Time the Deephaven recipe approach
start = time.time()
dh_result = empty_table(size).update(["X = (long)i", "XSquared = X * X"])
# Force computation by reading a value
_ = dh_result.head(1)
recipe_time = time.time() - start
print(f"Recipe approach: {recipe_time:.4f} seconds")
print(f"Speedup: {loop_time / recipe_time:.2f}x")

# Store results for display
pandas_result = f"Loop: {loop_time:.4f}s"
```

The recipe approach is typically **much faster** because:

1. **Vectorization** - Processes multiple values per CPU instruction.
2. **No Python overhead** - Computation stays in compiled code.
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
SIMD: [A1, A2, A3, A4] + [B1, B2, B3, B4] = [C1, C2, C3, C4] (four additions in one instruction)
```

### How Deephaven enables vectorization

Deephaven's engine is designed to enable CPU vectorization:

1. **Columnar storage** - Data for a column is stored contiguously in memory.
2. **Chunk-oriented processing** - Operations work on blocks of data at once.
3. **Type-specific operations** - Specialized code for each data type avoids type checks in inner loops.
4. **JIT compilation** - The JVM can optimize and vectorize hot code paths.

> By structuring our engine operations as chunk-oriented kernels, we allow the JVM's JIT compiler to vectorize computations where possible.

### The Chunk architecture

Deephaven moves data using a structure called a **Chunk**:

```
Chunk = contiguous block of typed data (e.g., 4096 doubles)
```

When you write:

```python skip-test
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

```python order=t1,t2 test-set=recipe-spec
from deephaven import time_table

t1 = time_table("PT1s").update("X = i")
t2 = t1.update("Y = X * 2")
```

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

This is **fundamentally impossible** with imperative loops - a loop executes once and stops!

### Update propagation example

```python ticking-table order=null test-set=update-propagation
from deephaven import time_table
from deephaven.updateby import cum_sum

# Create a ticking table (adds row every second)
source = time_table("PT1s").update(["X = i", "XSquared = X * X"])

# Add a cumulative sum - updates automatically!
result = source.update_by(cum_sum("SumX = X"))
```

Watch this table in the UI. Every second:

- A new row arrives in `source`.
- `XSquared` is computed for the new row.
- `SumX` is updated for the new row.
- **You wrote the recipe once, it runs forever**.

### Real-world example: Time operations

Here's a more complex example that demonstrates multiple concepts working together - time manipulation, chained operations, and Java function integration:

```python ticking-table order=null test-set=time-operations
from deephaven import time_table

# Create a table that ticks every second -- add a column that is nanos since the epoch
t1 = time_table("PT1s").update(["TsEpochNs = epochNanos(Timestamp)"])

# Create a new table that adds a Java instant column from the TsEpochNs column
# epochNanosToInstant is a Java function from DateTimeUtils
t2 = t1.update("TS2 = epochNanosToInstant(TsEpochNs)")

# Do some time operations
t3 = t2.update(
    [
        "TS3 = epochNanosToInstant(TsEpochNs + 2*SECOND)",
        "TS4 = Timestamp + 'PT2s'",
        "D3 = TS3-Timestamp",
        "D4 = TS4-Timestamp",
    ]
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

- Has no Python overhead.
- Can be JIT-optimized by the JVM.
- Can be vectorized by the CPU.
- Runs at native speed.

## Real-time processing: The killer feature

### Why recipes enable real-time

The recipe paradigm makes real-time processing trivial. Compare:

#### Loop approach (doesn't work for real-time):

```python skip-test
# ❌ This only runs once!
results = []
for row in source.iter_tuple():
    results.append(row.X * 2)
# What happens when new data arrives? Nothing!
```

#### Recipe approach (automatically handles updates):

```python skip-test
# ✓ This updates automatically!
result = source.update("Y = X * 2")
# New data arrives? Y is computed for new rows automatically!
```

### Incremental computation

The engine is smart about updates. It doesn't recompute everything - it only processes what changed:

```python ticking-table order=null test-set=incremental
from deephaven import time_table

# Ticking source with multiple operations
source = time_table("PT1s").update(["X = i", "Y = X * X", "Z = Y + 10", "W = Z * 2"])
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

### Real-world example: Live aggregations

```python ticking-table order=null test-set=live-agg
from deephaven import time_table
from deephaven.updateby import rolling_avg_tick

# Streaming data with rolling statistics
trades = time_table("PT0.1s").update(
    [
        "Symbol = (i % 3 == 0) ? `AAPL` : (i % 3 == 1) ? `GOOGL` : `MSFT`",
        "Price = 100 + randomGaussian(0, 5)",
        "Size = randomInt(1, 100)",
    ]
)

# Calculate 10-row rolling average - updates in real-time!
result = trades.update_by(
    rolling_avg_tick("AvgPrice = Price", rev_ticks=10), by="Symbol"
)
```

This query:

- Processes streaming trade data.
- Maintains separate rolling averages per symbol.
- Updates automatically as new data arrives.
- Would be **extremely difficult** to implement with loops.

## Memory efficiency

### No intermediate Python objects

Loop approach creates Python objects:

```python skip-test
# Creates 1,000,000 Python int objects!
results = [x * x for x in range(1_000_000)]
```

Recipe approach stays in native memory:

```python skip-test
# No Python objects created for data!
result = empty_table(1_000_000).update("XSquared = i * i")
```

### Column sharing and copy-on-write

Deephaven uses smart memory management:

```python order=t1,t2,t3 test-set=memory-efficient
from deephaven import empty_table

t1 = empty_table(1_000_000).update("X = i")

# t2 shares the X column with t1 - no copy!
t2 = t1.update("Y = X * 2")

# t3 also shares the X column - still no copy!
t3 = t1.where("X > 500000")
```

> A table may share its RowSet with any other table in its update graph that contains the same row keys... This sharing capability represents an important optimization that avoids some data processing or copying work.

### Columnar vs row-oriented storage

**Row-oriented** (like Python lists of dicts):

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

```python order=result test-set=element-wise
from deephaven import empty_table

result = empty_table(10).update(
    [
        "X = i",
        "Y = i * 10",
        "Z = sqrt(X * X + Y * Y)",  # Pythagorean theorem
    ]
)
```

Engine execution:

1. Reads `X` and `Y` columns in chunks.
2. Applies vectorized operations chunk-by-chunk.
3. Writes results to `Z` column.
4. No Python overhead, no intermediate objects.

### Pattern: Conditional operations

```python order=result test-set=conditional
from deephaven import empty_table

result = empty_table(10).update(
    ["X = i", "Category = (X < 3) ? `Small` : (X < 7) ? `Medium` : `Large`"]
)
```

The ternary operator compiles to:

- Efficient branch prediction.
- No Python if/else overhead.
- Vectorized where possible.

### Pattern: Cross-row operations

```python order=result test-set=cross-row
from deephaven import empty_table
from deephaven.updateby import cum_sum, rolling_avg_tick

result = (
    empty_table(10)
    .update("X = i + 1")
    .update_by([cum_sum("CumSum = X"), rolling_avg_tick("RollingAvg = X", rev_ticks=3)])
)
```

These operations:

- Maintain state efficiently.
- Update incrementally when data ticks.
- Cannot be expressed with simple loops.
- Are highly optimized in the engine.

## When loops ARE appropriate

### Valid use case: Data extraction

```python order=source test-set=valid-extract
from deephaven import empty_table

source = empty_table(5).update(["X = i", "Y = X * 2"])

# Extracting data to Python - loops are fine here
for row in source.iter_tuple():
    # Process in Python, make API calls, etc.
    print(f"X={row.X}, Y={row.Y}")
```

This is **extraction**, not **transformation**. The data is leaving Deephaven.

### Valid use case: Control flow

```python skip-test test-set=valid-control
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Symbol = (i % 3 == 0) ? `AAPL` : (i % 3 == 1) ? `GOOGL` : `MSFT`",
        "Price = 100 + randomGaussian(0, 5)",
    ]
)

# Using loops for control flow - fine!
tables = {}
for symbol in ["AAPL", "GOOGL", "MSFT"]:
    tables[symbol] = source.where(f"Symbol = `{symbol}`")
```

You're using loops to **control** table creation, not to **transform** table data.

### Invalid use case: Column transformations

```python skip-test
# ❌ NEVER do this!
results = []
for row in source.iter_tuple():
    results.append(row.X * row.Y)

# Now what? How do you get this back into a table?
# And what happens when data ticks?
```

Use `.update()` instead!

## Performance best practices

### 1. Let the engine vectorize

✅ **Good** - Vectorizable:

```python order=good test-set=best-practice-1
from deephaven import empty_table

good = empty_table(10).update(
    [
        "X = i",
        "Y = X * 2 + 5",  # Simple arithmetic - vectorizes well
    ]
)
```

⚠️ **Careful** - Complex functions may not vectorize:

```python order=careful test-set=best-practice-2
from deephaven import empty_table


def complex_calculation(x):
    # Complex Python function - called per value, not vectorized
    result = 0
    for i in range(int(x)):
        result += i**2
    return result


careful = empty_table(10).update(
    [
        "X = i + 1",
        "Y = complex_calculation(X)",  # Python function - not vectorized
    ]
)
```

### 2. Minimize cross-language calls

❌ **Slow** - Calls Python for every row:

```python skip-test
def python_func(x):
    return x * 2


t.update("Y = python_func(X)")  # Python call per row - slow!
```

✅ **Fast** - Stays in compiled code:

```python skip-test
t.update("Y = X * 2")  # Compiled code - fast!
```

### 3. Use appropriate operations

For rolling calculations, use `update_by`:

```python order=result test-set=best-practice-3
from deephaven import empty_table
from deephaven.updateby import rolling_avg_tick

result = (
    empty_table(100)
    .update("X = i")
    .update_by(rolling_avg_tick("AvgX = X", rev_ticks=10))
)
```

For aggregations, use dedicated methods:

```python order=result test-set=best-practice-4
from deephaven import empty_table

result = (
    empty_table(100)
    .update(["Group = i % 5", "Value = randomDouble(0, 100)"])
    .avg_by("Group")
)
```

### 4. Filter early

```python order=result test-set=best-practice-5
from deephaven import empty_table

# Filter first to minimize data processed
result = (
    empty_table(1_000_000)
    .update("X = i")
    .where("X > 900000")  # Filter early!
    .update("Y = X * X")  # Only processes 100k rows
)
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
3. **Vectorization = performance** - SIMD operations process multiple elements at once.
4. **No Python overhead** - Computation stays in compiled code.
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
- [Crash course: Vectorization](../getting-started/crash-course/vectorization-vs-loops.md)
- [Deephaven's design](./deephaven-design.md)
- [Table update model](./table-update-model.md)
- [Query engine parallelization](./query-engine/parallelization.md)
- [Table operations](../getting-started/crash-course/table-ops.md)
- [`update_by` operations](../how-to-guides/rolling-aggregations.md)
