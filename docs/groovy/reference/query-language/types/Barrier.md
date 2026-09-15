---
title: Barrier
---

A **barrier** is a synchronization primitive for coordinating execution order between column calculations and filters. In Groovy, there is no dedicated barrier type — **any Java object** can serve as a barrier. The object itself carries no data; it's a unique marker you create once and share between the operations that need to coordinate.

## Why use a barrier?

Deephaven parallelizes most column calculations and filters by default. A barrier lets you enforce that one operation completes all of its rows before another operation starts — for example, when one column populates a map that another column reads from, or computes a running total that another column depends on.

A barrier alone does not force either operation to run serially. If an operation has shared mutable state that could race across its own rows, you typically need **both** [`withSerial`](./ConcurrencyControl.md#withserial) (for sequential row processing within that operation) **and** a barrier (for ordering between operations).

## Creating a barrier

```groovy syntax
barrier = new Object()
```

Create one object per ordering constraint you need. Reusing the same instance for unrelated constraints would incorrectly link them together; use a separate instance for each independent constraint.

## Using a barrier

One operation **declares** the barrier — it goes first. Another operation **respects** the barrier — it waits until every operation that declares that barrier has finished all of its rows. Both roles are part of the [`ConcurrencyControl`](./ConcurrencyControl.md) interface, which [`Selectable`](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) (used by `select` and `update`) and [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) (used by `where`) both implement:

- [`withDeclaredBarriers(barriers)`](./ConcurrencyControl.md#withdeclaredbarriers) — this operation declares the given barrier(s); it runs to completion before any operation that respects the same barrier.
- [`withRespectedBarriers(barriers)`](./ConcurrencyControl.md#withrespectedbarriers) — this operation respects the given barrier(s); it doesn't start until every operation that declares the barrier has finished.

### Example: coordinating two columns

Consider two columns that share a counter, where column `A` should assign IDs 0-9 and column `B` should continue from 10-19. Without a barrier, both columns would start simultaneously, both read the counter starting at 0, and produce overlapping, incorrect results. With a barrier, column `A` runs first (0-9), then column `B` starts where `A` left off (10-19):

```groovy order=t
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

barrier = new Object()

// Column A: serial (protect counter) + declares barrier (must finish first)
colA = Selectable.parse("A = counter.getAndIncrement()")
    .withSerial()
    .withDeclaredBarriers(barrier)

// Column B: serial (protect counter) + respects barrier (waits for A)
colB = Selectable.parse("B = counter.getAndIncrement()")
    .withSerial()
    .withRespectedBarriers(barrier)

t = emptyTable(10).update([colA, colB])
```

Column `A` gets values 0-9. Column `B` gets values 10-19. Without the barrier, both columns would race and produce unpredictable results. Without `withSerial`, rows within each column would also race.

### Example: coordinating two filters

Barriers work the same way for [`Filter`](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) objects in `where` operations. Here, one filter populates a cache that a second filter depends on:

```groovy order=result
import io.deephaven.api.filter.Filter

cache = [:]

initCache = { key ->
    cache[key] = "Value_${key}"
    return true
}

useCache = { key -> cache.containsKey(key) }

barrier = new Object()

// Filter A: serial (map writes aren't thread-safe) + declares barrier (must finish first)
filterA = Filter.from("(boolean)initCache(Key)")[0].withSerial().withDeclaredBarriers(barrier)

// Filter B: respects barrier (waits for A to finish populating the cache)
filterB = Filter.from("(boolean)useCache(Key)")[0].withRespectedBarriers(barrier)

source = emptyTable(10).update("Key = i")
result = source.where(Filter.and(filterA, filterB))
```

### Multiple barriers

You can create multiple barrier objects when operations have different dependencies. Each barrier is an independent constraint, and one operation can respect more than one:

```groovy order=t
import io.deephaven.api.Selectable

barrierA = new Object()
barrierB = new Object()

// Column A declares barrierA
colA = Selectable.parse("A = i * 2").withDeclaredBarriers(barrierA)

// Column B declares barrierB
colB = Selectable.parse("B = i * 3").withDeclaredBarriers(barrierB)

// Column C respects BOTH barriers — waits for A and B to finish
colC = Selectable.parse("C = i * 4").withRespectedBarriers(barrierA, barrierB)

// Column D respects only barrierA — waits for A, but not B
colD = Selectable.parse("D = i * 5").withRespectedBarriers(barrierA)

t = emptyTable(10).update([colA, colB, colC, colD])
```

Execution order: `A` and `B` run in parallel (they don't depend on each other); `D` starts after `A` finishes (doesn't wait for `B`); `C` starts after both `A` and `B` finish.

## Related documentation

- [ConcurrencyControl](./ConcurrencyControl.md) — The interface that provides `withDeclaredBarriers` and `withRespectedBarriers`
- [Selectable Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/Selectable.html) — Uses barriers to coordinate column calculations
- [Filter Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/filter/Filter.html) — Uses barriers to coordinate filters
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
