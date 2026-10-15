---
title: Barrier
---

A [**Barrier**](#creating-a-barrier) is a marker object that orders one column calculation or filter before another. One operation **declares** the [Barrier](#creating-a-barrier) and runs first. Other operations **respect** the [Barrier](#creating-a-barrier) and wait until the declaring operation has finished all of its rows.

In Groovy, there is no dedicated [Barrier](#creating-a-barrier) type. Any Java object can serve as a [Barrier](#creating-a-barrier). Deephaven uses the object only as a marker that matches a declaring expression to the expressions that respect it, and does not use any state the object holds.

## Why use a Barrier?

Deephaven parallelizes column calculations and filters that are eligible for it. Eligibility depends on these factors:

- Whether the operation is stateless. Deephaven assumes formulas and filters are safe to run in parallel unless they are marked serial with [`withSerial`](./ConcurrencyControl.md#withserial).
- How many rows the operation processes.
- How many threads are available.

As a result, two eligible operations run at the same time, with no guaranteed order between them. A [Barrier](#creating-a-barrier) makes one operation finish all of its rows before another starts. Typical cases are one column that populates a map that another column reads, or one column that computes a running total that another column depends on.

## Creating a Barrier

```groovy syntax
barrier = new Object()
```

A plain `new Object()` works well because it holds nothing that code could accidentally rely on. Its default `equals` is identity-based, which matters for the matching rules below.

Create one object for each ordering constraint. Every expression that respects a [Barrier](#creating-a-barrier) waits for its single declarer, so one instance cannot express two independent constraints.

### How Deephaven matches a Barrier

Deephaven matches a declaring expression to its respecting expressions differently for filters and selectables:

- A **filter** keeps its [Barrier](#creating-a-barrier) bookkeeping in a `HashSet`, so matching follows `equals` and `hashCode`. Two different instances that compare equal are the same [Barrier](#creating-a-barrier).
- A **selectable** keeps its [Barrier](#creating-a-barrier) bookkeeping in an `IdentityHashMap`, so only the exact same instance matches, regardless of `equals`. A value-equal but distinct instance does not match, and a respected [Barrier](#creating-a-barrier) can come back "not defined".

A value type that overrides `equals`, such as a `String`, a boxed number, or a `List`, risks a duplicate-declaration error for filters and a false "not defined" for selectables.

## Using a Barrier

Two methods set the roles. Both belong to the [`ConcurrencyControl`](./ConcurrencyControl.md) interface, which [`Selectable`](./Selectable.md) (used by [`select`](../../table-operations/select/select.md) and [`update`](../../table-operations/select/update.md)) and [`Filter`](./Filter.md) (used by [`where`](../../table-operations/filter/where.md)) both implement:

- [`withDeclaredBarriers(barriers)`](./ConcurrencyControl.md#withdeclaredbarriers): this operation declares the given [Barrier](#creating-a-barrier) objects. It runs to completion before any operation that respects the same [Barrier](#creating-a-barrier).
- [`withRespectedBarriers(barriers)`](./ConcurrencyControl.md#withrespectedbarriers): this operation respects the given [Barrier](#creating-a-barrier) objects. It does not start until the operation that declares each [Barrier](#creating-a-barrier) has finished.

These rules apply to every [Barrier](#creating-a-barrier):

- One expression declares a given [Barrier](#creating-a-barrier). Declaring the same instance twice is an error.
- The declaring and respecting expressions are arguments of the same [`select`](../../table-operations/select/select.md), [`update`](../../table-operations/select/update.md), or [`where`](../../table-operations/filter/where.md) call. A [Barrier](#creating-a-barrier) cannot order operations across two separate calls.
- The declaring expression comes before every respecting expression, in left-to-right order. The engine raises an error if a [Barrier](#creating-a-barrier) is respected before it is declared, or never declared at all.
- A constant-valued [`Selectable`](./Selectable.md) cannot declare or respect a [Barrier](#creating-a-barrier). This restriction does not apply to a [`Filter`](./Filter.md).

For the last rule, the engine never evaluates constants during [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) processing, so it raises an error if you try. A constant is a literal, or literals joined by binary operators (arithmetic, comparison, logical, or bitwise), optionally parenthesized, such as [`Selectable.parse`](./Selectable.md#creating-a-selectable) `("A = 1")` or [`Selectable.parse`](./Selectable.md#creating-a-selectable) `("A = 1 + 2")`.

An expression that calls a method with no arguments, such as `counter.getAndIncrement()` in the example below, is not constant. The engine evaluates it for each row, so it can declare and respect [Barriers](#creating-a-barrier).

### Barriers and serial execution

A [Barrier](#creating-a-barrier) orders operations relative to each other. It does not change how one operation processes its own rows.

[`withSerial`](./ConcurrencyControl.md#withserial) makes one operation process its rows one at a time, in row set order. An operation whose own rows could race on shared mutable state needs `withSerial`, and it also needs a [Barrier](#creating-a-barrier) when another operation must wait for it.

### Example: coordinating two columns

Two columns share a counter. Column `A` assigns IDs 0-9, and column `B` continues from 10-19.

Without a [Barrier](#creating-a-barrier), the engine guarantees no order between `A` and `B`, so `B` could end up with 0-9 while `A` gets 10-19. With a [Barrier](#creating-a-barrier), column `A` runs first and column `B` starts where `A` left off. Each column also uses [`withSerial`](./ConcurrencyControl.md#withserial), because the counter is shared mutable state:

```groovy order=t
import io.deephaven.api.Selectable
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

barrier = new Object()

// Column A: serial (preserve row-to-value order) + declares barrier (must finish first)
colA = Selectable.parse("A = counter.getAndIncrement()")
    .withSerial()
    .withDeclaredBarriers(barrier)

// Column B: serial (preserve row-to-value order) + respects barrier (waits for A)
colB = Selectable.parse("B = counter.getAndIncrement()")
    .withSerial()
    .withRespectedBarriers(barrier)

t = emptyTable(10).update([colA, colB])
```

Column `A` gets values 0-9 and column `B` gets values 10-19. The [Barrier](#creating-a-barrier) guarantees that `A` finishes before `B` starts, and [`withSerial`](./ConcurrencyControl.md#withserial) keeps each column's rows in row set order, so each row gets the counter value that matches its position.

### Example: coordinating two filters

[`Filter`](./Filter.md) objects use [Barriers](#creating-a-barrier) in [`where`](../../table-operations/filter/where.md) the same way as columns do. Here, one filter populates a cache that a second filter reads.

Neither filter needs [`withSerial`](./ConcurrencyControl.md#withserial), because a `ConcurrentHashMap` is already safe for concurrent writes to distinct keys. The [Barrier](#creating-a-barrier) is what ensures the cache is fully populated before it is read. Because `filterA` finishes all ten rows first, `useCache` finds every key and `result` keeps all ten rows:

```groovy order=result
import io.deephaven.api.filter.Filter
import java.util.concurrent.ConcurrentHashMap

cache = new ConcurrentHashMap()

initCache = { key ->
    cache[key] = "Value_${key}"
    return true
}

useCache = { key -> cache.containsKey(key) }

barrier = new Object()

// Filter A: declares barrier (must finish first)
filterA = Filter.from("(boolean)initCache(Key)")[0].withDeclaredBarriers(barrier)

// Filter B: respects barrier (waits for A to finish populating the cache)
filterB = Filter.from("(boolean)useCache(Key)")[0].withRespectedBarriers(barrier)

source = emptyTable(10).update("Key = i")
result = source.where(Filter.and(filterA, filterB))
```

### Multiple Barriers

Create one [Barrier](#creating-a-barrier) for each independent dependency. One operation can respect more than one [Barrier](#creating-a-barrier):

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

Execution order:

- `A` and `B` do not depend on each other, so the engine is free to run them concurrently.
- `D` starts after `A` finishes. It does not wait for `B`.
- `C` starts after both `A` and `B` finish.

## Implicit Barriers

An implicit [Barrier](#creating-a-barrier) is one that Deephaven adds for you. When [implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are turned on, each serial column in a [`select`](../../table-operations/select/select.md) or [`update`](../../table-operations/select/update.md) waits for every earlier serial column in the same operation, without any explicit [Barrier](#creating-a-barrier) object.

[Implicit Barriers](../../../conceptual/query-engine/parallelization.md#implicit-barriers) are off by default. The [`QueryTable.serialSelectImplicitBarriers`](../../../conceptual/query-table-configuration.md#stateless-by-default) property turns them on.

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md)
- [Query table configuration](../../../conceptual/query-table-configuration.md)
- [ConcurrencyControl](./ConcurrencyControl.md)
- [Selectable](./Selectable.md)
- [Filter](./Filter.md)
- [ConcurrencyControl Javadoc](https://deephaven.io/core/javadoc/io/deephaven/api/ConcurrencyControl.html)
