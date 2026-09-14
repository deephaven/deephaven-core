# Finding 14 — building an error message with `Strings.of` discarded the real exception

**Severity:** medium — the actual cause of a failure was replaced by an unrelated internal error, and
the two data-index implementations reported different exceptions for one underlying problem.
**Status:** **FIXED**, with a test pinning the constraint.
**Repro test:** `engine/table/src/test/java/io/deephaven/engine/table/impl/select/WhereFilterStringsRenderingTest.java`
**Fuzzer seed:** `2423783905725303439L` (already in `INTERESTING_SEEDS` from finding 10; it regressed
into this once findings 11–13 changed which pushdown action ran, and now passes again).

## Symptom

The fuzzer reported it as a divergence rather than as a lost message:

```
both threw, but with different types:
  memory=java.lang.UnsupportedOperationException  disk=java.lang.ClassCastException

memory: TableInitializationException: Error while initializing where([fuzzP1 <= Col1 <= fuzzP2])
    caused by: UnsupportedOperationException: WhereFilters do not implement walk
disk:   TableInitializationException: Error while initializing where([fuzzP1 <= Col1 <= fuzzP2])
    caused by: FormulaEvaluationException: ClassCastException encountered in filter=...
    caused by: ClassCastException: class java.time.LocalDate cannot be cast to class java.lang.Boolean
```

The `ClassCastException` is the real problem. The in-memory path never showed it.

## Root cause

`DataIndexPushdownManager.pushdownDataIndex`:

```java
} catch (final Exception e) {
    throw new TableInitializationException(
            "Error applying filter " + Strings.of(copiedFilter) + " to data index table", e);
}
```

`io.deephaven.api.Strings.of` renders the **declarative** filter API through `Filter.Visitor`, and
`WhereFilter`'s default `walk(Filter.Visitor)` throws
`UnsupportedOperationException("WhereFilters do not implement walk")`. That default is deliberate — a
`WhereFilter` is the engine's own representation, not a declarative one — but it makes `Strings.of` a
trap inside a `catch`: the message argument is evaluated *before* the wrapping
`TableInitializationException` is constructed, so the rendering failure propagates and `e` is dropped
on the floor.

So every failure on this path was reported as "WhereFilters do not implement walk", whatever it
actually was.

The divergence the fuzzer saw comes from the two data-index implementations disagreeing about error
policy: `ParquetTableLocation.pushdownDataIndex` **swallows** any exception and declines the index
(with a `TODO DH-19443`), while `DataIndexPushdownManager` **throws**. One path therefore reached the
real `ClassCastException` later, when the filter was applied for real, and the other died in its own
error handling.

## Fix

[`DataIndexPushdownManager.pushdownDataIndex`](../../../../../../../../../../../engine/table/src/main/java/io/deephaven/engine/table/impl/dataindex/DataIndexPushdownManager.java)
uses the filter's own rendering, with a comment recording why not `Strings.of`:

```java
throw new TableInitializationException(
        "Error applying filter " + copiedFilter + " to data index table", e);
```

The real cause now survives as the wrapped exception, and the message still names the filter.

## Not fixed here

Two things this exposed, deliberately left alone:

- **The underlying `ClassCastException`.** The filter was `fuzzP1 <= Col1 <= fuzzP2`, a chained
  comparison whose bounds are query-scope `LocalDate` variables. Java has no chained comparison, so
  when this does not become a `RangeFilter` it compiles to a formula evaluating `(fuzzP1 <= Col1) <= fuzzP2`
  — `Boolean <= LocalDate` — and fails to cast. An inverted range like this one (`2022-01-11 <= Col1 <= 1970-01-01`)
  should simply match nothing. Worth its own investigation now that the error is visible.
- **The two implementations' error policies.** One swallows, one throws. They should agree, and
  `DH-19443` is the ticket for deciding which. Making them agree is what would have prevented the
  divergence report, rather than just unmasking the cause.

## Verification

- `WhereFilterStringsRenderingTest`, 4 tests: `Strings.of` on an initialized `ConditionFilter` throws
  with the "do not implement walk" message; `toString()` on the same filter renders; a `MatchFilter`
  behaves the same way, so the constraint is not specific to formula filters; and `Strings.of` on the
  declarative filter it was built from works, which is what that method is for. This pins the
  constraint so the trap is not walked into again.
- Seed `2423783905725303439L` passes, and all 24 `INTERESTING_SEEDS` pass via `testInterestingSeeds`.
- An in-memory attempt at an end-to-end repro is *not* included: constructing a data index and a filter
  that fails against the index table did not reach this catch block, so the test would have passed
  either way. The seed carries the end-to-end regression.
