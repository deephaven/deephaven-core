# Finding 15 — ordering incomparable types failed with an opaque `ClassCastException`

**Severity:** medium — the error named neither the comparison nor which operand was which, and arrived
mid-scan rather than at compile time. No wrong results: every affected combination throws.
**Status:** **FIXED** (diagnostic), plus a bench correction.
**Repro test:** `engine/table/src/test/java/io/deephaven/engine/table/impl/lang/IncomparableOrderingMessageTest.java`
**Fuzzer seeds (3, all now passing):** `-849809232336840231L`, `-2945246487059582836L`,
`-8652567192287095591L` — the last three failures in the stream, and all one root cause.

## Symptom

```
disk table threw but memory table did not:
  FormulaEvaluationException: ClassCastException encountered in filter={ 0 <= Col0 <= 1 }
    caused by: ClassCastException: class java.lang.Integer cannot be cast to class java.lang.Boolean
```

and, from other seeds, `String cannot be cast to ChronoLocalDate`,
`LocalTime cannot be cast to Boolean`, `Instant cannot be cast to Boolean`. One mechanism, several
type pairs.

## Root cause

`<`, `<=`, `>` and `>=` between two non-numeric types are rewritten by `QueryLanguageParser` into
`QueryLanguageFunctionUtils.less` / `lessEquals` / `greater` / `greaterEquals(Comparable, Comparable)`,
and all four delegate to one helper:

```java
public static int compareTo(Comparable obj1, Comparable obj2) {
    ...
    return obj1.compareTo(obj2);
}
```

`Comparable.compareTo` is specified only for *mutually comparable* arguments. Erasure means an
incomparable one is cast inside the callee, so the failure surfaces as
`ClassCastException: class java.lang.String cannot be cast to class java.time.chrono.ChronoLocalDate` —
which names an internal supertype, does not say which operand came from the column, and does not
mention the comparison at all.

### Why the fuzzer produced it: the chained comparison

The generated filter was `0 <= Col0 <= 1`. **Deephaven has no chained comparison.** Java's grammar
parses that as `(0 <= Col0) <= 1`, so the outer comparison orders a `Boolean` against an `Integer`.
`WhereFilterFactory`'s comparison pattern matches exactly one binary comparison, so the expression
falls through to a `ConditionFilter` over the raw text and is compiled as written:

```
formula   = 0 <= I <= 1
converted = lessEquals(lessEquals(0, I), 1)
claimed type = boolean          <- correct; the outer overload does return boolean
```

`AbstractConditionFilter.checkReturnType` therefore passes legitimately, and the failure waits until
rows are scanned.

### The memory/disk divergence was not itself a defect

Both paths agree the expression is broken. What differed was whether it got *evaluated*: the case's
filter was `and(not(0 <= Col0 <= 1), FuzzConst == null)`, and `FuzzConst == null` matches nothing, so a
conjunction that evaluates it first short-circuits and never touches the bad disjunct. On the disk
table `DeferredViewTable` split the conjunction — `FuzzConst` is view-computed, so it went post-view —
and the bad filter ran alone. Short-circuiting an invalid expression is legitimate on both sides.

## Fix

### Engine: name both types

[`GenerateQueryLanguageFunctions`](../../../../../../../../../../../replication/reflective/src/main/java/io/deephaven/replicators/GenerateQueryLanguageFunctions.java),
regenerating `QueryLanguageFunctionUtils` with `./gradlew generateQueryLanguageFunctions`:

```java
try {
    return obj1.compareTo(obj2);
} catch (final ClassCastException e) {
    throw new IllegalArgumentException("Cannot order a " + obj1.getClass().getCanonicalName()
            + " against a " + obj2.getClass().getCanonicalName()
            + "; the types are not mutually comparable", e);
}
```

One place covers all four operators, and the original `ClassCastException` is kept as the cause.

**Why not reject it at parse time.** That was tried first and reverted: it broke
`TestQueryLanguageParser.testComparisonConversion`, which asserts that `myTestClass > myIntObj`
*converts* — to `greater(myTestClass, myIntObj.intValue())` — rather than being rejected, where
`TestClass implements Comparable<TestClass>`. The parser deliberately accepts ordering between
unrelated `Comparable`s and defers to runtime. Narrowing what compiles is a query-language decision
with a central blast radius and an existing test asserting the current contract, so the diagnostic is
improved instead.

**Equality is untouched**, and was always safe: `eq` compares with `equals`, so `I == Str` and
`Bi == Ld` match nothing rather than failing. Numeric ordering across types is untouched too — those
resolve to dedicated overloads, so `Bi <= 1` and `BoxI <= BoxD` still work.

### Bench: stop generating an unsupported form

`FuzzFilters.twoSidedRange` emitted the chained form, commented *"The single-expression form, which
parses to one RangeFilter"* — which is simply false; it parses to a `ConditionFilter`. It now emits
`col >= lo && col <= hi`, which is what it meant: one expression carrying both bounds, cost-sorted as
a unit, and genuinely a pair of range filters.

To be clear about the campaign's ground rule: this is **not** suppressing a defect the bench found. The
chained form is a user error that Deephaven does not support, the bench was generating it on a wrong
belief, and the engine's handling of it is fixed above. What was removed is a bench bug.

## Verification

- `IncomparableOrderingMessageTest`, 8 tests: a `String` column ordered against a number and a
  `LocalDate` against a `String`, both asserting the message names both concrete types; the chained
  comparison, asserting it names `Boolean`; all four ordering operators; numeric cross-type ordering
  still working, including `BigInteger` against a boxed `int`; same-type ordering; equality across
  incomparable types still matching nothing; and null operands, whose handling sits above `compareTo`
  and is unchanged. Five of the eight fail without the fix.
- Full `:engine-table:test`, `:extensions-parquet-table:test` and `:extensions-parquet-base:test` pass,
  including `TestQueryLanguageParser`.
- All 3 fuzzer seeds pass, and a fresh 60-second sweep of 491 cases reports **no failures at all**.
