# Finding 21 — **bench defect**: an unguarded String method call made the oracle comparison meaningless

**This is a defect in the fuzzer, not in Deephaven.** The bench generated a filter that throws on null
data, then required the disk and memory paths to throw identically — which they cannot, for a reason that
is not a bug.
**Severity:** medium, for the bench — false positives, and of a kind that looks like a serious engine
divergence (`memory table threw but disk table did not`) and so costs real triage time.
**Status:** **FIXED.**
**Fuzzer seeds (2, both now passing):** `-9218664977068266450L`, `5844000365408086213L`

## Symptom

```
filter=and(Col0.matches(`z.*`), Col0 != null)
memory table threw but disk table did not:
  io.deephaven.engine.exceptions.TableInitializationException: Error while initializing
    where([Col0.matches(`z.*`), Col0 not in [null]])
  caused by: FormulaEvaluationException: java.lang.NullPointerException
    encountered in filter={ Col0.matches(`z.*`) }
  caused by: java.lang.NullPointerException:
    Cannot invoke "String.matches(String)" because "<local7>" is null
```

The second seed is the same shape through `contains`:
`not(or(Col0.contains(`Aa`), Col0 == null, ...))`.

## Root cause

`FuzzFilters.pattern` emitted the method call on a bare column reference:

```java
final String text = column.resultName() + "." + method + "(" + arg + ")";
```

A null `String` cannot receive a method call, so on a column with nulls this throws. **That is intended
engine behaviour**, not a defect — the query language compiles the call and Java does the rest. The
bench's own `singleInputFormula` already knew this and guards the analogous case:

```java
text = name + " != null && " + name + ".length() > " + random.nextInt(3);
```

`pattern` simply did not, and the two generators disagreed.

The reason this produces a *false positive* rather than a real finding is worth being precise about.
Whether the throwing formula is ever evaluated depends on how many rows the *other* filters in the
conjunction left behind. In the first seed the column is all-null and the companion filter is
`Col0 != null`:

- **On disk**, `Col0 != null` is a match filter with pushdown support. It pruned every row, so the
  formula was never evaluated on a non-empty chunk, and the query returned an empty result.
- **In memory** there is no pushdown, the formula ran first, and it threw.

So the two paths disagreed on whether the query throws *at all* — which is exactly what pushdown is
supposed to be able to change. Comparing an exception-throwing query across two paths with different
pruning is not a valid oracle comparison, whatever the engine does.

## Fix

`FuzzFilters.pattern` now guards the receiver, adopting the convention `singleInputFormula` already used:

```java
final String text = column.resultName() + " != null && "
        + column.resultName() + "." + method + "(" + arg + ")";
```

This costs no coverage. The text parses to a `ConditionFilter` either way — it is not recognized as a
specialized pattern filter, as the `filter={ Col0.matches(...) }` in the stack trace shows — and the
generator already tags it `Tier.CHUNK_ONLY`, so no pushdown action was being probed that the guard now
hides.

## Why this is a bench correction and not a suppression

The campaign runs with no failure-mode suppressions, so the distinction matters. This does not stop the
bench from generating a shape, or hide a class of engine answer. It fixes a generated *query* that did
not mean what the generator intended: the comment above the code says it is there to "probe the
pattern-filter pushdown path", and a query whose result is an exception probes nothing. Every filter the
bench emitted before the fix, it still emits — now well-defined on the data it is applied to. This is
the same category as finding 15's chained-comparison correction, where the bench emitted
`lo <= col <= hi` believing it produced a range filter.

## The engine-side observation, recorded rather than fixed

There is a real, if minor, user-visible consequence underneath this, and it should be written down
rather than quietly dropped along with the false positive:

> Whether `where("S.matches(`x.*`)")` throws on a table containing nulls can depend on whether an
> accompanying filter's pushdown pruned the rows first — so the same query over the same data can throw
> in memory and succeed on parquet, or change behaviour when a pushdown toggle changes.

This is not being fixed, for reasons that are about blast radius rather than convenience:

- Making the residual always evaluate over every row would defeat the purpose of pushdown.
- Making String methods null-safe is a query-language semantics change affecting every formula, not a
  filter fix. Finding 15 already established that this campaign should not make central query-language
  changes on a fuzzer's evidence: a parse-time rejection of incomparable ordering was tried there and
  reverted because an existing test asserted the opposite contract.
- The user-facing remedy already exists and is what the bench now generates: guard the receiver.

If it is ever worth addressing, the shape would be a documentation note that formula filters may not be
evaluated for rows excluded by other filters, and that a formula must therefore be total over the
column's domain — including null.
