# Finding 2 — `epochNanos` decided representability from the seconds alone

**Severity:** high — one half rejects representable values with an exception; the other half silently
returns an unrelated number for an ordinary historical date.
**Status:** **FIXED**, with regression tests.
**Repro tests:**
- `engine/time/src/test/java/io/deephaven/time/EpochNanosBoundaryTest.java`
- `codec/builtin/src/test/java/io/deephaven/util/codec/ZonedDateTimeCodecTest.java` (`testMax`, `testMin`)

**Fuzzer seed:** `8750790217018904276L` (`maxTableSize=1000`)
**Share of the run that found it:** 25 of 89 failures.

## Symptom

Writing an `Instant` column holding a top-of-range value:

```
io.deephaven.time.DateTimeUtils$DateTimeOverflowException:
    Numeric overflow detected during conversion of 9223372036 to nanoseconds
```

## Root cause

`DateTimeUtils.safeComputeNanos` — the sole implementation behind `epochNanos(Instant)` and
`epochNanos(ZonedDateTime)`:

```java
if (epochSecond >= MAX_CONVERTIBLE_SECONDS) {   // Long.MAX_VALUE / 1_000_000_000L == 9223372036
    throw new DateTimeOverflowException(...);
}
return epochSecond * 1_000_000_000L + nanoOfSecond;
```

Whether `(epochSecond, nanoOfSecond)` fits in a `long` of nanoseconds is not a property of
`epochSecond`. Testing it as one is wrong in both directions:

**Rejects representable values.** `9223372036 * 1_000_000_000 + 854775807` is exactly
`Long.MAX_VALUE`. The bound rejected the entire last second of the range — every instant from
`2262-04-11T23:47:16Z` through `2262-04-11T23:47:16.854775807Z`.

**Accepts unrepresentable ones, silently.** No bound applied to large negative seconds, so they
multiplied, wrapped, and returned a value with no relation to the input. `1000-01-01T00:00:00Z` has
`epochSecond == -30610224000`; the true product is about `-3.06e19`, far below `Long.MIN_VALUE`, and
the wrapped result is *positive* — a date in the future, with no exception raised.

What makes the arithmetic itself sound is that two's-complement is modular: `es * 1e9 + ns` is exact
whenever the mathematical result fits, **even when the multiplication alone overflows**. That is
what happens at the bottom of the range: `-9223372037 * 1e9` overflows, adding `ns` overflows back,
and the two cancel to the correct answer. So the bottom second worked, by accident, and any fix that
guards the multiplication in isolation (`Math.multiplyExact`) would newly break it.

## Fix

[`DateTimeUtils.safeComputeNanos`](../../../../../../../../../../../engine/time/src/main/java/io/deephaven/time/DateTimeUtils.java)

Check the combined result by inverting it, which is exact in both directions:

```java
final long nanos = epochSecond * 1_000_000_000L + nanoOfSecond;
if (Math.floorDiv(nanos, 1_000_000_000L) != epochSecond
        || Math.floorMod(nanos, 1_000_000_000L) != nanoOfSecond) {
    throw new DateTimeOverflowException(...);
}
return nanos;
```

The pair is representable exactly when it can be recovered from the total. This relies on
`nanoOfSecond` being in `[0, 1_000_000_000)`, which `Instant.getNano()` and `ZonedDateTime.getNano()`
both guarantee; the javadoc now states that precondition. `floorDiv`/`floorMod` rather than `/` and
`%` because the value is signed. The message now names both operands, since either can be at fault.

### The same defect, copied

`ZonedDateTimeCodec.safeComputeNanos` carries a copy, marked *"Sadly, this is copied from
DateTimeUtils, since we cannot depend on the engine-time package."* Its bound is
`epochSecond > MAX_CONVERTIBLE_SECONDS - 1`, so it got the top of the range right and the bottom
wrong: the bottom second failed with an undocumented `ArithmeticException` out of `Math.addExact`,
and far-negative seconds wrapped silently as above. It now uses the same round-trip check, keeping
its existing `IllegalArgumentException` type. Its `MAX_CONVERTIBLE_SECONDS` constant is gone, having
had no remaining use.

`DateTimeUtils.secondsToNanos` was checked and is **not** affected: it takes whole seconds, and its
`Math.abs(seconds) > MAX_CONVERTIBLE_SECONDS` bound is exact for that input.

### An existing test asserted the bug

`ZonedDateTimeCodecTest.testMax` asserted that `Instant.ofEpochSecond(9223372036)` **throws** — a
value that is representable. That assertion is why the off-by-one survived. It now asserts the true
boundaries at both ends, and `testMin` covers the end that had no coverage at all.

## Not fixed here

`Instant.ofEpochSecond(-9223372037, 145224192)` is exactly `Long.MIN_VALUE` nanos, which is
`QueryConstants.NULL_LONG` — so a non-null instant at the very bottom of the range converts to the
null sentinel. That is a sentinel collision rather than an arithmetic defect, it behaved this way
before and after, and the fuzzer's `MIN_EPOCH_NANOS` is `Long.MIN_VALUE + 1` so it does not generate
that value. Noted rather than changed.

## Verification

- `EpochNanosBoundaryTest`, 8 tests: both boundaries convert; the whole top second converts; one
  nanosecond past either end throws; far-out-of-range values throw rather than wrap (including the
  year 1000, `Instant.MIN` and `Instant.MAX`); `epochNanos(ZonedDateTime)` matches; ordinary values
  round trip; and a sampled sweep of the representable range.
- `ZonedDateTimeCodecTest`, 4 tests including the two rewritten ones.
- Full `:engine-time:test` and `:codec-builtin:test` suites pass.
- Seed `8750790217018904276L` now writes and reads its `Instant` column successfully. It does not yet
  pass: it advances to `DateTimeException: Invalid value for NanoOfSecond ... -876543211` while
  reading a pre-epoch `LocalDateTime` column back, which is finding 3.
