# Finding 3 — a pre-epoch `LocalDateTime` with a sub-second part cannot be read back from parquet

**Severity:** high — data written successfully becomes permanently unreadable. Not pushdown-specific:
`readTable(...).select()` is enough.
**Status:** **FIXED**, with regression tests.
**Repro tests:**
- `extensions/parquet/base/src/test/java/io/deephaven/parquet/base/materializers/PreEpochLocalDateTimeMaterializerTest.java`
- `extensions/parquet/table/src/test/java/io/deephaven/parquet/table/PreEpochLocalDateTimeRoundTripTest.java`

**Fuzzer seed:** `8750790217018904276L` (`maxTableSize=1000`) — **now passes end to end**, and is the
first entry in `INTERESTING_SEEDS`.
**Share of the run that found it:** 20 of 89 failures.

## Symptom

The write succeeds and the file is correct. The read fails:

```
io.deephaven.UncheckedDeephavenException: Failed to read parquet page data for row: ..., column: Col1
    caused by: java.time.DateTimeException:
        Invalid value for NanoOfSecond (valid values 0 - 999999999): -1
```

```java
ParquetTools.writeTable(TableTools.newTable(
        TableTools.col("Timestamp", LocalDateTime.parse("1969-12-31T23:59:59.999999999"))), dest);
ParquetTools.readTable(dest).select();   // throws
```

## Root cause

`LocalDateTimeFromNanosMaterializer.convertValue`, and its `Micros` and `Millis` siblings:

```java
return LocalDateTime.ofEpochSecond(value / 1_000_000_000L, (int) ((value % 1_000_000_000L) * NANO),
        ZoneOffset.UTC);
```

Java's `/` and `%` truncate toward zero, so for a negative `value` the remainder is negative:
`-1 % 1_000_000_000 == -1`. `LocalDateTime.ofEpochSecond` requires a nano-of-second in
`[0, 999999999]` and rejects anything else outright — hence the exception naming `-1`.

This affects **every** pre-epoch value with a non-zero sub-second part, at all three parquet
precisions, and it is asymmetric with the write path: `TransferUtils.epochNanosUTC` computes
`secondsToNanos(toEpochSecond(UTC)) + getNano()`, which is correct across zero
(`-1_000_000_000 + 999999999 == -1`). So the value on disk was right and only the read was wrong.

Whole-second pre-epoch values were unaffected — the remainder is zero, where truncation and flooring
agree — which is why the defect looked narrower than it was.

### Why it survived

`TestLocalDateTimeMaterializers` covers exactly one value per precision, `123456789123456789L`,
positive in all three. No pre-epoch value appears in it, so the sign asymmetry was never exercised.

## Fix

Source template
[`LocalDateTimeFromMillisMaterializer`](../../../../../../../../../base/src/main/java/io/deephaven/parquet/base/materializers/LocalDateTimeFromMillisMaterializer.java),
with the `Micros` and `Nanos` variants regenerated via `./gradlew replicatePageMaterializers`:

```java
return LocalDateTime.ofEpochSecond(Math.floorDiv(value, 1_000L),
        (int) (Math.floorMod(value, 1_000L) * MILLI), ZoneOffset.UTC);
```

`Math.floorMod` returns a result with the sign of the divisor, so the nano-of-second is always in
`[0, 1_000_000_000)` and `Math.floorDiv` carries the borrow into the seconds. The literal `1_000L`
is kept in both positions because the replicator rewrites it textually to `1_000_000L` and
`1_000_000_000L`.

The `LocalTimeFrom*Materializer` and `InstantNanos*Materializer` siblings were checked and do not
share the defect: `LocalTime.ofNanoOfDay` takes a non-negative day offset, and the `Instant`
materializers return epoch nanos as a `long` without splitting them.

## Adjacent, not fixed here

`TransferUtils.epochNanosUTC` computes `secondsToNanos(seconds) + nano` with no check on the sum, so
a `LocalDateTime` in the top 0.855 s of the representable range wraps silently on **write** — the
same class of defect as finding 2, in a third copy of the arithmetic. The fuzzer does not currently
generate a value there (`FuzzType.LOCAL_DATE_TIME`'s largest is `2261-12-31T23:59:59.999999999`), so
it is recorded here rather than changed; it gets its own finding if a later run reaches it.

## Verification

- `PreEpochLocalDateTimeMaterializerTest`, 6 tests: the original `-1` value; pre-epoch sweeps at all
  three precisions against `Instant.ofEpochSecond` as the oracle; pre-epoch whole seconds; and the
  epoch and post-epoch controls.
- `PreEpochLocalDateTimeRoundTripTest`, 5 tests: `writeTable`/`readTable().select()` round trips for
  pre-epoch sub-second values, values interleaved with nulls, the whole-second and post-epoch
  controls, and a `where` over a read-back pre-epoch column.
- Full `:extensions-parquet-base:test` and `:extensions-parquet-table:test` pass, including the
  pre-existing `TestLocalDateTimeMaterializers`.
- Seed `8750790217018904276L` passes.
