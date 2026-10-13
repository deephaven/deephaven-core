//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class InstantPushdownHandlerTest {

    private static Statistics<?> instantStatsMillis(final Instant minInc, final Instant maxInc) {
        return instantStats(LogicalTypeAnnotation.TimeUnit.MILLIS, minInc.toEpochMilli(), maxInc.toEpochMilli());
    }

    private static Statistics<?> instantStatsMicros(final Instant minInc, final Instant maxInc) {
        return instantStats(LogicalTypeAnnotation.TimeUnit.MICROS,
                DateTimeUtils.epochMicros(minInc), DateTimeUtils.epochMicros(maxInc));
    }

    private static Statistics<?> instantStatsNanos(final long minNanos, final long maxNanos) {
        return instantStats(LogicalTypeAnnotation.TimeUnit.NANOS, minNanos, maxNanos);
    }

    /**
     * A Deephaven null {@link Instant} is the {@code NULL_LONG} sentinel. Deephaven's own writer turns that into a
     * Parquet null, but a writer that is not Deephaven may store the value outright -- Deephaven then reads the row
     * back as null while Parquet counts no nulls at all. So the sentinel has to be looked for in {@code min}/{@code
     * max} and cannot be answered by the null-aware check in {@code StatisticsEvaluator.makeForFilter} alone.
     * <p>
     * {@code ParquetTableFilterTest#testForExtremes} pins the same story for the other primitive types, against a real
     * pyarrow-written file; timestamps are covered here because no such fixture exists for them.
     */
    @Test
    public void storedNullSentinelIsFoundInStatistics() {
        final StatisticsEvaluator evaluator = InstantPushdownHandler.maybeCreateEvaluator(
                new MatchFilter(MatchOptions.REGULAR, "t", new Object[] {null}));

        // Proven free of Parquet nulls and the statistics stop well short of the sentinel: nothing reads back as null.
        assertFalse(evaluator.maybeOverlaps(instantStatsNanos(0L, 50L)));

        // Proven free of Parquet nulls, but the statistics reach the sentinel, so a stored value may read back as
        // null. This is the half the proof cannot answer.
        assertTrue(evaluator.maybeOverlaps(instantStatsNanos(QueryConstants.NULL_LONG, 50L)));
    }

    /**
     * A null lower bound held <i>exclusively</i> -- {@code X > null} -- is the one range shape a null row does not
     * satisfy, so the sentinel must not keep a row group alive on its own. Instant's null is {@code NULL_LONG} in the
     * underlying epoch nanoseconds, the bottom of the value domain, so a row group holding nothing but the sentinel can
     * be excluded outright.
     */
    @Test
    public void exclusiveNullLowerBoundExcludesTheSentinel() {
        // `null < X < 50 ns`: nothing here but the sentinel, which this filter does not match.
        assertFalse(evaluate(
                new InstantRangeFilter("t", QueryConstants.NULL_LONG, 50L, false, false),
                instantStatsNanos(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG)));

        // `X < 50 ns` -- the same range with the bound held inclusively -- matches the null rows, so the same row
        // group has to be kept.
        assertTrue(evaluate(
                new InstantRangeFilter("t", QueryConstants.NULL_LONG, 50L, true, false),
                instantStatsNanos(QueryConstants.NULL_LONG, QueryConstants.NULL_LONG)));

        // A sentinel minimum with ordinary timestamps above it overlaps either way.
        assertTrue(evaluate(
                new InstantRangeFilter("t", QueryConstants.NULL_LONG, 50L, false, false),
                instantStatsNanos(QueryConstants.NULL_LONG, 10L)));

        // ... but not once those timestamps are all above the upper bound.
        assertFalse(evaluate(
                new InstantRangeFilter("t", QueryConstants.NULL_LONG, 50L, false, false),
                instantStatsNanos(60L, 100L)));
    }

    @Test
    public void instantRangeFilterScenarios() {
        final Statistics<?> stats = instantStatsMillis(
                Instant.ofEpochMilli(0L), // 0 ms
                Instant.ofEpochMilli(50L)); // 50 ms

        // wholly inside
        assertTrue(evaluate(
                new InstantRangeFilter("t",
                        1_000_000L, 2_000_000L, true, true), // 1-2 ms
                stats));

        // matches lower edge inclusive vs exclusive
        assertTrue(evaluate(
                new InstantRangeFilter("t",
                        0L, 0L, true, true),
                stats));
        assertFalse(evaluate(
                new InstantRangeFilter("t",
                        0L, 0L, false, false),
                stats));

        // disjoint before / after
        assertFalse(evaluate(
                new InstantRangeFilter("t",
                        -20_000_000L, -10_000_000L, true, true), // -20 to -10 ms
                stats));
        assertFalse(evaluate(
                new InstantRangeFilter("t",
                        60_000_000L, 70_000_000L, true, true),
                stats)); // 60-70 ms

        // constructor reversal still overlaps
        assertTrue(evaluate(
                new InstantRangeFilter("t",
                        40_000_000L, 10_000_000L, true, true), // reversed 40 ms / 10 ms
                stats));

        // NULL bound disables push-down
        assertTrue(evaluate(
                new InstantRangeFilter("t",
                        QueryConstants.NULL_LONG, 2_000_000L, true, true),
                stats));
    }

    @Test
    public void instantMatchFilterScenarios() {
        final Statistics<?> stats = instantStatsMillis(
                Instant.ofEpochMilli(1), // 1 ms
                Instant.ofEpochMilli(10)); // 10 ms

        // at least one in range
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "t",
                        Instant.ofEpochMilli(2), // inside
                        Instant.ofEpochMilli(20)), // outside
                stats));

        // all values outside
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "t",
                        Instant.ofEpochMilli(20), Instant.ofEpochMilli(30)),
                stats));

        // non-Instant value short-circuits to true
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "t",
                        "not-an-instant"),
                stats));

        // empty list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "t"), stats));

        // list containing null
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "t",
                        Instant.ofEpochMilli(2), null),
                stats));
    }

    @Test
    public void instantInvertMatchFilterScenarios() {
        // stats 0..100 ms; NOT IN {50 ms} leaves gaps
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "t",
                        Instant.ofEpochMilli(50)),
                instantStatsMillis(Instant.ofEpochMilli(0L),
                        Instant.ofEpochMilli(100))));

        // single-point stats excluded
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "t",
                        Instant.ofEpochMilli(25)),
                instantStatsMillis(Instant.ofEpochMilli(25),
                        Instant.ofEpochMilli(25))));

        // single-point stats, exclusion miss
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "t",
                        Instant.ofEpochMilli(26L)),
                instantStatsMillis(Instant.ofEpochMilli(25L),
                        Instant.ofEpochMilli(25L))));

        // null in the exclusion list disables push-down
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "t",
                        (Object) null),
                instantStatsMillis(Instant.ofEpochMilli(10L), Instant.ofEpochMilli(20L))));
    }

    /**
     * An {@link InstantRangeFilter} must resolve to this handler and not to {@code LongPushdownHandler}, which claims
     * any {@link LongRangeFilter} and so would claim this subclass too if it were offered the filter first. Only this
     * handler converts the statistics out of the file's timestamp unit; the long handler reads them raw, comparing
     * epoch-nanosecond filter bounds against microseconds or milliseconds and excluding every row group of a file that
     * a writer other than Deephaven stamped. Nanosecond files agree unit-for-unit and hide the difference, as does
     * every other test here: they call the typed overload directly and so never exercise the resolution that picks the
     * handler.
     */
    @Test
    public void instantRangeFilterResolvesToUnitAwareHandler() {
        final Instant rowGroupMin = Instant.parse("2021-01-01T00:00:00Z");
        final Instant rowGroupMax = Instant.parse("2021-12-31T00:00:00Z");

        // Contains the row group outright, so no row group may be excluded.
        final InstantRangeFilter containing = new InstantRangeFilter("t",
                DateTimeUtils.epochNanos(Instant.parse("2020-01-01T00:00:00Z")),
                DateTimeUtils.epochNanos(Instant.parse("2022-01-01T00:00:00Z")),
                true, true);
        // Falls entirely before it, so every row group must still be excluded -- the evaluator has not simply gone
        // blind.
        final InstantRangeFilter disjoint = new InstantRangeFilter("t",
                DateTimeUtils.epochNanos(Instant.parse("2019-01-01T00:00:00Z")),
                DateTimeUtils.epochNanos(Instant.parse("2019-06-01T00:00:00Z")),
                true, true);

        for (final Statistics<?> stats : List.of(
                instantStatsMicros(rowGroupMin, rowGroupMax),
                instantStatsMillis(rowGroupMin, rowGroupMax))) {
            assertTrue(StatisticsEvaluator.resolveHandler(containing).maybeOverlaps(stats));
            assertFalse(StatisticsEvaluator.resolveHandler(disjoint).maybeOverlaps(stats));
        }
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.makeForFilter} does per location.
     */
    private static Statistics<?> instantStats(
            final LogicalTypeAnnotation.TimeUnit unit, final long minInc, final long maxInc) {
        final PrimitiveType col = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(/* adjustedToUTC */ true, unit))
                .named("instCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(minInc))
                .withMax(BytesUtils.longToBytes(maxInc))
                .withNumNulls(0L)
                .build();
    }

    private static long nanos(final String instant) {
        return DateTimeUtils.epochNanos(Instant.parse(instant));
    }

    /**
     * DH-23756, PD-009: a MILLIS or MICROS statistic beyond the epoch-nanosecond range -- the common {@code 9999-12-31}
     * sentinel -- made every pushed-down filter on its column throw. The out-of-range extreme now saturates, so the row
     * group is still bounded by the other one: a filter wholly before it excludes it, and anything overlapping keeps
     * it.
     */
    @Test
    public void statisticsBeyondTheNanosRangeStillPrune() {
        final long sentinelMillis = Instant.parse("9999-12-31T00:00:00Z").toEpochMilli();
        final long year1Millis = Instant.parse("0001-01-01T00:00:00Z").toEpochMilli();
        final long startOf2020Millis = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();

        for (final LogicalTypeAnnotation.TimeUnit unit : new LogicalTypeAnnotation.TimeUnit[] {
                LogicalTypeAnnotation.TimeUnit.MILLIS, LogicalTypeAnnotation.TimeUnit.MICROS}) {
            final long perMilli = unit == LogicalTypeAnnotation.TimeUnit.MILLIS ? 1L : 1_000L;

            // Real dates up to the sentinel: the maximum is out of range, the minimum is not.
            final Statistics<?> upToSentinel = instantStats(unit, startOf2020Millis * perMilli,
                    sentinelMillis * perMilli);
            assertFalse(unit + " before", evaluate(new InstantRangeFilter("t",
                    nanos("2019-01-01T00:00:00Z"), nanos("2019-12-31T00:00:00Z"), true, true), upToSentinel));
            assertTrue(unit + " overlapping", evaluate(new InstantRangeFilter("t",
                    nanos("2021-01-01T00:00:00Z"), nanos("2022-01-01T00:00:00Z"), true, true), upToSentinel));
            assertTrue(unit + " to the top", evaluate(new InstantRangeFilter("t",
                    nanos("2200-01-01T00:00:00Z"), Long.MAX_VALUE, true, true), upToSentinel));
            assertFalse(unit + " match before", evaluate(new MatchFilter(MatchOptions.REGULAR, "t",
                    Instant.parse("2019-06-01T00:00:00Z")), upToSentinel));
            assertTrue(unit + " match inside", evaluate(new MatchFilter(MatchOptions.REGULAR, "t",
                    Instant.parse("2021-06-01T00:00:00Z")), upToSentinel));

            // Nothing but the sentinel, or nothing but values before the range: an ordinary filter excludes it.
            final Statistics<?> onlySentinel = instantStats(unit, sentinelMillis * perMilli,
                    sentinelMillis * perMilli);
            assertFalse(unit + " only sentinel", evaluate(new InstantRangeFilter("t",
                    nanos("2020-01-01T00:00:00Z"), nanos("2021-01-01T00:00:00Z"), true, true), onlySentinel));
            final Statistics<?> onlyYear1 = instantStats(unit, year1Millis * perMilli, year1Millis * perMilli);
            assertFalse(unit + " only year 1", evaluate(new InstantRangeFilter("t",
                    nanos("2020-01-01T00:00:00Z"), nanos("2021-01-01T00:00:00Z"), true, true), onlyYear1));
        }
    }

    /**
     * DH-23756: a MILLIS or MICROS statistic before the epoch-nanosecond range -- the common {@code 0001-01-01}
     * sentinel -- has no epoch nanoseconds of its own. Deephaven would order it above {@code NULL_LONG} and below
     * {@code MIN_LONG}, two adjacent {@code long}s, so a minimum saturates to the one and a maximum to the other. A row
     * group holding such values must then be kept by every filter they satisfy, even one naming null or the earliest
     * instant ({@code MIN_LONG}), and still be excluded by one wholly after them.
     * <p>
     * Saturating both extremes to {@code NULL_LONG} made the row group look like nothing but nulls, so an is-not-null
     * filter silently dropped every row; saturating both to {@code MIN_LONG} would do the same to a filter on the
     * earliest instant.
     */
    @Test
    public void statisticsBeforeTheNanosRangeStraddleNullAndTheEarliestInstant() {
        final long year1Millis = Instant.parse("0001-01-01T00:00:00Z").toEpochMilli();
        final long year1000Millis = Instant.parse("1000-01-01T00:00:00Z").toEpochMilli();
        final long startOf2020Millis = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        final Instant earliest = DateTimeUtils.epochNanosToInstant(QueryConstants.MIN_LONG);

        final MatchFilter isNull = new MatchFilter(MatchOptions.REGULAR, "t", new Object[] {null});
        final MatchFilter isNotNull = new MatchFilter(MatchOptions.INVERTED, "t", new Object[] {null});
        final MatchFilter isNotEarliest = new MatchFilter(MatchOptions.INVERTED, "t", earliest);
        // `t > null`, `t < earliest` and `t > earliest`, built as RangeFilter builds them.
        final InstantRangeFilter aboveNull =
                new InstantRangeFilter("t", QueryConstants.NULL_LONG, Long.MAX_VALUE, false, true);
        final InstantRangeFilter beforeEarliest =
                new InstantRangeFilter("t", QueryConstants.MIN_LONG, Long.MIN_VALUE, true, false);
        final InstantRangeFilter afterEarliest =
                new InstantRangeFilter("t", QueryConstants.MIN_LONG, Long.MAX_VALUE, false, true);

        for (final LogicalTypeAnnotation.TimeUnit unit : new LogicalTypeAnnotation.TimeUnit[] {
                LogicalTypeAnnotation.TimeUnit.MILLIS, LogicalTypeAnnotation.TimeUnit.MICROS}) {
            final long perMilli = unit == LogicalTypeAnnotation.TimeUnit.MILLIS ? 1L : 1_000L;

            // Both extremes before the range, equal or not, and only the minimum before it.
            final Statistics<?> onlyYear1 = instantStats(unit, year1Millis * perMilli, year1Millis * perMilli);
            final Statistics<?> allBefore = instantStats(unit, year1Millis * perMilli, year1000Millis * perMilli);
            final Statistics<?> fromYear1 = instantStats(unit, year1Millis * perMilli, startOf2020Millis * perMilli);

            for (final Statistics<?> stats : List.of(onlyYear1, allBefore, fromYear1)) {
                final String label = unit + " " + stats.genericGetMin() + ".." + stats.genericGetMax() + ": ";
                // Every value before the range satisfies these, so none may exclude the row group.
                assertTrue(label + "is not null", evaluate(isNotNull, stats));
                assertTrue(label + "above null", evaluate(aboveNull, stats));
                assertTrue(label + "is not earliest", evaluate(isNotEarliest, stats));
                assertTrue(label + "before earliest", evaluate(beforeEarliest, stats));
                // No such value is null, but the saturated minimum is NULL_LONG, so an is-null filter cannot tell.
                assertTrue(label + "is null", evaluate(isNull, stats));
            }

            // No value before the range is after the earliest instant, so a row group of nothing else is excluded.
            for (final Statistics<?> stats : List.of(onlyYear1, allBefore)) {
                assertFalse(unit + " after earliest", evaluate(afterEarliest, stats));
            }
        }
    }

    /**
     * DH-23756, PD-008: NANOS statistics at the top of the {@code long} range are legal epoch nanoseconds, but the
     * evaluator used to round-trip them through {@link Instant}, and {@link DateTimeUtils#epochNanos(Instant)} cannot
     * convert the top ~0.855 s back. They are now used as they are.
     */
    @Test
    public void nanosStatisticsAtTheTopOfTheLongRangePrune() {
        final long firstRejected = 9_223_372_036_000_000_000L;
        for (final long max : new long[] {
                firstRejected, firstRejected + 500_000_000L, Long.MAX_VALUE - 1, Long.MAX_VALUE}) {
            final Statistics<?> stats = instantStats(LogicalTypeAnnotation.TimeUnit.NANOS, firstRejected, max);
            assertFalse("before, max " + max,
                    evaluate(new InstantRangeFilter("t", 0L, 1_000_000L, true, true), stats));
            assertTrue("overlapping, max " + max,
                    evaluate(new InstantRangeFilter("t", firstRejected, Long.MAX_VALUE, true, true), stats));
            assertFalse("match just below, max " + max, evaluate(new MatchFilter(MatchOptions.REGULAR, "t",
                    Instant.ofEpochSecond(9_223_372_035L, 999_999_999L)), stats));
        }
    }

    private static boolean evaluate(final InstantRangeFilter filter, final Statistics<?> stats) {
        return InstantPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return InstantPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
