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

    private static Statistics<?> instantStatsMillis(Instant minInc, Instant maxInc) {
        final PrimitiveType col = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(/* adjustedToUTC */ true,
                        LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("instCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(minInc.toEpochMilli()))
                .withMax(BytesUtils.longToBytes(maxInc.toEpochMilli()))
                .withNumNulls(0L)
                .build();
    }

    private static Statistics<?> instantStatsMicros(final Instant minInc, final Instant maxInc) {
        final PrimitiveType col = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(/* adjustedToUTC */ true,
                        LogicalTypeAnnotation.TimeUnit.MICROS))
                .named("instCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(DateTimeUtils.epochMicros(minInc)))
                .withMax(BytesUtils.longToBytes(DateTimeUtils.epochMicros(maxInc)))
                .withNumNulls(0L)
                .build();
    }

    private static Statistics<?> instantStatsNanos(final long minNanos, final long maxNanos) {
        final PrimitiveType col = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(/* adjustedToUTC */ true,
                        LogicalTypeAnnotation.TimeUnit.NANOS))
                .named("instCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(minNanos))
                .withMax(BytesUtils.longToBytes(maxNanos))
                .withNumNulls(0L)
                .build();
    }

    /**
     * A Deephaven null {@link Instant} is the {@code NULL_LONG} sentinel. Deephaven's own writer turns that into a
     * Parquet null, but a writer that is not Deephaven may store the value outright -- Deephaven then reads the row
     * back as null while Parquet counts no nulls at all. So the sentinel has to be looked for in {@code min}/{@code
     * max} and cannot be answered by the null gate in {@code StatisticsEvaluator.maybeMakeForFilter} alone.
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

    @Test
    public void instantRangeFilterScenarios() {
        final Statistics<?> stats = instantStatsMillis(
                Instant.ofEpochMilli(0L), // 0 ms
                Instant.ofEpochMilli(50L)); // 50 ms

        // wholly inside
        assertTrue(evaluate(
                new InstantRangeFilter("t",
                        1_000_000L, 2_000_000L, true, true), // 1–2 ms
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
                stats)); // 60–70 ms

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
     * {@code StatisticsEvaluator.maybeMakeForFilter} does per location.
     */
    private static boolean evaluate(final InstantRangeFilter filter, final Statistics<?> stats) {
        return InstantPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return InstantPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
