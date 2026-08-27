//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.stream.LongStream;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class LongPushdownHandlerTest {

    private static Statistics<?> longStats(final long minInc, final long maxInc) {
        final PrimitiveType col = Types.required(INT64)
                .as(LogicalTypeAnnotation.intType(64, /* signed */ true))
                .named("longCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(minInc))
                .withMax(BytesUtils.longToBytes(maxInc))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void longRangeFilterScenarios() {
        final Statistics<?> stats = longStats(-5_000L, 5_000L);

        // range wholly inside
        assertTrue(evaluate(
                new LongRangeFilter("l", -1_000L, 1_000L, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(evaluate(
                new LongRangeFilter("l", -5_000L, 5_000L, true, true), stats));

        // half-open overlaps
        assertTrue(evaluate(
                new LongRangeFilter("l", -5_000L, 0L, true, false), stats));
        assertTrue(evaluate(
                new LongRangeFilter("l", 0L, 5_000L, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(evaluate(
                new LongRangeFilter("l", -5_000L, -5_000L, false, false), stats));
        assertFalse(evaluate(
                new LongRangeFilter("l", 5_000L, 5_000L, false, false), stats));

        // single-point inside
        assertTrue(evaluate(
                new LongRangeFilter("l", 42L, 42L, true, true), stats));

        // disjoint below and above
        assertFalse(evaluate(
                new LongRangeFilter("l", -20_000L, -15_000L, true, true), stats));
        assertFalse(evaluate(
                new LongRangeFilter("l", 15_000L, 20_000L, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(evaluate(
                new LongRangeFilter("l", 2_000L, -2_000L, true, true), stats));

        // NULL bound disables push-down
        assertTrue(evaluate(
                new LongRangeFilter("l", QueryConstants.NULL_LONG, 0L, true, true), stats));

        // stats at full domain
        final Statistics<?> statsFull = longStats(Long.MIN_VALUE, Long.MAX_VALUE);
        assertTrue(evaluate(
                new LongRangeFilter("l", 0L, 0L, true, true), statsFull));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(evaluate(
                new LongRangeFilter("i", 3, 3, false, true), longStats(3, 4)));
    }

    @Test
    public void longMatchFilterScenarios() {
        final Statistics<?> stats = longStats(1_000L, 2_000L);

        // unsorted list with duplicates, one inside
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "l", 5_000L, 1_500L, 1_800L, 1_800L),
                stats));

        // all values outside
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "l", 9_000L, 10_000L),
                stats));

        // large list mostly outside, one inside
        final Object[] many = LongStream.range(50_000L, 50_100L).boxed().toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 1_234L;
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "l", withInside), stats));

        // empty list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "l"), stats));

        // A null among the values no longer declines push-down. To Parquet the sentinel is an ordinary value,
        // so it is tested against min/max like any other; Parquet nulls are ruled out separately, by the
        // null gate in StatisticsEvaluator.maybeMakeForFilter.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "l",
                        QueryConstants.NULL_LONG, 123L),
                stats));

        // ...but a row group whose values reach the sentinel may hold rows that read back as null.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "l", QueryConstants.NULL_LONG),
                longStats(QueryConstants.NULL_LONG, 2_000L)));
    }

    @Test
    public void longInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "l", -1L, 0L, 1L),
                longStats(-5L, 5L)));

        // stats fully covered by exclusion list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "l", 42L),
                longStats(42L, 42L)));

        // exclude 10-19 leaves gaps 0-9 and 20-29
        final Object[] exclude = LongStream.range(10L, 20L).boxed().toArray();
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "l", exclude),
                longStats(0L, 29L)));

        // empty exclusion list
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "l"),
                longStats(7L, 8L)));

        // NULL disables push-down
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "l", QueryConstants.NULL_LONG),
                longStats(100L, 200L)));

        // Inverse match of {5, 6} against statistics [5, 6] should return false but currently returns true since
        // the implementation assumes the range (5, 6) overlaps with the statistics range [5, 6].
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", 5, 6),
                longStats(5, 6)));
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.maybeMakeForFilter} does per location.
     */
    private static boolean evaluate(final LongRangeFilter filter, final Statistics<?> stats) {
        return LongPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return LongPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
