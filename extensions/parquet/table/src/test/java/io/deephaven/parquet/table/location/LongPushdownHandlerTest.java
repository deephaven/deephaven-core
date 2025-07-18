//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

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
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", -1_000L, 1_000L, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", -5_000L, 5_000L, true, true), stats));

        // half-open overlaps
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", -5_000L, 0L, true, false), stats));
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", 0L, 5_000L, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", -5_000L, -5_000L, false, false), stats));
        assertFalse(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", 5_000L, 5_000L, false, false), stats));

        // single-point inside
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", 42L, 42L, true, true), stats));

        // disjoint below and above
        assertFalse(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", -20_000L, -15_000L, true, true), stats));
        assertFalse(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", 15_000L, 20_000L, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", 2_000L, -2_000L, true, true), stats));

        // NULL bound disables push-down
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", QueryConstants.NULL_LONG, 0L, true, true), stats));

        // stats at full domain
        final Statistics<?> statsFull = longStats(Long.MIN_VALUE, Long.MAX_VALUE);
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("l", 0L, 0L, true, true), statsFull));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(LongPushdownHandler.maybeOverlaps(
                new LongRangeFilter("i", 3, 3, false, true), longStats(3, 4)));
    }

    @Test
    public void longMatchFilterScenarios() {
        final Statistics<?> stats = longStats(1_000L, 2_000L);

        // unsorted list with duplicates, one inside
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "l", 5_000L, 1_500L, 1_800L, 1_800L),
                stats));

        // all values outside
        assertFalse(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "l", 9_000L, 10_000L),
                stats));

        // large list mostly outside, one inside
        final Object[] many = LongStream.range(50_000L, 50_100L).boxed().toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 1_234L;
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "l", withInside), stats));

        // empty list
        assertFalse(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "l"), stats));

        // list containing NULL
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "l",
                        QueryConstants.NULL_LONG, 123L),
                stats));
    }

    @Test
    public void longInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "l", -1L, 0L, 1L),
                longStats(-5L, 5L)));

        // stats fully covered by exclusion list
        assertFalse(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "l", 42L),
                longStats(42L, 42L)));

        // exclude 10-19 leaves gaps 0-9 and 20-29
        final Object[] exclude = LongStream.range(10L, 20L).boxed().toArray();
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "l", exclude),
                longStats(0L, 29L)));

        // empty exclusion list
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "l"),
                longStats(7L, 8L)));

        // NULL disables push-down
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "l", QueryConstants.NULL_LONG),
                longStats(100L, 200L)));

        // Inverse match of {5, 6} against statistics [5, 6] should return false but currently returns true since
        // the implementation assumes the range (5, 6) overlaps with the statistics range [5, 6].
        assertTrue(LongPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", 5, 6),
                longStats(5, 6)));
    }
}
