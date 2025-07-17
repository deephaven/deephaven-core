//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.DoubleRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.stream.IntStream;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class DoublePushdownHandlerTest {

    private static Statistics<?> doubleStats(final double minInc, final double maxInc) {
        final PrimitiveType col = Types.required(DOUBLE).named("doubleCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(Double.doubleToLongBits(minInc)))
                .withMax(BytesUtils.longToBytes(Double.doubleToLongBits(maxInc)))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void doubleRangeFilterScenarios() {
        final Statistics<?> stats = doubleStats(-500.5, 500.5);

        // range wholly inside
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", -100.0, 100.0, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", -500.5, 500.5, true, true), stats));

        // half-open overlaps
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", -500.5, 0.0, true, false), stats));
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", 0.0, 500.5, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", -500.5, -500.5, false, false), stats));
        assertFalse(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", 500.5, 500.5, false, false), stats));

        // single-point inside
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", 42.0, 42.0, true, true), stats));

        // disjoint below and above
        assertFalse(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", -2_000.0, -1_500.0, true, true), stats));
        assertFalse(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", 1_500.0, 2_000.0, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", 200.0, -200.0, true, true), stats));

        // ranges using inf still overlap finite stats
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", Double.NEGATIVE_INFINITY, -1.0, true, true), stats));
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", 1.0, Double.POSITIVE_INFINITY, true, true), stats));

        // NULL or NaN bound disables push-down
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", QueryConstants.NULL_DOUBLE, 0.0, true, true), stats));
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", -1.0, Double.NaN, true, true), stats));

        // stats (-Inf .. +Inf), any finite filter overlaps
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("d", -10.0, 10.0, true, true),
                doubleStats(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(DoublePushdownHandler.maybeOverlaps(
                new DoubleRangeFilter("i", 3.0, 3.0, false, true), doubleStats(3, 4)));
    }

    @Test
    public void doubleMatchFilterScenarios() {
        final Statistics<?> stats = doubleStats(100.0, 300.0);

        // unsorted list with duplicates, one inside
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "d", 500.0, 150.0, 220.0, 220.0),
                stats));

        // all values outside
        assertFalse(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "d", 400.0, 401.0),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(0, 100).mapToObj(i -> 1_000.0 - i).toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 250.0;
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "d", withInside), stats));

        // list containing inf values
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "d",
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 200.0),
                stats));

        // stats (-Inf .. +Inf), inside match should still overlap
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "d", 0.0),
                doubleStats(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));

        // empty list
        assertFalse(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "d"), stats));

        // list containing NULL or NaN
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "d",
                        QueryConstants.NULL_DOUBLE, 500.0),
                stats));
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "d",
                        Double.NaN, 500.0),
                stats));
    }

    @Test
    public void doubleInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d", -1.0, 0.0, 1.0),
                doubleStats(-5.0, 5.0)));

        // stats fully covered by exclusion list
        assertFalse(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d", 77.7),
                doubleStats(77.7, 77.7)));

        // exclude 10-19 leaves gaps 0-9 and 20-29
        final Object[] exclude = IntStream.range(10, 20).mapToObj(i -> (double) i).toArray();
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d", exclude),
                doubleStats(0.0, 29.0)));

        // excluding inf still leaves a finite gap
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d",
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY),
                doubleStats(-10.0, 10.0)));

        // stats (-Inf .. +Inf) and exclusion misses, still overlap
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d", 0.0),
                doubleStats(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));

        // empty exclusion list
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d"),
                doubleStats(1.0, 2.0)));

        // NULL or NaN disables push-down
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d", QueryConstants.NULL_DOUBLE),
                doubleStats(5.0, 6.0)));
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d", Double.NaN),
                doubleStats(5.0, 6.0)));

        final double nextAfterFive = Math.nextAfter(5.0, Double.POSITIVE_INFINITY);
        // Inverse match of {5, nextAfterFive} against statistics [5, nextAfterFive] should return false but currently
        // returns true since the implementation assumes the range (5, nextAfterFive) overlaps with the statistics range
        // [5,nextAfterFive].
        assertTrue(DoublePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", 5, nextAfterFive),
                doubleStats(5.0, nextAfterFive)));
    }
}
