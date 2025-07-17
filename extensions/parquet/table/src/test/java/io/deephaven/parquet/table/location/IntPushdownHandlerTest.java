//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.IntRangeFilter;
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

import java.util.stream.IntStream;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class IntPushdownHandlerTest {

    private static Statistics<?> intStats(final int minInc, final int maxInc) {
        final PrimitiveType col = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ true))
                .named("intCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.intToBytes(minInc))
                .withMax(BytesUtils.intToBytes(maxInc))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void intRangeFilterScenarios() {
        final Statistics<?> stats = intStats(-500, 500);

        // range wholly inside
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", -200, 200, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", -500, 500, true, true), stats));

        // half-open overlaps
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", -500, 0, true, false), stats));
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", 0, 500, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", -500, -500, false, false), stats));
        assertFalse(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", 500, 500, false, false), stats));

        // single-point inside
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", 123, 123, true, true), stats));

        // disjoint below and above
        assertFalse(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", -2000, -1500, true, true), stats));
        assertFalse(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", 1500, 2000, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", 100, -100, true, true), stats));

        // NULL bound disables push-down
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", QueryConstants.NULL_INT, 0, true, true), stats));

        // stats at full int domain
        final Statistics<?> statsFull = intStats(Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", 0, 0, true, true), statsFull));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(IntPushdownHandler.maybeOverlaps(
                new IntRangeFilter("i", 3, 3, false, true), intStats(3, 4)));
    }

    @Test
    public void intMatchFilterScenarios() {
        final Statistics<?> stats = intStats(10, 30);

        // unsorted list with duplicates, one inside
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "i", 50, 15, 22, 22),
                stats));

        // all values outside
        assertFalse(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "i", 100, 200),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(1000, 1100).boxed().toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 25;
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "i", withInside), stats));

        // empty list
        assertFalse(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "i"), stats));

        // list containing NULL
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "i",
                        QueryConstants.NULL_INT, 50),
                stats));
    }

    @Test
    public void intInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", -1, 0, 1),
                intStats(-5, 5)));

        // stats fully covered by exclusion list
        assertFalse(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", 42),
                intStats(42, 42)));

        // exclude 10-19 leaves a gap 0-9 and 20-29
        final Object[] exclude = IntStream.range(10, 20).boxed().toArray();
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", exclude),
                intStats(0, 29)));

        // empty exclusion list
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i"),
                intStats(1, 2)));

        // NULL disables push-down
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", QueryConstants.NULL_INT),
                intStats(5, 6)));

        // Inverse match of {5, 6} against statistics [5, 6] should return false but currently returns true since
        // the implementation assumes the range (5, 6) overlaps with the statistics range [5, 6].
        assertTrue(IntPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", 5, 6),
                intStats(5, 6)));
    }
}
