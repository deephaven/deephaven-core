//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.ShortRangeFilter;
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
public class ShortPushdownHandlerTest {

    private static Statistics<?> shortStats(final short minInc, final short maxInc) {
        final PrimitiveType col = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ true))
                .named("shortCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.intToBytes(minInc))
                .withMax(BytesUtils.intToBytes(maxInc))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void shortRangeFilterScenarios() {
        final Statistics<?> stats = shortStats((short) -1000, (short) 1000);

        // range wholly inside
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) -200, (short) 200, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) -1000, (short) 1000, true, true), stats));

        // half-open overlaps
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) -1000, (short) 0, true, false), stats));
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) 0, (short) 1000, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) -1000, (short) -1000, false, false), stats));
        assertFalse(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) 1000, (short) 1000, false, false), stats));

        // single-point inside
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) 123, (short) 123, true, true), stats));

        // disjoint below and above
        assertFalse(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) -20000, (short) -15000, true, true), stats));
        assertFalse(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) 15000, (short) 20000, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) 300, (short) -300, true, true), stats));

        // NULL bound disables push-down
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", QueryConstants.NULL_SHORT, (short) 0, true, true), stats));

        // stats at full short domain
        final Statistics<?> statsFull = shortStats(Short.MIN_VALUE, Short.MAX_VALUE);
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("s", (short) 0, (short) 0, true, true), statsFull));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(ShortPushdownHandler.maybeOverlaps(
                new ShortRangeFilter("i", (short) 3, (short) 3, false, true), shortStats((short) 3, (short) 4)));
    }

    @Test
    public void shortMatchFilterScenarios() {
        final Statistics<?> stats = shortStats((short) 100, (short) 200);

        // unsorted list with duplicates, one inside
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "s", (short) 50, (short) 150, (short) 180, (short) 180),
                stats));

        // all values outside
        assertFalse(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "s", (short) 300, (short) 400),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(1000, 1100).mapToObj(i -> (short) i).toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = (short) 150;
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "s", withInside), stats));

        // empty list
        assertFalse(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "s"), stats));

        // list containing NULL
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "s",
                        QueryConstants.NULL_SHORT, (short) 42),
                stats));
    }

    @Test
    public void shortInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "s", (short) -1, (short) 0, (short) 1),
                shortStats((short) -5, (short) 5)));

        // stats fully covered by exclusion list
        assertFalse(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "s", (short) 77),
                shortStats((short) 77, (short) 77)));

        // exclude 10-19 leaves gap 0-9 and 20-29
        final Object[] exclude = IntStream.rangeClosed(10, 19).mapToObj(i -> (short) i).toArray();
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "s", exclude),
                shortStats((short) 0, (short) 29)));

        // empty exclusion list
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "s"),
                shortStats((short) 1, (short) 2)));

        // NULL disables push-down
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "s", QueryConstants.NULL_SHORT),
                shortStats((short) 11, (short) 12)));

        // Inverse match of {5, 6} against statistics [5, 6] should return false but currently returns true since
        // the implementation assumes the range (5, 6) overlaps with the statistics range [5, 6].
        assertTrue(ShortPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", 5, 6),
                shortStats((short) 11, (short) 12)));
    }
}
