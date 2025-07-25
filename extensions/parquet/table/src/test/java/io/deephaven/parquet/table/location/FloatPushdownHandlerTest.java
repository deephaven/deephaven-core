//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.FloatRangeFilter;
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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class FloatPushdownHandlerTest {

    private static Statistics<?> floatStats(final float minInc, final float maxInc) {
        final PrimitiveType col = Types.required(FLOAT).named("floatCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.intToBytes(Float.floatToIntBits(minInc)))
                .withMax(BytesUtils.intToBytes(Float.floatToIntBits(maxInc)))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void floatRangeFilterScenarios() {
        final Statistics<?> stats = floatStats(-50.5f, 50.5f);

        // range wholly inside
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", -10f, 10f, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", -50.5f, 50.5f, true, true), stats));

        // half-open overlaps
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", -50.5f, 0f, true, false), stats));
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", 0f, 50.5f, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", -50.5f, -50.5f, false, false), stats));
        assertFalse(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", 50.5f, 50.5f, false, false), stats));

        // single-point inside
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", 25f, 25f, true, true), stats));

        // disjoint below and above
        assertFalse(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", -128f, -120f, true, true), stats));
        assertFalse(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", 60f, 70f, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", 10f, -10f, true, true), stats));

        // ranges that use inf still overlap finite stats
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", Float.NEGATIVE_INFINITY, -1f, true, true), stats));
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", 1f, Float.POSITIVE_INFINITY, true, true), stats));

        // NULL or NaN bound disables push-down
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", QueryConstants.NULL_FLOAT, 0f, true, true), stats));
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("f", -1f, Float.NaN, true, true), stats));

        // stats (-Inf .. +Inf), any finite filter overlaps
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("d", -10.0f, 10.0f, true, true),
                floatStats(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(FloatPushdownHandler.maybeOverlaps(
                new FloatRangeFilter("i", 3.0f, 3.0f, false, true), floatStats(3, 4)));
    }

    @Test
    public void floatMatchFilterScenarios() {
        final Statistics<?> stats = floatStats(10f, 30f);

        // unsorted list with duplicates, one inside
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "f", 50f, 15f, 22f, 22f),
                stats));

        // all values outside
        assertFalse(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "f", 40f, 41f),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(0, 100).mapToObj(i -> 100f - i).toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 25f;
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "f", withInside), stats));

        // list containing inf values
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "f",
                        Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 20f),
                stats));

        // empty list
        assertFalse(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "f"), stats));

        // list containing NULL or NaN
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "f",
                        QueryConstants.NULL_FLOAT, 50f),
                stats));
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "f",
                        Float.NaN, 50f),
                stats));

        // stats (-Inf .. +Inf), inside match should still overlap
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "d", 0.0),
                floatStats(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)));
    }

    @Test
    public void floatInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "f", -1f, 0f, 1f),
                floatStats(-5f, 5f)));

        // stats fully covered by exclusion list
        assertFalse(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "f", 42f),
                floatStats(42f, 42f)));

        // exclude 10-19 leaves gaps 0-9 and 20-29
        final Object[] exclude = IntStream.range(10, 20).mapToObj(i -> (float) i).toArray();
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "f", exclude),
                floatStats(0f, 29f)));

        // excluding inf still leaves a finite gap
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "f",
                        Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY),
                floatStats(-10f, 10f)));

        // stats (-Inf .. +Inf) and exclusion misses, still overlap
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "d", 0.0),
                floatStats(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)));

        // empty exclusion list
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "f"),
                floatStats(1f, 2f)));

        // NULL or NaN disables push-down
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "f", QueryConstants.NULL_FLOAT),
                floatStats(5f, 6f)));
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "f", Float.NaN),
                floatStats(5f, 6f)));

        final float nextAfterFive = Math.nextAfter(5.0f, Float.POSITIVE_INFINITY);
        // Inverse match of {5, nextAfterFive} against statistics [5, nextAfterFive] should return false but currently
        // returns true since the implementation assumes the range (5, nextAfterFive) overlaps with the statistics range
        // [5,nextAfterFive].
        assertTrue(FloatPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", 5, nextAfterFive),
                floatStats(5.0f, nextAfterFive)));
    }
}
