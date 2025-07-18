//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.ByteRangeFilter;
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
public class BytePushdownHandlerTest {

    private static Statistics<?> byteStats(final byte minInc, final byte maxInc) {
        final PrimitiveType col = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ true))
                .named("byteCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.intToBytes(minInc))
                .withMax(BytesUtils.intToBytes(maxInc))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void byteRangeFilterScenarios() {
        final Statistics<?> stats = byteStats((byte) -50, (byte) 50);

        // range wholly inside
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) -10, (byte) 10, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) -50, (byte) 50, true, true), stats));

        // half-open overlaps
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) -50, (byte) 0, true, false), stats));
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) 0, (byte) 50, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) -50, (byte) -50, false, false), stats));
        assertFalse(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) 50, (byte) 50, false, false), stats));

        // single-point inside
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) 20, (byte) 20, true, true), stats));

        // disjoint below and above
        assertFalse(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) -127, (byte) -120, true, true), stats));
        assertFalse(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) 60, (byte) 70, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) 10, (byte) -10, true, true), stats));

        // NULL bound disables push-down
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", QueryConstants.NULL_BYTE, (byte) 0, true, true), stats));

        // stats at full byte domain
        final Statistics<?> statsFull = byteStats(Byte.MIN_VALUE, Byte.MAX_VALUE);
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("b", (byte) 0, (byte) 0, true, true), statsFull));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(BytePushdownHandler.maybeOverlaps(
                new ByteRangeFilter("i", (byte) 3, (byte) 3, false, true), byteStats((byte) 3, (byte) 4)));
    }

    @Test
    public void byteMatchFilterScenarios() {
        final Statistics<?> stats = byteStats((byte) 10, (byte) 30);

        // unsorted list with duplicates, one inside
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "b", (byte) 50, (byte) 15, (byte) 22, (byte) 22),
                stats));

        // all values outside
        assertFalse(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "b", (byte) 40, (byte) 41),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(0, 100)
                .mapToObj(i -> (byte) (100 - i))
                .toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = (byte) 25;
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "b", withInside), stats));

        // empty list
        assertFalse(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "b"), stats));

        // list containing NULL
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "b",
                        QueryConstants.NULL_BYTE, (byte) 50),
                stats));
    }

    @Test
    public void byteInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "b", (byte) -1, (byte) 1, (byte) 0),
                byteStats((byte) -5, (byte) 5)));

        // stats fully covered by exclusion list
        assertFalse(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "b", (byte) 20),
                byteStats((byte) 20, (byte) 20)));

        // exclude 0-8 leaves gap at 9
        final Object[] exclude = IntStream.rangeClosed(0, 8)
                .mapToObj(i -> (byte) i)
                .toArray();
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "b", exclude),
                byteStats((byte) 0, (byte) 9)));

        // empty exclusion list
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "b"),
                byteStats((byte) 1, (byte) 2)));

        // NULL disables push-down
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "b", QueryConstants.NULL_BYTE),
                byteStats((byte) 5, (byte) 6)));

        // Inverse match of {5, 6} against statistics [5, 6] should return false but currently returns true since
        // the implementation assumes the range (5, 6) overlaps with the statistics range [5, 6].
        assertTrue(BytePushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", 5, 6),
                byteStats((byte) 5, (byte) 6)));
    }
}
