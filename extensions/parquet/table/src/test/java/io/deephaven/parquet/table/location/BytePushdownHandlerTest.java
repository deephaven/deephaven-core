//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
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
        assertTrue(evaluate(
                new ByteRangeFilter("b", (byte) -10, (byte) 10, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(evaluate(
                new ByteRangeFilter("b", (byte) -50, (byte) 50, true, true), stats));

        // half-open overlaps
        assertTrue(evaluate(
                new ByteRangeFilter("b", (byte) -50, (byte) 0, true, false), stats));
        assertTrue(evaluate(
                new ByteRangeFilter("b", (byte) 0, (byte) 50, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(evaluate(
                new ByteRangeFilter("b", (byte) -50, (byte) -50, false, false), stats));
        assertFalse(evaluate(
                new ByteRangeFilter("b", (byte) 50, (byte) 50, false, false), stats));

        // single-point inside
        assertTrue(evaluate(
                new ByteRangeFilter("b", (byte) 20, (byte) 20, true, true), stats));

        // disjoint below and above
        assertFalse(evaluate(
                new ByteRangeFilter("b", (byte) -127, (byte) -120, true, true), stats));
        assertFalse(evaluate(
                new ByteRangeFilter("b", (byte) 60, (byte) 70, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(evaluate(
                new ByteRangeFilter("b", (byte) 10, (byte) -10, true, true), stats));

        // NULL bound disables push-down
        assertTrue(evaluate(
                new ByteRangeFilter("b", QueryConstants.NULL_BYTE, (byte) 0, true, true), stats));

        // stats at full byte domain
        final Statistics<?> statsFull = byteStats(Byte.MIN_VALUE, Byte.MAX_VALUE);
        assertTrue(evaluate(
                new ByteRangeFilter("b", (byte) 0, (byte) 0, true, true), statsFull));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(evaluate(
                new ByteRangeFilter("i", (byte) 3, (byte) 3, false, true), byteStats((byte) 3, (byte) 4)));
    }

    /**
     * A null lower bound held <i>exclusively</i> -- {@code X > null} -- is the one range shape a null row does not
     * satisfy, so the sentinel must not keep a row group alive on its own. {@code NULL_BYTE} is {@code Byte.MIN_VALUE},
     * the bottom of the value domain, so a row group whose {@code min} is the sentinel has to be judged on whatever
     * sits above it -- and one holding nothing else can be excluded outright.
     */
    @Test
    public void exclusiveNullLowerBoundExcludesTheSentinel() {
        // `X > null`, per ByteRangeFilter.gt: (NULL_BYTE, MAX_BYTE].
        final ByteRangeFilter notNull = ByteRangeFilter.gt("b", QueryConstants.NULL_BYTE);

        // Nothing here but the sentinel, which this filter does not match.
        assertFalse(evaluate(notNull, byteStats(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE)));

        // Any ordinary value does match, whether or not the row group also reaches down to the sentinel.
        assertTrue(evaluate(notNull, byteStats((byte) -5, (byte) 5)));
        assertTrue(evaluate(notNull, byteStats(QueryConstants.NULL_BYTE, (byte) 5)));

        // `null < X < (byte) 5`: the sentinel rows no longer count, so a row group holding nothing else is excluded...
        assertFalse(evaluate(
                new ByteRangeFilter("b", QueryConstants.NULL_BYTE, (byte) 5, false, false),
                byteStats(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE)));

        // ... which is exactly the row group that `X < (byte) 5`, holding the same bound inclusively, has to keep.
        assertTrue(evaluate(
                new ByteRangeFilter("b", QueryConstants.NULL_BYTE, (byte) 5, true, false),
                byteStats(QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE)));

        // A sentinel minimum with ordinary values above it overlaps either way.
        assertTrue(evaluate(
                new ByteRangeFilter("b", QueryConstants.NULL_BYTE, (byte) 5, false, false),
                byteStats(QueryConstants.NULL_BYTE, (byte) 10)));
    }

    @Test
    public void byteMatchFilterScenarios() {
        final Statistics<?> stats = byteStats((byte) 10, (byte) 30);

        // unsorted list with duplicates, one inside
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "b", (byte) 50, (byte) 15, (byte) 22, (byte) 22),
                stats));

        // all values outside
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "b", (byte) 40, (byte) 41),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(0, 100)
                .mapToObj(i -> (byte) (100 - i))
                .toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = (byte) 25;
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "b", withInside), stats));

        // empty list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "b"), stats));

        // A null among the values no longer declines push-down. To Parquet the sentinel is an ordinary value,
        // so it is tested against min/max like any other; Parquet nulls are ruled out separately, by the
        // null-aware check in StatisticsEvaluator.makeForFilter.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "b",
                        QueryConstants.NULL_BYTE, (byte) 50),
                stats));

        // ...but a row group whose values reach the sentinel may hold rows that read back as null.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "b", QueryConstants.NULL_BYTE),
                byteStats(QueryConstants.NULL_BYTE, (byte) 30)));
    }

    @Test
    public void byteInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "b", (byte) -1, (byte) 1, (byte) 0),
                byteStats((byte) -5, (byte) 5)));

        // stats fully covered by exclusion list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "b", (byte) 20),
                byteStats((byte) 20, (byte) 20)));

        // exclude 0-8 leaves gap at 9
        final Object[] exclude = IntStream.rangeClosed(0, 8)
                .mapToObj(i -> (byte) i)
                .toArray();
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "b", exclude),
                byteStats((byte) 0, (byte) 9)));

        // empty exclusion list
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "b"),
                byteStats((byte) 1, (byte) 2)));

        // NULL disables push-down
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "b", QueryConstants.NULL_BYTE),
                byteStats((byte) 5, (byte) 6)));

        // Inverse match of {5, 6} against statistics [5, 6] should return false but currently returns true since
        // the implementation assumes the range (5, 6) overlaps with the statistics range [5, 6].
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", 5, 6),
                byteStats((byte) 5, (byte) 6)));
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.makeForFilter} does per location.
     */
    private static boolean evaluate(final ByteRangeFilter filter, final Statistics<?> stats) {
        return BytePushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return BytePushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
