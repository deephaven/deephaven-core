//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
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
        assertTrue(evaluate(
                new ShortRangeFilter("s", (short) -200, (short) 200, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(evaluate(
                new ShortRangeFilter("s", (short) -1000, (short) 1000, true, true), stats));

        // half-open overlaps
        assertTrue(evaluate(
                new ShortRangeFilter("s", (short) -1000, (short) 0, true, false), stats));
        assertTrue(evaluate(
                new ShortRangeFilter("s", (short) 0, (short) 1000, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(evaluate(
                new ShortRangeFilter("s", (short) -1000, (short) -1000, false, false), stats));
        assertFalse(evaluate(
                new ShortRangeFilter("s", (short) 1000, (short) 1000, false, false), stats));

        // single-point inside
        assertTrue(evaluate(
                new ShortRangeFilter("s", (short) 123, (short) 123, true, true), stats));

        // disjoint below and above
        assertFalse(evaluate(
                new ShortRangeFilter("s", (short) -20000, (short) -15000, true, true), stats));
        assertFalse(evaluate(
                new ShortRangeFilter("s", (short) 15000, (short) 20000, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(evaluate(
                new ShortRangeFilter("s", (short) 300, (short) -300, true, true), stats));

        // NULL bound disables push-down
        assertTrue(evaluate(
                new ShortRangeFilter("s", QueryConstants.NULL_SHORT, (short) 0, true, true), stats));

        // stats at full short domain
        final Statistics<?> statsFull = shortStats(Short.MIN_VALUE, Short.MAX_VALUE);
        assertTrue(evaluate(
                new ShortRangeFilter("s", (short) 0, (short) 0, true, true), statsFull));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(evaluate(
                new ShortRangeFilter("i", (short) 3, (short) 3, false, true), shortStats((short) 3, (short) 4)));
    }

    /**
     * A null lower bound held <i>exclusively</i> -- {@code X > null} -- is the one range shape a null row does not
     * satisfy, so the sentinel must not keep a row group alive on its own. {@code NULL_SHORT} is
     * {@code Short.MIN_VALUE}, the bottom of the value domain, so a row group whose {@code min} is the sentinel has to
     * be judged on whatever sits above it -- and one holding nothing else can be excluded outright.
     */
    @Test
    public void exclusiveNullLowerBoundExcludesTheSentinel() {
        // `X > null`, per ShortRangeFilter.gt: (NULL_SHORT, MAX_SHORT].
        final ShortRangeFilter notNull = ShortRangeFilter.gt("s", QueryConstants.NULL_SHORT);

        // Nothing here but the sentinel, which this filter does not match.
        assertFalse(evaluate(notNull, shortStats(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT)));

        // Any ordinary value does match, whether or not the row group also reaches down to the sentinel.
        assertTrue(evaluate(notNull, shortStats((short) -5, (short) 5)));
        assertTrue(evaluate(notNull, shortStats(QueryConstants.NULL_SHORT, (short) 5)));

        // `null < X < (short) 5`: the sentinel rows no longer count, so a row group holding nothing else is excluded...
        assertFalse(evaluate(
                new ShortRangeFilter("s", QueryConstants.NULL_SHORT, (short) 5, false, false),
                shortStats(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT)));

        // ... which is exactly the row group that `X < (short) 5`, holding the same bound inclusively, has to keep.
        assertTrue(evaluate(
                new ShortRangeFilter("s", QueryConstants.NULL_SHORT, (short) 5, true, false),
                shortStats(QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT)));

        // A sentinel minimum with ordinary values above it overlaps either way.
        assertTrue(evaluate(
                new ShortRangeFilter("s", QueryConstants.NULL_SHORT, (short) 5, false, false),
                shortStats(QueryConstants.NULL_SHORT, (short) 10)));
    }

    @Test
    public void shortMatchFilterScenarios() {
        final Statistics<?> stats = shortStats((short) 100, (short) 200);

        // unsorted list with duplicates, one inside
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "s", (short) 50, (short) 150, (short) 180, (short) 180),
                stats));

        // all values outside
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "s", (short) 300, (short) 400),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(1000, 1100).mapToObj(i -> (short) i).toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = (short) 150;
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "s", withInside), stats));

        // empty list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "s"), stats));

        // A null among the values no longer declines push-down. To Parquet the sentinel is an ordinary value,
        // so it is tested against min/max like any other; Parquet nulls are ruled out separately, by the
        // null-aware check in StatisticsEvaluator.makeForFilter.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "s",
                        QueryConstants.NULL_SHORT, (short) 42),
                stats));

        // ...but a row group whose values reach the sentinel may hold rows that read back as null.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "s", QueryConstants.NULL_SHORT),
                shortStats(QueryConstants.NULL_SHORT, (short) 200)));
    }

    @Test
    public void shortInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "s", (short) -1, (short) 0, (short) 1),
                shortStats((short) -5, (short) 5)));

        // stats fully covered by exclusion list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "s", (short) 77),
                shortStats((short) 77, (short) 77)));

        // exclude 10-19 leaves gap 0-9 and 20-29
        final Object[] exclude = IntStream.rangeClosed(10, 19).mapToObj(i -> (short) i).toArray();
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "s", exclude),
                shortStats((short) 0, (short) 29)));

        // empty exclusion list
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "s"),
                shortStats((short) 1, (short) 2)));

        // NULL disables push-down
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "s", QueryConstants.NULL_SHORT),
                shortStats((short) 11, (short) 12)));

        // Inverse match of {5, 6} against statistics [5, 6] could return false -- for an integral type those two
        // excluded values cover the whole interval -- but the handler excludes only a single-valued row group, so
        // this stays a "maybe".
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", 5, 6),
                shortStats((short) 5, (short) 6)));
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.makeForFilter} does per location.
     */
    private static boolean evaluate(final ShortRangeFilter filter, final Statistics<?> stats) {
        return ShortPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return ShortPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
