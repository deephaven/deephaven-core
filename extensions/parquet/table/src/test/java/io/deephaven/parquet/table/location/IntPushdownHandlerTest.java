//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
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
        assertTrue(evaluate(
                new IntRangeFilter("i", -200, 200, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(evaluate(
                new IntRangeFilter("i", -500, 500, true, true), stats));

        // half-open overlaps
        assertTrue(evaluate(
                new IntRangeFilter("i", -500, 0, true, false), stats));
        assertTrue(evaluate(
                new IntRangeFilter("i", 0, 500, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(evaluate(
                new IntRangeFilter("i", -500, -500, false, false), stats));
        assertFalse(evaluate(
                new IntRangeFilter("i", 500, 500, false, false), stats));

        // single-point inside
        assertTrue(evaluate(
                new IntRangeFilter("i", 123, 123, true, true), stats));

        // disjoint below and above
        assertFalse(evaluate(
                new IntRangeFilter("i", -2000, -1500, true, true), stats));
        assertFalse(evaluate(
                new IntRangeFilter("i", 1500, 2000, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(evaluate(
                new IntRangeFilter("i", 100, -100, true, true), stats));

        // NULL bound disables push-down
        assertTrue(evaluate(
                new IntRangeFilter("i", QueryConstants.NULL_INT, 0, true, true), stats));

        // stats at full int domain
        final Statistics<?> statsFull = intStats(Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertTrue(evaluate(
                new IntRangeFilter("i", 0, 0, true, true), statsFull));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(evaluate(
                new IntRangeFilter("i", 3, 3, false, true), intStats(3, 4)));
    }

    /**
     * A null lower bound held <i>exclusively</i> -- {@code X > null} -- is the one range shape a null row does not
     * satisfy, so the sentinel must not keep a row group alive on its own. {@code NULL_INT} is
     * {@code Integer.MIN_VALUE}, the bottom of the value domain, so a row group whose {@code min} is the sentinel has
     * to be judged on whatever sits above it -- and one holding nothing else can be excluded outright.
     */
    @Test
    public void exclusiveNullLowerBoundExcludesTheSentinel() {
        // `X > null`, per IntRangeFilter.gt: (NULL_INT, MAX_INT].
        final IntRangeFilter notNull = IntRangeFilter.gt("i", QueryConstants.NULL_INT);

        // Nothing here but the sentinel, which this filter does not match.
        assertFalse(evaluate(notNull, intStats(QueryConstants.NULL_INT, QueryConstants.NULL_INT)));

        // Any ordinary value does match, whether or not the row group also reaches down to the sentinel.
        assertTrue(evaluate(notNull, intStats(-5, 5)));
        assertTrue(evaluate(notNull, intStats(QueryConstants.NULL_INT, 5)));

        // `null < X < 5`: the sentinel rows no longer count, so a row group holding nothing else is excluded...
        assertFalse(evaluate(
                new IntRangeFilter("i", QueryConstants.NULL_INT, 5, false, false),
                intStats(QueryConstants.NULL_INT, QueryConstants.NULL_INT)));

        // ... which is exactly the row group that `X < 5`, holding the same bound inclusively, has to keep.
        assertTrue(evaluate(
                new IntRangeFilter("i", QueryConstants.NULL_INT, 5, true, false),
                intStats(QueryConstants.NULL_INT, QueryConstants.NULL_INT)));

        // A sentinel minimum with ordinary values above it overlaps either way.
        assertTrue(evaluate(
                new IntRangeFilter("i", QueryConstants.NULL_INT, 5, false, false),
                intStats(QueryConstants.NULL_INT, 10)));
    }

    @Test
    public void intMatchFilterScenarios() {
        final Statistics<?> stats = intStats(10, 30);

        // unsorted list with duplicates, one inside
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "i", 50, 15, 22, 22),
                stats));

        // all values outside
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "i", 100, 200),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(1000, 1100).boxed().toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 25;
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "i", withInside), stats));

        // empty list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "i"), stats));

        // A null among the values no longer declines push-down. To Parquet the sentinel is an ordinary value,
        // so it is tested against min/max like any other; Parquet nulls are ruled out separately, by the
        // null-aware check in StatisticsEvaluator.makeForFilter.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "i",
                        QueryConstants.NULL_INT, 50),
                stats));

        // ...but a row group whose values reach the sentinel may hold rows that read back as null.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "i", QueryConstants.NULL_INT),
                intStats(QueryConstants.NULL_INT, 30)));
    }

    @Test
    public void intInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", -1, 0, 1),
                intStats(-5, 5)));

        // stats fully covered by exclusion list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", 42),
                intStats(42, 42)));

        // exclude 10-19 leaves a gap 0-9 and 20-29
        final Object[] exclude = IntStream.range(10, 20).boxed().toArray();
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", exclude),
                intStats(0, 29)));

        // empty exclusion list
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i"),
                intStats(1, 2)));

        // NULL disables push-down
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", QueryConstants.NULL_INT),
                intStats(5, 6)));

        // Inverse match of {5, 6} against statistics [5, 6] could return false -- for an integral type those two
        // excluded values cover the whole interval -- but the handler excludes only a single-valued row group, so
        // this stays a "maybe".
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", 5, 6),
                intStats(5, 6)));
    }

    /**
     * The evaluator is built once per filter and applied to each row group in turn, so it must not depend on any
     * particular statistics object and must not be consumed by use. This matters most for the inverted match, whose
     * values are sorted at creation -- {@code maybeMatchesInverse} walks the gaps between adjacent values and now
     * relies on that having already happened.
     */
    @Test
    public void evaluatorIsReusableAcrossRowGroups() {
        final StatisticsEvaluator regular =
                IntPushdownHandler.maybeCreateEvaluator(new MatchFilter(MatchOptions.REGULAR, "i", 30, 10, 20));
        assertTrue(apply(regular, intStats(5, 15)));
        assertFalse(apply(regular, intStats(100, 200)));
        assertTrue(apply(regular, intStats(25, 35)));
        // Repeating a row group gives the same answer.
        assertFalse(apply(regular, intStats(100, 200)));

        // Values deliberately supplied out of order: the inverse walk needs them sorted, once, at creation.
        final StatisticsEvaluator inverted =
                IntPushdownHandler.maybeCreateEvaluator(new MatchFilter(MatchOptions.INVERTED, "i", 30, 10, 20));
        assertTrue(apply(inverted, intStats(0, 100)));
        assertFalse(apply(inverted, intStats(10, 10)));
        assertTrue(apply(inverted, intStats(0, 100)));
    }

    /**
     * Applies {@code evaluator} to one row group, deriving whether the group is free of nulls from the statistics as
     * {@link ParquetTableLocation} does for a flat column.
     */
    private static boolean apply(final StatisticsEvaluator evaluator, final Statistics<?> stats) {
        return evaluator.maybeOverlaps(stats);
    }


    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.makeForFilter} does per location.
     */
    private static boolean evaluate(final IntRangeFilter filter, final Statistics<?> stats) {
        return IntPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return IntPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
