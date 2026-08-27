//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
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
        assertTrue(evaluate(
                new FloatRangeFilter("f", -10f, 10f, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(evaluate(
                new FloatRangeFilter("f", -50.5f, 50.5f, true, true), stats));

        // half-open overlaps
        assertTrue(evaluate(
                new FloatRangeFilter("f", -50.5f, 0f, true, false), stats));
        assertTrue(evaluate(
                new FloatRangeFilter("f", 0f, 50.5f, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(evaluate(
                new FloatRangeFilter("f", -50.5f, -50.5f, false, false), stats));
        assertFalse(evaluate(
                new FloatRangeFilter("f", 50.5f, 50.5f, false, false), stats));

        // single-point inside
        assertTrue(evaluate(
                new FloatRangeFilter("f", 25f, 25f, true, true), stats));

        // disjoint below and above
        assertFalse(evaluate(
                new FloatRangeFilter("f", -128f, -120f, true, true), stats));
        assertFalse(evaluate(
                new FloatRangeFilter("f", 60f, 70f, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(evaluate(
                new FloatRangeFilter("f", 10f, -10f, true, true), stats));

        // ranges that use inf still overlap finite stats
        assertTrue(evaluate(
                new FloatRangeFilter("f", Float.NEGATIVE_INFINITY, -1f, true, true), stats));
        assertTrue(evaluate(
                new FloatRangeFilter("f", 1f, Float.POSITIVE_INFINITY, true, true), stats));

        // NULL or NaN bound disables push-down
        assertTrue(evaluate(
                new FloatRangeFilter("f", QueryConstants.NULL_FLOAT, 0f, true, true), stats));
        assertTrue(evaluate(
                new FloatRangeFilter("f", -1f, Float.NaN, true, true), stats));

        // stats (-Inf .. +Inf), any finite filter overlaps
        assertTrue(evaluate(
                new FloatRangeFilter("d", -10.0f, 10.0f, true, true),
                floatStats(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(evaluate(
                new FloatRangeFilter("i", 3.0f, 3.0f, false, true), floatStats(3, 4)));
    }

    @Test
    public void floatMatchFilterScenarios() {
        final Statistics<?> stats = floatStats(10f, 30f);

        // unsorted list with duplicates, one inside
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "f", 50f, 15f, 22f, 22f),
                stats));

        // all values outside
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "f", 40f, 41f),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(0, 100).mapToObj(i -> 100f - i).toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 25f;
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "f", withInside), stats));

        // list containing inf values
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "f",
                        Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 20f),
                stats));

        // empty list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "f"), stats));

        // A null among the values no longer declines push-down. To Parquet the sentinel is an ordinary value,
        // so it is tested against min/max like any other; Parquet nulls are ruled out separately, by the
        // null gate in StatisticsEvaluator.maybeMakeForFilter.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "f",
                        QueryConstants.NULL_FLOAT, 50f),
                stats));

        // ...but a row group whose values reach the sentinel may hold rows that read back as null.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "f", QueryConstants.NULL_FLOAT),
                floatStats(QueryConstants.NULL_FLOAT, 30f)));

        // NaN is different: conforming writers omit it from min/max, so it can never be ruled out.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "f",
                        Float.NaN, 50f),
                stats));

        // stats (-Inf .. +Inf), inside match should still overlap
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d", 0.0),
                floatStats(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)));
    }

    @Test
    public void floatInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "f", -1f, 0f, 1f),
                floatStats(-5f, 5f)));

        // Fully covered by the exclusion list, but still not excludable: the statistics cannot rule out a NaN.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "f", 42f),
                floatStats(42f, 42f)));

        // exclude 10-19 leaves gaps 0-9 and 20-29
        final Object[] exclude = IntStream.range(10, 20).mapToObj(i -> (float) i).toArray();
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "f", exclude),
                floatStats(0f, 29f)));

        // excluding inf still leaves a finite gap
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "f",
                        Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY),
                floatStats(-10f, 10f)));

        // stats (-Inf .. +Inf) and exclusion misses, still overlap
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d", 0.0),
                floatStats(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)));

        // empty exclusion list
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "f"),
                floatStats(1f, 2f)));

        // NULL or NaN disables push-down
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "f", QueryConstants.NULL_FLOAT),
                floatStats(5f, 6f)));
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "f", Float.NaN),
                floatStats(5f, 6f)));

        final float nextAfterFive = Math.nextAfter(5.0f, Float.POSITIVE_INFINITY);
        // Inverse match of {5, nextAfterFive} against statistics [5, nextAfterFive] should return false but currently
        // returns true since the implementation assumes the range (5, nextAfterFive) overlaps with the statistics range
        // [5,nextAfterFive].
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", 5, nextAfterFive),
                floatStats(5.0f, nextAfterFive)));
    }

    /**
     * {@code {min=1.0, max=1.0}} is what a conforming writer emits for both {@code {1.0}} and {@code {1.0, NaN}}, so it
     * cannot rule out a NaN, and a NaN would satisfy the inverted match.
     */
    @Test
    public void floatInvertedMatchCannotExcludeInvisibleNaN() {
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "f", 1.0f),
                floatStats(1.0f, 1.0f)));

        // A regular match over the same statistics is unaffected: NaN never equals a non-NaN value.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "f", 2.0f),
                floatStats(1.0f, 1.0f)));
    }

    /**
     * Both ends of a parsed float comparison are unusual. {@code X < v} arrives as {@code [NULL_FLOAT, v)}, and
     * {@code NULL_FLOAT} is {@code -Float.MAX_VALUE}, which sits <i>above</i> negative infinity; {@code X > v} arrives
     * as {@code (v, NaN]}, NaN being chosen because Deephaven orders it above every value so the results exclude it.
     * Read literally, every comparison against NaN is false and the row group would always be pruned. The handler
     * substitutes the true infinities.
     */
    @Test
    public void floatSentinelAndNaNBoundsAreReadAsTheDomainExtremes() {
        // NaN upper bound: values above 5 exist here, so this must not be excluded.
        assertTrue(evaluate(
                FloatRangeFilter.gt("x", 5.0f), floatStats(1.0f, 10.0f)));
        assertTrue(evaluate(
                FloatRangeFilter.geq("x", 5.0f), floatStats(1.0f, 10.0f)));

        // Nothing above 5 here, so it still prunes.
        assertFalse(evaluate(
                FloatRangeFilter.gt("x", 5.0f), floatStats(1.0f, 3.0f)));

        // Null sentinel lower bound, the mirror case.
        assertTrue(evaluate(
                FloatRangeFilter.lt("x", 5.0f), floatStats(1.0f, 3.0f)));
        assertFalse(evaluate(
                FloatRangeFilter.lt("x", 5.0f), floatStats(10.0f, 10.0f)));

        // A row group whose maximum is negative infinity still matches "< 5"; NULL_FLOAT sits above it, so
        // reading the sentinel literally as the lower bound would have excluded this.
        assertTrue(evaluate(
                FloatRangeFilter.lt("x", 5.0f),
                floatStats(Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY)));
    }

    /**
     * {@code X > v} is built as {@code (v, NaN)} with the upper bound <i>exclusive</i>. Under Deephaven's ordering NaN
     * is above every value, so "exclusive of NaN" means every value except NaN -- up to and including positive
     * infinity. Substituting the infinity for NaN therefore has to make that bound inclusive; leaving it exclusive
     * would exclude a row group whose values are all positive infinity, which do match.
     */
    @Test
    public void floatPositiveInfinityMatchesGreaterThan() {
        assertTrue(evaluate(
                FloatRangeFilter.gt("x", 5.0f),
                floatStats(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY)));
        assertTrue(evaluate(
                FloatRangeFilter.geq("x", 5.0f),
                floatStats(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY)));
        assertTrue(evaluate(
                FloatRangeFilter.gt("x", 5.0f), floatStats(1f, Float.POSITIVE_INFINITY)));
    }

    /**
     * {@code >} and {@code >=} differ only in {@code isLowerInclusive}, and the only input that distinguishes them is a
     * row group whose maximum is exactly the pivot: nothing there is strictly greater, but the maximum itself satisfies
     * {@code >=}. The same holds mirrored for {@code <} against the minimum.
     */
    @Test
    public void floatStrictAndInclusiveBoundsDifferAtTheExtreme() {
        assertFalse(evaluate(
                FloatRangeFilter.gt("x", 10.0f), floatStats(1.0f, 10.0f)));
        assertTrue(evaluate(
                FloatRangeFilter.geq("x", 10.0f), floatStats(1.0f, 10.0f)));

        assertFalse(evaluate(
                FloatRangeFilter.lt("x", 1.0f), floatStats(1.0f, 10.0f)));
        assertTrue(evaluate(
                FloatRangeFilter.leq("x", 1.0f), floatStats(1.0f, 10.0f)));
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.maybeMakeForFilter} does per location.
     */
    private static boolean evaluate(final FloatRangeFilter filter, final Statistics<?> stats) {
        return FloatPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return FloatPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
