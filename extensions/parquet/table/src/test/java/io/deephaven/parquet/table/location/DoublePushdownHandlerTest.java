//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
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
        assertTrue(evaluate(
                new DoubleRangeFilter("d", -100.0, 100.0, true, true), stats));

        // filter equal to statistics inclusive
        assertTrue(evaluate(
                new DoubleRangeFilter("d", -500.5, 500.5, true, true), stats));

        // half-open overlaps
        assertTrue(evaluate(
                new DoubleRangeFilter("d", -500.5, 0.0, true, false), stats));
        assertTrue(evaluate(
                new DoubleRangeFilter("d", 0.0, 500.5, false, true), stats));

        // edge inclusive vs exclusive
        assertFalse(evaluate(
                new DoubleRangeFilter("d", -500.5, -500.5, false, false), stats));
        assertFalse(evaluate(
                new DoubleRangeFilter("d", 500.5, 500.5, false, false), stats));

        // single-point inside
        assertTrue(evaluate(
                new DoubleRangeFilter("d", 42.0, 42.0, true, true), stats));

        // disjoint below and above
        assertFalse(evaluate(
                new DoubleRangeFilter("d", -2_000.0, -1_500.0, true, true), stats));
        assertFalse(evaluate(
                new DoubleRangeFilter("d", 1_500.0, 2_000.0, true, true), stats));

        // constructor value-swap still overlaps
        assertTrue(evaluate(
                new DoubleRangeFilter("d", 200.0, -200.0, true, true), stats));

        // ranges using inf still overlap finite stats
        assertTrue(evaluate(
                new DoubleRangeFilter("d", Double.NEGATIVE_INFINITY, -1.0, true, true), stats));
        assertTrue(evaluate(
                new DoubleRangeFilter("d", 1.0, Double.POSITIVE_INFINITY, true, true), stats));

        // NULL or NaN bound disables push-down
        assertTrue(evaluate(
                new DoubleRangeFilter("d", QueryConstants.NULL_DOUBLE, 0.0, true, true), stats));
        assertTrue(evaluate(
                new DoubleRangeFilter("d", -1.0, Double.NaN, true, true), stats));

        // stats (-Inf .. +Inf), any finite filter overlaps
        assertTrue(evaluate(
                new DoubleRangeFilter("d", -10.0, 10.0, true, true),
                doubleStats(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));

        // Overlapping (3,3] with stats [3, 4] should return false
        assertFalse(evaluate(
                new DoubleRangeFilter("i", 3.0, 3.0, false, true), doubleStats(3, 4)));
    }

    @Test
    public void doubleMatchFilterScenarios() {
        final Statistics<?> stats = doubleStats(100.0, 300.0);

        // unsorted list with duplicates, one inside
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "d", 500.0, 150.0, 220.0, 220.0),
                stats));

        // all values outside
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "d", 400.0, 401.0),
                stats));

        // large list mostly outside, one inside
        final Object[] many = IntStream.range(0, 100).mapToObj(i -> 1_000.0 - i).toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 250.0;
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d", withInside), stats));

        // list containing inf values
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d",
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 200.0),
                stats));

        // stats (-Inf .. +Inf), inside match should still overlap
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d", 0.0),
                doubleStats(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));

        // empty list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d"), stats));

        // A null among the values no longer declines push-down. To Parquet the sentinel is an ordinary value,
        // so it is tested against min/max like any other; Parquet nulls are ruled out separately, by the
        // null-aware check in StatisticsEvaluator.makeForFilter.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d",
                        QueryConstants.NULL_DOUBLE, 500.0),
                stats));

        // ...but a row group whose values reach the sentinel may hold rows that read back as null.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d", QueryConstants.NULL_DOUBLE),
                doubleStats(QueryConstants.NULL_DOUBLE, 300.0)));

        // What NaN does here depends on nanMatch, not on NaN being invisible to min/max. MatchOptions.REGULAR means
        // IEEE equality, under which `x == NaN` is false for every x -- so the NaN is inert and the other value
        // decides: nothing in the statistics is 500.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d",
                        Double.NaN, 500.0),
                stats));

        // With nanMatch set -- which is how an explicit value list is actually parsed -- a NaN row does match, and no
        // statistics can prove the row group holds none, so it must be kept.
        assertTrue(evaluate(
                new MatchFilter(nanMatchOptions(false), "d",
                        Double.NaN, 500.0),
                stats));
    }

    @Test
    public void doubleInvertMatchFilterScenarios() {
        // gaps remain inside stats
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d", -1.0, 0.0, 1.0),
                doubleStats(-5.0, 5.0)));

        // Fully covered by the exclusion list, but still not excludable: the statistics cannot rule out a NaN.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d", 77.7),
                doubleStats(77.7, 77.7)));

        // exclude 10-19 leaves gaps 0-9 and 20-29
        final Object[] exclude = IntStream.range(10, 20).mapToObj(i -> (double) i).toArray();
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d", exclude),
                doubleStats(0.0, 29.0)));

        // excluding inf still leaves a finite gap
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d",
                        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY),
                doubleStats(-10.0, 10.0)));

        // stats (-Inf .. +Inf) and exclusion misses, still overlap
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d", 0.0),
                doubleStats(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));

        // empty exclusion list
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d"),
                doubleStats(1.0, 2.0)));

        // NULL or NaN disables push-down
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d", QueryConstants.NULL_DOUBLE),
                doubleStats(5.0, 6.0)));
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d", Double.NaN),
                doubleStats(5.0, 6.0)));

        final double nextAfterFive = Math.nextAfter(5.0, Double.POSITIVE_INFINITY);
        // Inverse match of {5, nextAfterFive} against statistics [5, nextAfterFive] should return false but currently
        // returns true since the implementation assumes the range (5, nextAfterFive) overlaps with the statistics range
        // [5,nextAfterFive].
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", 5, nextAfterFive),
                doubleStats(5.0, nextAfterFive)));
    }

    /**
     * {@code {min=1.0, max=1.0}} is what a conforming writer emits for both {@code {1.0}} and {@code {1.0, NaN}}, so it
     * cannot rule out a NaN, and a NaN would satisfy the inverted match.
     */
    @Test
    public void doubleInvertedMatchCannotExcludeInvisibleNaN() {
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "d", 1.0),
                doubleStats(1.0, 1.0)));

        // A regular match over the same statistics is unaffected: NaN never equals a non-NaN value.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "d", 2.0),
                doubleStats(1.0, 1.0)));
    }

    /**
     * Both ends of a parsed double comparison are unusual. {@code X < v} arrives as {@code [NULL_DOUBLE, v)}, and
     * {@code NULL_DOUBLE} is {@code -Double.MAX_VALUE}, which sits <i>above</i> negative infinity; {@code X > v}
     * arrives as {@code (v, NaN]}, NaN being chosen because Deephaven orders it above every value so the results
     * exclude it. Read literally, every comparison against NaN is false and the row group would always be pruned. The
     * handler substitutes the true infinities.
     */
    @Test
    public void doubleSentinelAndNaNBoundsAreReadAsTheDomainExtremes() {
        // NaN upper bound: values above 5 exist here, so this must not be excluded.
        assertTrue(evaluate(
                DoubleRangeFilter.gt("x", 5.0), doubleStats(1.0, 10.0)));
        assertTrue(evaluate(
                DoubleRangeFilter.geq("x", 5.0), doubleStats(1.0, 10.0)));

        // Nothing above 5 here, so it still prunes.
        assertFalse(evaluate(
                DoubleRangeFilter.gt("x", 5.0), doubleStats(1.0, 3.0)));

        // Null sentinel lower bound, the mirror case.
        assertTrue(evaluate(
                DoubleRangeFilter.lt("x", 5.0), doubleStats(1.0, 3.0)));
        assertFalse(evaluate(
                DoubleRangeFilter.lt("x", 5.0), doubleStats(10.0, 10.0)));

        // A row group whose maximum is negative infinity still matches "< 5"; NULL_DOUBLE sits above it, so
        // reading the sentinel literally as the lower bound would have excluded this.
        assertTrue(evaluate(
                DoubleRangeFilter.lt("x", 5.0),
                doubleStats(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY)));
    }

    /**
     * A null lower bound held <i>exclusively</i> -- {@code X > null} -- is the one range shape a null row does not
     * satisfy. {@code NULL_DOUBLE} is {@code -Double.MAX_VALUE} rather than the bottom of the domain -- the infinities
     * lie outside it -- so the sentinel sits inside the interval the filter spans numerically, and the handler keeps
     * even a row group of nothing but sentinels: a conservative answer, not a wrong one. What the exclusive bound does
     * settle is a range with nothing inside it at all.
     */
    @Test
    public void exclusiveNullLowerBoundKeepsSentinelRowGroupsConservatively() {
        // `null < X < -Infinity` matches nothing at all: only a null sorts below negative infinity, and holding the
        // bound exclusively rules the null out.
        assertFalse(evaluate(
                new DoubleRangeFilter("x", QueryConstants.NULL_DOUBLE, Double.NEGATIVE_INFINITY, false, false),
                doubleStats(Double.NEGATIVE_INFINITY, 10.0)));

        // `X < -Infinity` -- the same range with the bound held inclusively -- matches exactly the null rows, and
        // this row group's statistics span the sentinel.
        assertTrue(evaluate(
                new DoubleRangeFilter("x", QueryConstants.NULL_DOUBLE, Double.NEGATIVE_INFINITY, true, false),
                doubleStats(Double.NEGATIVE_INFINITY, 10.0)));

        // A row group of nothing but the sentinel matches no filter that holds the null bound exclusively, but the
        // handler does not test for that shape and keeps the row group conservatively.
        assertTrue(evaluate(
                new DoubleRangeFilter("x", QueryConstants.NULL_DOUBLE, 5.0, false, true),
                doubleStats(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE)));
        assertTrue(evaluate(
                DoubleRangeFilter.gt("x", QueryConstants.NULL_DOUBLE),
                doubleStats(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE)));

        // Held inclusively those same rows read back as null and match.
        assertTrue(evaluate(
                new DoubleRangeFilter("x", QueryConstants.NULL_DOUBLE, 5.0, true, true),
                doubleStats(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE)));
        assertTrue(evaluate(
                DoubleRangeFilter.geq("x", QueryConstants.NULL_DOUBLE),
                doubleStats(QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE)));

        // `null < X <= 5`: ordinary values below the upper bound overlap, negative infinity among them.
        assertTrue(evaluate(
                new DoubleRangeFilter("x", QueryConstants.NULL_DOUBLE, 5.0, false, true),
                doubleStats(Double.NEGATIVE_INFINITY, 10.0)));
        assertFalse(evaluate(
                new DoubleRangeFilter("x", QueryConstants.NULL_DOUBLE, 5.0, false, true),
                doubleStats(6.0, 10.0)));
    }

    /**
     * {@code X > v} is built as {@code (v, NaN)} with the upper bound <i>exclusive</i>. Under Deephaven's ordering NaN
     * is above every value, so "exclusive of NaN" means every value except NaN -- up to and including positive
     * infinity. Substituting the infinity for NaN therefore has to make that bound inclusive; leaving it exclusive
     * would exclude a row group whose values are all positive infinity, which do match.
     */
    @Test
    public void doublePositiveInfinityMatchesGreaterThan() {
        assertTrue(evaluate(
                DoubleRangeFilter.gt("x", 5.0),
                doubleStats(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertTrue(evaluate(
                DoubleRangeFilter.geq("x", 5.0),
                doubleStats(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertTrue(evaluate(
                DoubleRangeFilter.gt("x", 5.0), doubleStats(1.0, Double.POSITIVE_INFINITY)));
    }

    /**
     * {@code >} and {@code >=} differ only in {@code isLowerInclusive}, and the only input that distinguishes them is a
     * row group whose maximum is exactly the pivot: nothing there is strictly greater, but the maximum itself satisfies
     * {@code >=}. The same holds mirrored for {@code <} against the minimum.
     */
    @Test
    public void doubleStrictAndInclusiveBoundsDifferAtTheExtreme() {
        assertFalse(evaluate(
                DoubleRangeFilter.gt("x", 10.0), doubleStats(1.0, 10.0)));
        assertTrue(evaluate(
                DoubleRangeFilter.geq("x", 10.0), doubleStats(1.0, 10.0)));

        assertFalse(evaluate(
                DoubleRangeFilter.lt("x", 1.0), doubleStats(1.0, 10.0)));
        assertTrue(evaluate(
                DoubleRangeFilter.leq("x", 1.0), doubleStats(1.0, 10.0)));
    }

    /**
     * A NaN upper bound stands for "unbounded above" only when it is <i>exclusive</i>. Held <i>inclusively</i> it
     * matches the NaN rows themselves: {@code DoubleComparisons} places NaN equal to itself, so the chunk filter the
     * engine installs admits them, while a conforming writer keeps NaN out of {@code min}/{@code max} and no statistics
     * can prove a row group holds none. Reading an inclusive bound as unbounded above would exclude a row group whose
     * NaN rows match.
     * <p>
     * Of the factories, {@code gt} and {@code geq} are the ones that put NaN in the upper slot for an ordinary pivot,
     * and they always make it exclusive -- so they keep pruning, as the last two assertions re-check. {@code lt} and
     * {@code leq} put their own pivot there instead, so {@code leq("d", NaN)} does produce an inclusive NaN upper; it
     * matches every row, so giving up its pruning costs nothing.
     */
    @Test
    public void doubleInclusiveNaNUpperBoundCannotExcludeNaNRows() {
        // A row group of {1.0, 2.0, NaN}: a conforming writer emits [1.0, 2.0], leaving the NaN invisible.
        final Statistics<?> stats = doubleStats(1.0, 2.0);

        // `[5.0, NaN]` matches every value >= 5.0 *and* every NaN, so the invisible NaN row forbids excluding this
        // row group -- even though nothing in [1.0, 2.0] reaches 5.0.
        assertTrue(evaluate(new DoubleRangeFilter("d", 5.0, Double.NaN), stats));
        assertTrue(evaluate(new DoubleRangeFilter("d", 5.0, Double.NaN, true, true), stats));

        // The degenerate `[NaN, NaN]`, which matches exactly the NaN rows.
        assertTrue(evaluate(new DoubleRangeFilter("d", Double.NaN, Double.NaN, true, true), stats));

        // `leq` with a NaN pivot is the one factory call that presents an inclusive NaN upper bound -- the
        // constructor orders the pair with DoubleComparisons, which puts NaN above the NULL sentinel. Asserted
        // directly, because the evaluator would answer "maybe" either way and so cannot tell the two apart.
        final DoubleRangeFilter leqNaN = DoubleRangeFilter.leq("d", Double.NaN);
        assertTrue("leq(NaN) lands NaN in the upper slot", Double.isNaN(leqNaN.getUpper()));
        assertTrue("leq(NaN) holds that bound inclusively", leqNaN.isUpperInclusive());
        // It therefore matches every row, so keeping the row group is both required and free.
        assertTrue(evaluate(leqNaN, stats));

        // Exclusive NaN uppers cannot match a NaN row, so they still prune: declining the inclusive case above costs
        // no pruning that was previously available.
        assertFalse(evaluate(DoubleRangeFilter.gt("d", 5.0), stats));
        assertFalse(evaluate(DoubleRangeFilter.geq("d", 5.0), stats));
    }

    /**
     * Whether a NaN row satisfies a match filter comes from {@code nanMatch} and {@code inverted}, not from the shape
     * of the filter. The old rule -- "every inverted match admits a NaN row" -- is false: {@code !isNaN(X)} reaches
     * this handler as an inverted match whose only value is NaN, with {@code nanMatch} set, and no NaN row satisfies
     * it.
     */
    @Test
    public void nanAdmittanceFollowsNanMatchNotFilterShape() {
        // A row group of {1.0, 2.0, NaN}: a conforming writer emits [1.0, 2.0], leaving the NaN invisible.
        final Statistics<?> stats = doubleStats(1.0, 2.0);

        // `X != 5.0` with IEEE equality: !(NaN == 5.0) holds, so a NaN row matches and the group must be kept.
        assertTrue(evaluate(new MatchFilter(MatchOptions.INVERTED, "d", 5.0), stats));

        // `isNaN(X)`: inverted=false, nanMatch=true, values={NaN}. A NaN row matches, so the group must be kept.
        assertTrue(evaluate(new MatchFilter(nanMatchOptions(false), "d", Double.NaN), stats));

        // `!isNaN(X)`: inverted=true, nanMatch=true, values={NaN}. No NaN row matches -- but every non-NaN row does,
        // and usable statistics guarantee one exists, so "maybe" is still the only correct answer.
        assertTrue(evaluate(new MatchFilter(nanMatchOptions(true), "d", Double.NaN), stats));
    }

    /**
     * The two cases the old blanket "keep everything" answer gave up. Neither is reachable from a parsed comparison,
     * but both are expressible, and in both the statistics settle the question outright.
     */
    @Test
    public void nanValuesThatCannotMatchNoLongerCostPruning() {
        final Statistics<?> stats = doubleStats(1.0, 2.0);

        // `X == NaN` with IEEE equality matches no row whatsoever, NaN rows included.
        assertFalse(evaluate(new MatchFilter(MatchOptions.REGULAR, "d", Double.NaN), stats));

        // `X not in (5.0, NaN)` with nanMatch: a NaN row is excluded by the NaN in the values, so only the ordinary
        // gap walk remains -- and [1.0, 2.0] lies wholly inside a gap, so the group survives.
        assertTrue(evaluate(new MatchFilter(nanMatchOptions(true), "d", 5.0, Double.NaN), stats));

        // Same filter against a row group holding nothing but the excluded value: now it can be pruned.
        assertFalse(evaluate(new MatchFilter(nanMatchOptions(true), "d", 5.0, Double.NaN), doubleStats(5.0, 5.0)));

        // A row group above every non-NaN value still holds matching rows -- 10.0 is not in (5.0, NaN). This is
        // what makes dropping the NaN load-bearing: left among the values it sorts last, and the walk's closing
        // `max > values[n-1]` becomes `max > NaN`, which is false -- silently pruning a row group that matches.
        assertTrue(evaluate(new MatchFilter(nanMatchOptions(true), "d", 5.0, Double.NaN), doubleStats(10.0, 10.0)));
    }

    /** {@code MatchOptions} as {@code isNaN}/{@code !isNaN} build them: {@code nanMatch} set. */
    private static MatchOptions nanMatchOptions(final boolean inverted) {
        return MatchOptions.builder().nanMatch(true).inverted(inverted).build();
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.makeForFilter} does per location.
     */
    private static boolean evaluate(final DoubleRangeFilter filter, final Statistics<?> stats) {
        return DoublePushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return DoublePushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
