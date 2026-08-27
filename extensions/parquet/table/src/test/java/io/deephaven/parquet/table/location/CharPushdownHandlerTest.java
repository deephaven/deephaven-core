//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.impl.select.CharRangeFilter;
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
public class CharPushdownHandlerTest {

    private static Statistics<?> charStats(char minInc, char maxInc) {
        // UINT_16, matching what Deephaven writes for a char column (TypeInfos). A UINT_8 annotation would be
        // rejected by any real writer for values above 255 -- including Character.MAX_VALUE, used below.
        final PrimitiveType col = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ false))
                .named("charCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.intToBytes(minInc))
                .withMax(BytesUtils.intToBytes(maxInc))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void charRangeFilterScenarios() {
        final Statistics<?> statsAZ = charStats('A', 'Z');

        // range wholly inside
        assertTrue(evaluate(
                new CharRangeFilter("c", 'B', 'Y', true, true), statsAZ));

        // filter equal to statistics inclusive
        assertTrue(evaluate(
                new CharRangeFilter("c", 'A', 'Z', true, true), statsAZ));

        // half-open overlaps
        assertTrue(evaluate(
                new CharRangeFilter("c", 'A', 'M', true, false), statsAZ));
        assertTrue(evaluate(
                new CharRangeFilter("c", 'M', 'Z', false, true), statsAZ));

        // edge inclusive vs exclusive
        assertFalse(evaluate(
                new CharRangeFilter("c", 'A', 'A', false, false), statsAZ));
        assertFalse(evaluate(
                new CharRangeFilter("c", 'Z', 'Z', false, false), statsAZ));

        // single-point inside
        assertTrue(evaluate(
                new CharRangeFilter("c", 'M', 'M', true, true), statsAZ));

        // disjoint below and above
        assertFalse(evaluate(
                new CharRangeFilter("c", '0', '9', true, true), statsAZ));
        assertFalse(evaluate(
                new CharRangeFilter("c", 'a', 'f', true, true), statsAZ));

        // constructor value-swap still overlaps
        assertTrue(evaluate(
                new CharRangeFilter("c", 'Y', 'B', true, true), statsAZ));

        // NULL bound disables push-down
        assertTrue(evaluate(
                new CharRangeFilter("c", QueryConstants.NULL_CHAR, 'C', true, true), statsAZ));

        // statistics at char domain extremes
        final Statistics<?> statsFull = charStats(Character.MIN_VALUE, Character.MAX_VALUE);
        assertTrue(evaluate(
                new CharRangeFilter("c", 'A', 'A', true, true), statsFull));

        // Overlapping (a,a] with stats [a, b] should return false
        assertFalse(evaluate(
                new CharRangeFilter("i", 'a', 'a', false, true), charStats('a', 'b')));
    }

    /**
     * {@code X < v} accepts a null row, and Deephaven surfaces a null from two places. A Parquet null is answered by
     * the null gate in {@code StatisticsEvaluator.maybeMakeForFilter}; a stored value equal to {@code NULL_CHAR} reads
     * back as null and has to be found in min/max instead.
     * <p>
     * char is the type where that second case bites. Its sentinel is {@code Character.MAX_VALUE}, the <i>top</i> of the
     * value domain, while Deephaven orders null at the <i>bottom</i> -- so a row group whose values run up to the
     * sentinel has a high {@code min} and would otherwise be excluded by a filter that those very rows satisfy. The
     * integral types are accidentally safe here, their sentinel being {@code MIN_VALUE}.
     */
    @Test
    public void unboundedBelowRangeKeepsRowGroupsThatCanReadBackAsNull() {
        // `X < 'A'`, per RangeFilter.makeRangeFilter: [NULL_CHAR, 'A').
        final CharRangeFilter lessThanA = new CharRangeFilter("c", QueryConstants.NULL_CHAR, 'A', true, false);

        // No Parquet nulls, but the values reach the sentinel, so those rows read back as null and match.
        assertTrue(evaluate(
                lessThanA, charStats('Z', QueryConstants.NULL_CHAR)));

        // No Parquet nulls and no sentinel among the values: nothing here can be below 'A'.
        assertFalse(evaluate(lessThanA, charStats('Z', 'z')));

        // The Parquet-null half of the question is not this handler's: see
        // PushdownHandlerNullStatisticsTest#nullGateKeepsRowGroupsThatMayHoldParquetNulls.
    }

    @Test
    public void charMatchFilterScenarios() {
        final Statistics<?> stats = charStats('G', 'P');

        // unsorted list with duplicates, at least one inside
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "c", 'x', 'C', 'M', 'M', 'a'),
                stats));

        // all values outside after sort
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR,
                        "c", 'q', 'r', 's'),
                stats));

        // large array mostly outside but one inside
        final Object[] many = IntStream.range(0, 100)
                .mapToObj(i -> (char) ('z' - i)) // z..d (outside)
                .toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 'H'; // inside
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "c", withInside), stats));

        // empty list
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "c"), stats));

        // A null among the values no longer declines push-down. To Parquet the sentinel is an ordinary value,
        // so it is tested against min/max like any other; Parquet nulls are ruled out separately, by the
        // null gate in StatisticsEvaluator.maybeMakeForFilter.
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "c", QueryConstants.NULL_CHAR, 'X'), stats));

        // ...but a row group whose values reach the sentinel may hold rows that read back as null.
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.REGULAR, "c", QueryConstants.NULL_CHAR),
                charStats('G', QueryConstants.NULL_CHAR)));
    }

    @Test
    public void charInvertMatchFilterScenarios() {
        // stats B..G; NOT IN {C,D,E} leaves gaps
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "c", 'E', 'C', 'D'),
                charStats('B', 'G')));

        // stats D..D; NOT IN {D} removes the only value, leaving no gap inside stats
        assertFalse(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "c", 'D'),
                charStats('D', 'D')));

        // stats A..Z; NOT IN list of 25 letters leaves single-point gap
        final Object[] exclude = IntStream.rangeClosed('A', 'Z')
                .filter(c -> c != 'M') // exclude all but M
                .mapToObj(c -> (char) c)
                .toArray();
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "c", exclude),
                charStats('A', 'Z')));

        // excluding nothing (empty list) treated as maybe overlap
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "c"), charStats('A', 'B')));

        // NULL value disables push-down in inverted mode
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "c", QueryConstants.NULL_CHAR),
                charStats('A', 'B')));

        // Inverse match of {'A', 'B'} against statistics ['A', 'A'] should return false but currently returns true
        // since the implementation assumes the range ('A', 'B') overlaps with the statistics range ['A', 'B'].
        assertTrue(evaluate(
                new MatchFilter(MatchOptions.INVERTED, "i", 'A', 'B'),
                charStats('A', 'B')));
    }

    /**
     * {@code X < v} arrives as {@code [NULL_CHAR, v)}, and {@code NULL_CHAR} is {@code Character.MAX_VALUE} -- the
     * numerically <i>largest</i> char, though it denotes the bottom of the ordering. Read literally it makes the
     * interval look empty, which would prune every row group. The handler substitutes {@code MIN_CHAR}.
     */
    @Test
    public void sentinelLowerBoundIsReadAsTheBottomOfTheDomain() {
        // Values below 'm' exist here, so this must not be excluded.
        assertTrue(evaluate(CharRangeFilter.lt("c", 'm'), charStats('a', 'f')));
        assertTrue(evaluate(CharRangeFilter.leq("c", 'm'), charStats('a', 'f')));

        // Nothing below 'm' here, so it still prunes.
        assertFalse(evaluate(CharRangeFilter.lt("c", 'm'), charStats('n', 'z')));

        // The greater-than direction is bounded by MAX_CHAR and was already sound.
        assertTrue(evaluate(CharRangeFilter.gt("c", 'm'), charStats('n', 'z')));
        assertFalse(evaluate(CharRangeFilter.gt("c", 'm'), charStats('a', 'f')));
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.maybeMakeForFilter} does per location.
     */
    private static boolean evaluate(final CharRangeFilter filter, final Statistics<?> stats) {
        return CharPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return CharPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
