//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

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
        final PrimitiveType col = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ false))
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
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'B', 'Y', true, true), statsAZ));

        // filter equal to statistics inclusive
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'A', 'Z', true, true), statsAZ));

        // half-open overlaps
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'A', 'M', true, false), statsAZ));
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'M', 'Z', false, true), statsAZ));

        // edge inclusive vs exclusive
        assertFalse(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'A', 'A', false, false), statsAZ));
        assertFalse(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'Z', 'Z', false, false), statsAZ));

        // single-point inside
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'M', 'M', true, true), statsAZ));

        // disjoint below and above
        assertFalse(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", '0', '9', true, true), statsAZ));
        assertFalse(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'a', 'f', true, true), statsAZ));

        // constructor value-swap still overlaps
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'Y', 'B', true, true), statsAZ));

        // NULL bound disables push-down
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", QueryConstants.NULL_CHAR, 'C', true, true), statsAZ));

        // statistics at char domain extremes
        final Statistics<?> statsFull = charStats(Character.MIN_VALUE, Character.MAX_VALUE);
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("c", 'A', 'A', true, true), statsFull));

        // Overlapping (a,a] with stats [a, b] should return false
        assertFalse(CharPushdownHandler.maybeOverlaps(
                new CharRangeFilter("i", 'a', 'a', false, true), charStats('a', 'b')));
    }

    @Test
    public void charMatchFilterScenarios() {
        final Statistics<?> stats = charStats('G', 'P');

        // unsorted list with duplicates, at least one inside
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "c", 'x', 'C', 'M', 'M', 'a'),
                stats));

        // all values outside after sort
        assertFalse(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "c", 'q', 'r', 's'),
                stats));

        // large array mostly outside but one inside
        final Object[] many = IntStream.range(0, 100)
                .mapToObj(i -> (char) ('z' - i)) // z..d (outside)
                .toArray();
        final Object[] withInside = new Object[many.length + 1];
        System.arraycopy(many, 0, withInside, 0, many.length);
        withInside[withInside.length - 1] = 'H'; // inside
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "c", withInside), stats));

        // empty list
        assertFalse(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "c"), stats));

        // list containing NULL disables push-down
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "c", QueryConstants.NULL_CHAR, 'X'), stats));
    }

    @Test
    public void charInvertMatchFilterScenarios() {
        // stats B..G; NOT IN {C,D,E} leaves gaps
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "c", 'E', 'C', 'D'),
                charStats('B', 'G')));

        // stats D..D; NOT IN {D} removes the only value, leaving no gap inside stats
        assertFalse(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "c", 'D'),
                charStats('D', 'D')));

        // stats A..Z; NOT IN list of 25 letters leaves single-point gap
        final Object[] exclude = IntStream.rangeClosed('A', 'Z')
                .filter(c -> c != 'M') // exclude all but M
                .mapToObj(c -> (char) c)
                .toArray();
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "c", exclude),
                charStats('A', 'Z')));

        // excluding nothing (empty list) treated as maybe overlap
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "c"), charStats('A', 'B')));

        // NULL value disables push-down in inverted mode
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "c", QueryConstants.NULL_CHAR),
                charStats('A', 'B')));

        // Inverse match of {'A', 'B'} against statistics ['A', 'A'] should return false but currently returns true
        // since the implementation assumes the range ('A', 'B') overlaps with the statistics range ['A', 'B'].
        assertTrue(CharPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "i", 'A', 'B'),
                charStats('A', 'B')));
    }
}
