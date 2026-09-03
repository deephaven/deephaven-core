//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.assertBackedBy;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rangesOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.render;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.renderRanges;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.unionRanges;
import static org.junit.Assert.assertEquals;

/**
 * A rowset added to the random builder goes through its range queue when it has few ranges, however many keys those
 * ranges cover, and is unioned into the accumulator only when it has many. Either way the result must be right, in
 * whatever order the rowsets arrive.
 */
public class MixedBuilderRandomLargeAddTest {

    private static final long BIG = 64 * 1024;

    /** Many keys in two ranges: SortedRanges-backed, so it reaches the builder as a set rather than as a range. */
    private static WritableRowSet wideTwoRanges(final long j) {
        final RowSetBuilderSequential sb = RowSetFactory.builderSequential();
        sb.appendRange(j * 4 * BIG, j * 4 * BIG + BIG - 1);
        sb.appendKey(j * 4 * BIG + 2 * BIG);
        final WritableRowSet rs = sb.build();
        assertBackedBy("two wide ranges", rs, "SortedRanges");
        return rs;
    }

    /** Many keys in many ranges: every other key of a block and a bit more. */
    private static WritableRowSet manySingletons(final long j) {
        final RowSetBuilderSequential sb = RowSetFactory.builderSequential();
        for (long k = 0; k <= BIG; ++k) {
            sb.appendKey(j * 4 * BIG + 2 * k);
        }
        return sb.build();
    }

    private static void checkAllOrders(final java.util.function.LongFunction<WritableRowSet> maker, final int count) {
        final List<WritableRowSet> sets = new ArrayList<>();
        List<long[]> expectedRanges = new ArrayList<>();
        for (long j = 0; j < count; ++j) {
            final WritableRowSet rs = maker.apply(j);
            sets.add(rs);
            expectedRanges = unionRanges(expectedRanges, rangesOf(rs));
        }
        final String expected = render(expectedRanges);
        for (final String order : new String[] {"ascending", "descending", "interleaved"}) {
            final AdaptiveOrderedLongSetBuilderRandom builder = new AdaptiveOrderedLongSetBuilderRandom();
            for (int i = 0; i < count; ++i) {
                final int j;
                if (order.equals("ascending")) {
                    j = i;
                } else if (order.equals("descending")) {
                    j = count - 1 - i;
                } else {
                    j = i % 2 == 0 ? i / 2 : count - 1 - i / 2;
                }
                builder.addRowSet(sets.get(j));
            }
            try (final WritableRowSet result = new WritableRowSetImpl(builder.getOrderedLongSet())) {
                result.validate();
                assertEquals(order, expected, renderRanges(result));
            }
        }
        sets.forEach(WritableRowSet::close);
    }

    @Test
    public void testWideRowSetsWithFewRanges() {
        checkAllOrders(MixedBuilderRandomLargeAddTest::wideTwoRanges, 40);
    }

    @Test
    public void testRowSetsWithManyRanges() {
        checkAllOrders(MixedBuilderRandomLargeAddTest::manySingletons, 6);
    }
}
