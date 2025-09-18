//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.api.agg.Aggregation.AggDistinct;
import static io.deephaven.engine.testutil.TstUtils.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Test QueryTable select and update operations.
 */
public class QueryTableSelectBarrierTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testBarrier() {
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

        final int size = 1_000_000;
        final Table x = TableTools.emptyTable(size);
        final AtomicInteger a = new AtomicInteger();
        QueryScope.addParam("a", a);

        final boolean old = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        try (final SafeCloseable ignored = () -> QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = old) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

            final SelectColumn sa = SelectColumnFactory.getExpression("A=a.getAndIncrement()");
            final SelectColumn sb = SelectColumnFactory.getExpression("B=a.getAndIncrement()");
            final Table y = x.update(List.of(sa, sb));

            final Table expected = y.updateView("A=i", "B=1_000_000 + i");
            assertTableEquals(expected, y);

            a.set(0);

            final Table z = x.update(List.of(SelectColumn.ofStateless(sa), SelectColumn.ofStateless(sb)));

            // these things should happen at the same time, so we should sum to the expected value;
            // but everything should be mixed up
            final long doubleSize = size * 2;
            final long expectedSum = ((doubleSize - 1) * doubleSize) / 2;
            final Table zs = z.sumBy();
            final long za, zb;
            try (CloseablePrimitiveIteratorOfLong zai = zs.longColumnIterator("A")) {
                za = zai.nextLong();
            }
            try (CloseablePrimitiveIteratorOfLong zbi = zs.longColumnIterator("B")) {
                zb = zbi.nextLong();
            }
            assertEquals(expectedSum, za + zb);

            // now check for swizzling
            assertFalse(z.where(FilterComparison.geq(ColumnName.of("A"), Literal.of(size))).isEmpty());
            assertFalse(z.where(FilterComparison.leq(ColumnName.of("B"), Literal.of(size))).isEmpty());

            // and the column itself should not be ordered either
            assertTrue(isOutOfOrder(z, "A"));
            assertTrue(isOutOfOrder(z, "B"));

            // now let's keep things stateless, but insert a barrier so that A and B do not intermix
            a.set(0);

            final Table u = x.update(List.of(SelectColumn.ofStateless(sa).withBarriers(a),
                    SelectColumn.ofStateless(sb).respectsBarriers(a)));
            TableTools.show(u);

            // check for swizzling
            assertTrue(u.where(FilterComparison.gt(ColumnName.of("A"), Literal.of(size))).isEmpty());
            assertTrue(u.where(FilterComparison.lt(ColumnName.of("B"), Literal.of(size))).isEmpty());

            // we should have parallelized each column
            assertTrue(isOutOfOrder(u, "A"));
            assertTrue(isOutOfOrder(u, "B"));

            // check the expected sums
            final long expectedSumA = (((long) size - 1) * size) / 2;
            final Table us = u.sumBy();
            final long ua, ub;
            try (CloseablePrimitiveIteratorOfLong uai = us.longColumnIterator("A")) {
                ua = uai.nextLong();
            }
            try (CloseablePrimitiveIteratorOfLong ubi = us.longColumnIterator("B")) {
                ub = ubi.nextLong();
            }
            assertEquals(expectedSumA, ua);
            assertEquals(expectedSumA + ((long) size * size), ub);

        }
    }

    private static boolean isOutOfOrder(Table z, final String column) {
        boolean outOfOrder = false;
        int prev = Integer.MIN_VALUE;
        try (CloseablePrimitiveIteratorOfInt zai = z.integerColumnIterator(column)) {
            while (zai.hasNext()) {
                int zav = zai.next();
                if (zav < prev) {
                    outOfOrder = true;
                    break;
                }
                prev = zav;
            }
        }
        return outOfOrder;
    }
}

