//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.api.expression.Method;
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
import static io.deephaven.api.agg.Aggregation.AggSum;
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
    public void testBarrierSelectColumn() {
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

        final int size = 1_000_000;
        final Table x = TableTools.emptyTable(size);
        final AtomicInteger a = new AtomicInteger();
        QueryScope.addParam("a", a);

        try (final SafeCloseable ignored = new SaveQueryTableOptions()) {
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
            checkTotalSum(size, z);

            // now check for swizzling
            checkMixedColumns(z, size, true);

            // and the column itself should not be ordered either
            assertTrue(isOutOfOrder(z, "A"));
            assertTrue(isOutOfOrder(z, "B"));

            // now let's keep things stateless, but insert a barrier so that A and B do not intermix
            a.set(0);

            final Table u = x.update(List.of(SelectColumn.ofStateless(sa).withBarriers(a),
                    SelectColumn.ofStateless(sb).respectsBarriers(a)));

            TableTools.showWithRowSet(u);

            // check for swizzling
            checkMixedColumns(u, size, false);

            // we should have parallelized each column
            assertTrue(isOutOfOrder(u, "A"));
            assertTrue(isOutOfOrder(u, "B"));

            // check the expected sums
            checkIndividualSums(size, u);
        }
    }

    @Test
    public void testBarrierSelectable() {
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

        final int size = 1_000_000;
        final AtomicInteger a = new AtomicInteger();
        QueryScope.addParam("a", a);
        final Table x = TableTools.emptyTable(size).updateView("AtomicInt=a");

        try (final SafeCloseable ignored = new SaveQueryTableOptions()) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
            QueryTable.STATELESS_SELECT_BY_DEFAULT = true;

            final Selectable sa =
                    Selectable.of(ColumnName.of("A"), Method.of(ColumnName.of("AtomicInt"), "getAndIncrement"));
            final Selectable sb =
                    Selectable.of(ColumnName.of("B"), Method.of(ColumnName.of("AtomicInt"), "getAndIncrement"));

            final Table y = x.update(List.of(sa.withSerial(), sb.withSerial()));

            final Table expected = x.updateView("A=i", "B=1_000_000 + i");
            assertTableEquals(expected, y);

            a.set(0);

            final Table z = x.update(List.of(sa, sb));

            // these things should happen at the same time, so we should sum to the expected value;
            // but everything should be mixed up
            checkTotalSum(size, z);

            // now check for swizzling
            checkMixedColumns(z, size, true);

            // and the column itself should not be ordered either
            assertTrue(isOutOfOrder(z, "A"));
            assertTrue(isOutOfOrder(z, "B"));

            // now let's keep things stateless, but insert a barrier so that A and B do not intermix
            a.set(0);

            final Table u = x.update(List.of(sa.withBarriers(a), sb.respectsBarriers(a)));

            // check for swizzling
            checkMixedColumns(u, size, false);

            // we should have parallelized each column
            assertTrue(isOutOfOrder(u, "A"));
            assertTrue(isOutOfOrder(u, "B"));

            // check the expected sums
            checkIndividualSums(size, u);

            // now make B serial; but let A do what it wants
            a.set(0);
            final Table v = x.update(List.of(sa, sb.withSerial()));
            checkTotalSum(size, v);
            // let a float, even though b is serial
            assertTrue(isOutOfOrder(v, "A"));
            assertFalse(isOutOfOrder(v, "B"));

            // now make A serial; but let B do what it wants
            a.set(0);
            final Table w = x.update(List.of(sa.withSerial(), sb));
            checkTotalSum(size, w);

            assertFalse(isOutOfOrder(w, "A"));
            assertTrue(isOutOfOrder(w, "B"));
        }
    }

    private static void checkIndividualSums(int size, Table u) {
        final long expectedSumA = (((long) size - 1) * size) / 2;
        final Table us = u.aggBy(AggSum("A", "B"));
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

    private static void checkMixedColumns(final Table result, final int size, final boolean expectMixed) {
        final Table aBig = result.where(FilterComparison.geq(ColumnName.of("A"), Literal.of(size)));
        final Table bSmall = result.where(FilterComparison.lt(ColumnName.of("B"), Literal.of(size)));

        final boolean aHasLargeValues = !aBig.isEmpty();
        final boolean bHasSmallValues = !bSmall.isEmpty();
        if (expectMixed) {
            assertTrue(aHasLargeValues);
            assertTrue(bHasSmallValues);
        } else {
            assertFalse(aHasLargeValues);
            assertFalse(bHasSmallValues);
        }
    }

    private static void checkTotalSum(int size, Table z) {
        final long doubleSize = size * 2;
        final long expectedSum = ((doubleSize - 1) * doubleSize) / 2;
        final Table zs = z.aggBy(AggSum("A", "B"));
        final long za, zb;
        try (CloseablePrimitiveIteratorOfLong zai = zs.longColumnIterator("A")) {
            za = zai.nextLong();
        }
        try (CloseablePrimitiveIteratorOfLong zbi = zs.longColumnIterator("B")) {
            zb = zbi.nextLong();
        }
        assertEquals(expectedSum, za + zb);
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

    private static class SaveQueryTableOptions implements SafeCloseable {
        final boolean oldForceParallel = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        final boolean oldStateless = QueryTable.STATELESS_SELECT_BY_DEFAULT;

        @Override
        public void close() {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = oldForceParallel;
            QueryTable.STATELESS_SELECT_BY_DEFAULT = oldStateless;
        }
    }
}

