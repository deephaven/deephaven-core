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

import static io.deephaven.api.agg.Aggregation.*;
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

    @Test
    public void testBarrierAcrossShift() {
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

        final int size = 1_000_000;
        final Table x = TableTools.emptyTable(size);
        final AtomicInteger a = new AtomicInteger();
        QueryScope.addParam("a", a);

        try (final SafeCloseable ignored = new SaveQueryTableOptions()) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

            final SelectColumn sa = SelectColumnFactory.getExpression("A=a.getAndIncrement()");
            final SelectColumn sb = SelectColumnFactory.getExpression("B=a.getAndIncrement()");
            final SelectColumn sc = SelectColumnFactory.getExpression("C=B_[i-1]");
            final SelectColumn sd = SelectColumnFactory.getExpression("D=a.getAndIncrement()");
            final Table y = x.update(List.of(sa, sb, sc, sd));

            final Table expected =
                    y.updateView("A=i", "B=1_000_000 + i", "C=i == 0 ? null : 999_999 + i", "D=2_000_000 + i");
            assertTableEquals(expected, y);

            a.set(0);

            final Table z = x.update(List.of(SelectColumn.ofStateless(sa), SelectColumn.ofStateless(sb),
                    SelectColumn.ofStateless(sc), SelectColumn.ofStateless(sd)));

            // these things should happen at the same time, so we should sum to the expected value;
            // but everything should be mixed up
            checkTotalSumThree(size, z);

            // we also need to make sure that C is actually B[i - 1]
            assertTrue(z.where("C != B_[i - 1]").isEmpty());

            TableTools.show(z);

            // now check for swizzling with A and B; but not D because the shift split this into two distinct operations
            // internally
            checkMixedAandB(z);

            // and the column itself should not be ordered either
            assertTrue(isOutOfOrder(z, "A"));
            assertTrue(isOutOfOrder(z, "B"));
            assertTrue(isOutOfOrder(z, "D"));

            // now let's keep things stateless, but insert a barrier so that A and B do not intermix
            a.set(0);

            final Table u = x.update(List.of(SelectColumn.ofStateless(sa).withBarriers(a),
                    SelectColumn.ofStateless(sb).respectsBarriers(a), SelectColumn.ofStateless(sc),
                    SelectColumn.ofStateless(sd)));

            // check for swizzling
            checkMixedColumns(u, size, false);

            // we should have parallelized each column
            assertTrue(isOutOfOrder(u, "A"));
            assertTrue(isOutOfOrder(u, "B"));
            assertTrue(isOutOfOrder(u, "D"));

            // we also need to make sure that C is actually B[i - 1]
            assertTrue(u.where("C != B_[i - 1]").isEmpty());

            checkTotalSumThree(size, u);

            // now let's put the barrier on a and respect it on d, which is trivially going to be enforced; but also
            // ensures we don't blow up with a missing barrier
            a.set(0);
            final Table v = x.update(List.of(SelectColumn.ofStateless(sa).withBarriers(a),
                    SelectColumn.ofStateless(sb), SelectColumn.ofStateless(sc),
                    SelectColumn.ofStateless(sd).respectsBarriers(a)));

            // check for swizzling
            TableTools.show(v);
            checkMixedColumns(v, size, true);

            // we should have parallelized each column
            assertTrue(isOutOfOrder(v, "A"));
            assertTrue(isOutOfOrder(v, "B"));
            assertTrue(isOutOfOrder(v, "D"));

            // we also need to make sure that C is actually B[i - 1]
            assertTrue(v.where("C != B_[i - 1]").isEmpty());
            checkTotalSumThree(size, u);

        }
    }

    @Test
    public void testBarrierAliases() {
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

        final int size = 1_000_000;
        final Table x = TableTools.emptyTable(size);
        final AtomicInteger a = new AtomicInteger();
        final AtomicInteger b = new AtomicInteger(10_000_000);
        QueryScope.addParam("a", a);
        QueryScope.addParam("b", b);

        try (final SafeCloseable ignored = new SaveQueryTableOptions()) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

            final SelectColumn sa = SelectColumnFactory.getExpression("A=a.getAndIncrement()");
            final SelectColumn sb = SelectColumnFactory.getExpression("B=b.getAndIncrement()");
            final SelectColumn sc = SelectColumnFactory.getExpression("C=A");
            final SelectColumn sd = SelectColumnFactory.getExpression("D=B");
            final SelectColumn sa1 = SelectColumnFactory.getExpression("A=b.getAndIncrement()");
            final SelectColumn sb1 = SelectColumnFactory.getExpression("B=a.getAndIncrement()");
            final Table y = x.update(List.of(sa, sb, sc, sd, sa1, sb1));

            final Table expected =
                    y.updateView("C=i", "D=10_000_000 + i", "A=11_000_000 + i", "B=1_000_000 + i");
            assertTableEquals(expected, y);

            a.set(0);
            b.set(10_000_000);

            final Table z = x.update(List.of(SelectColumn.ofStateless(sa), SelectColumn.ofStateless(sb),
                    SelectColumn.ofStateless(sc), SelectColumn.ofStateless(sd), SelectColumn.ofStateless(sa1),
                    SelectColumn.ofStateless(sb1)));

            assertTrue(isOutOfOrder(z, "A"));
            assertTrue(isOutOfOrder(z, "B"));
            assertTrue(isOutOfOrder(z, "C"));
            assertTrue(isOutOfOrder(z, "D"));

            // and it is a free-for-all in terms of the order of evaluation
            final Table min_z = z.aggBy(AggMin("A", "B", "C", "D"));
            final Table max_z = z.aggBy(AggMax("A", "B", "C", "D"));
            assertTrue(getValInt(min_z, "B") < getValInt(max_z, "C"));
            assertTrue(getValInt(min_z, "B") < getValInt(max_z, "D"));

            // but now we add a barrier to A(1); and then respect it for B(2)
            a.set(0);
            b.set(10_000_000);
            final Table u = x.update(List.of(SelectColumn.ofStateless(sa).withBarriers(a), SelectColumn.ofStateless(sb),
                    SelectColumn.ofStateless(sc), SelectColumn.ofStateless(sd), SelectColumn.ofStateless(sa1),
                    SelectColumn.ofStateless(sb1).respectsBarriers(a)));

            assertTrue(isOutOfOrder(u, "A"));
            assertTrue(isOutOfOrder(u, "B"));
            assertTrue(isOutOfOrder(u, "C"));
            assertTrue(isOutOfOrder(u, "D"));

            final Table min_u = z.aggBy(AggMin("A", "B", "C", "D"));
            final Table max_u = z.aggBy(AggMax("A", "B", "C", "D"));
            assertTrue(getValInt(min_u, "A") > getValInt(max_u, "C"));
            assertTrue(getValInt(min_u, "B") < getValInt(max_u, "D"));

        }
    }


    private static void checkIndividualSums(int size, Table u) {
        final long expectedSumA = (((long) size - 1) * size) / 2;
        final Table us = u.aggBy(AggSum("A", "B"));
        final long ua = getValLong(us, "A");
        final long ub = getValLong(us, "B");
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

    /**
     * Check A mixes with B, but neither mix with D.
     */
    private static void checkMixedAandB(final Table result) {
        final Table min = result.aggBy(AggMin("A", "B", "D"));
        final Table max = result.aggBy(AggMax("A", "B", "D"));

        final boolean aMixedWithB = getValInt(max, "A") > getValInt(min, "B");
        final boolean aMixedWithD = getValInt(max, "A") > getValInt(min, "D");
        final boolean bMixedWithD = getValInt(max, "B") > getValInt(min, "D");

        assertTrue(aMixedWithB);
        assertFalse(aMixedWithD);
        assertFalse(bMixedWithD);
    }

    private static void checkTotalSum(int size, Table z) {
        final long doubleSize = size * 2;
        final long expectedSum = ((doubleSize - 1) * doubleSize) / 2;
        final Table zs = z.aggBy(AggSum("A", "B"));
        final long za = getValLong(zs, "A");
        final long zb = getValLong(zs, "B");
        assertEquals(expectedSum, za + zb);
    }

    private static void checkTotalSumThree(int size, Table z) {
        final long tripleSize = size * 3;
        final long expectedSum = ((tripleSize - 1) * tripleSize) / 2;
        final Table zs = z.aggBy(AggSum("A", "B", "D"));
        final long za = getValLong(zs, "A");
        final long zb = getValLong(zs, "B");
        final long zd = getValLong(zs, "D");
        assertEquals(expectedSum, za + zb + zd);
    }

    private static long getValLong(Table zs, final String a) {
        try (CloseablePrimitiveIteratorOfLong zai = zs.longColumnIterator(a)) {
            return zai.nextLong();
        }
    }

    private static long getValInt(Table zs, final String a) {
        try (CloseablePrimitiveIteratorOfInt zai = zs.integerColumnIterator(a)) {
            return zai.nextInt();
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

