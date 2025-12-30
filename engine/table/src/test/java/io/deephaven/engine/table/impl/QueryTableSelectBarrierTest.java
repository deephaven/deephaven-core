//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.expression.Method;
import io.deephaven.api.literal.Literal;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfInt;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
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
    // If you would like to convince yourself that we are reliably producing out-of-order conditions as appropriate,
    // then you can increase this amount to run the tests many times.
    private static final int REPEATS_FOR_CONFIDENCE = 1;

    @Test
    public void testPropertyDefaults() {
        assertTrue(QueryTable.STATELESS_FILTERS_BY_DEFAULT);
        assertTrue(QueryTable.STATELESS_SELECT_BY_DEFAULT);
        assertFalse(QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS);
    }

    @Test
    public void testRepeatedBarrierSelectColumn() {
        for (int ii = 0; ii < REPEATS_FOR_CONFIDENCE; ++ii) {
            System.out.println("Repetition " + ii);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testBarrierSelectColumn();
            }
        }
    }

    private void testBarrierSelectColumn() {
        final int segments = 10;
        final int size = Math.toIntExact(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS * segments);

        final Table x = TableTools.emptyTable(size);
        final SlowedAtomicInteger slow_a = new SlowedAtomicInteger(0, 2);
        QueryScope.addParam("slow_a", slow_a);
        QueryScope.addParam("size", size);

        // use segments times the number of columns we are evaluating threads, so we can start off each of the
        // individual blocks of data at once to maximize "chaos" in
        // terms of the atomic integers being mixed up
        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, segments * 2);
        final ExecutionContext executionContext = ExecutionContext.getContext().withOperationInitializer(threadPool);
        try (final SafeCloseable ignored = executionContext.open();
                final SafeCloseable ignored2 = new SaveQueryTableOptions();
                final SafeCloseable ignored3 = threadPool::shutdown) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
            QueryTable.STATELESS_SELECT_BY_DEFAULT = false;
            QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS = false;

            final SelectColumn sa =
                    SelectColumnFactory.getExpression("A=slow_a.getAndIncrementSlow(0, new int[]{ 0, 1 })");
            final SelectColumn sb =
                    SelectColumnFactory.getExpression("B=slow_a.getAndIncrementSlow(1, new int[]{ 1, 0 })");

            slow_a.reset(0, -1);
            System.out.println(Instant.now() + ": stateful");
            final Table y = x.update(List.of(sa, sb));

            final Table expected = y.updateView("A=i", "B=size + i");
            assertTableEquals(expected, y);

            slow_a.reset(0, 2);
            System.out.println(Instant.now() + ": stateless");
            final Table z = x.update(List.of(SelectColumn.ofStateless(sa), SelectColumn.ofStateless(sb)));

            // these things should happen at the same time, so we should sum to the expected value;
            // but everything should be mixed up
            checkTotalSum(size, z);

            // now check for swizzling
            checkMixedColumns(z, true);

            // and the column itself should not be ordered either
            assertTrue(isOutOfOrder(z, "A"));
            assertTrue(isOutOfOrder(z, "B"));

            // now let's keep things stateless, but insert a barrier so that A and B do not intermix
            slow_a.reset(0, 1);

            System.out.println(Instant.now() + ": barrier");
            final Table u = x.update(List.of(SelectColumn.ofStateless(sa).withDeclaredBarriers(slow_a),
                    SelectColumn.ofStateless(sb).withRespectedBarriers(slow_a)));

            // check for swizzling
            checkMixedColumns(u, false);

            // we should have parallelized each column
            assertTrue(isOutOfOrder(u, "A"));
            assertTrue(isOutOfOrder(u, "B"));

            // check the expected sums
            checkIndividualSums(size, u);
            System.out.println(Instant.now() + ": done");
        }
    }

    // if you would like to convince yourself that we are reliably producing out-of-order conditions as appropriate
    @Test
    public void testRepeatedBarrierSelectable() {
        for (int ii = 0; ii < REPEATS_FOR_CONFIDENCE; ++ii) {
            System.out.println("Repetition " + ii);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testBarrierSelectable();
            }
        }
    }

    private void testBarrierSelectable() {
        QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;

        final int segments = 10;
        final int size = Math.toIntExact(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS * segments);
        final SlowedAtomicInteger slow_a = new SlowedAtomicInteger(0, 2);
        QueryScope.addParam("slow_a", slow_a);
        QueryScope.addParam("size", size);
        final Table x = TableTools.emptyTable(size)
                .updateView("AtomicInt=slow_a")
                .updateView("SecondChecks=new int[]{1, 0, -1, 1}")
                .updateView("FirstChecks=new int[]{0, 1, 0, -1}");

        // use segments times the number of columns we are evaluating threads, so we can start off each of the
        // individual blocks of data at once to maximize "chaos" in
        // terms of the atomic integers being mixed up
        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, segments * 4);
        final ExecutionContext executionContext = ExecutionContext.getContext().withOperationInitializer(threadPool);
        try (final SafeCloseable ignored = executionContext.open();
                final SafeCloseable ignored2 = new SaveQueryTableOptions();
                final SafeCloseable ignored3 = threadPool::shutdown) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
            QueryTable.STATELESS_SELECT_BY_DEFAULT = true;
            QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS = false;

            final Selectable sa =
                    Selectable.of(ColumnName.of("A"), Method.of(ColumnName.of("AtomicInt"), "getAndIncrementSlow",
                            Literal.of(0), ColumnName.of("FirstChecks")));
            final Selectable sb =
                    Selectable.of(ColumnName.of("B"), Method.of(ColumnName.of("AtomicInt"), "getAndIncrementSlow",
                            Literal.of(1), ColumnName.of("SecondChecks")));

            slow_a.reset(0, -1);
            System.out.println(Instant.now() + ": serial");
            final Table y = x.update(List.of(sa.withSerial(), sb.withSerial()));

            final Table expected =
                    x.updateView(List.of(SelectColumn.ofStateless(SelectColumnFactory.getExpression("A=i"),
                            SelectColumnFactory.getExpression("B=size + i"))));
            assertTableEquals(expected, y);

            slow_a.reset(0, 2);

            System.out.println(Instant.now() + ": stateless");
            final Table z = x.update(List.of(sa, sb));

            // these things should happen at the same time, so we should sum to the expected value;
            // but everything should be mixed up
            checkTotalSum(size, z);

            // now check for swizzling
            checkMixedColumns(z, true);

            // and the column itself should not be ordered either
            assertTrue(isOutOfOrder(z, "A"));
            assertTrue(isOutOfOrder(z, "B"));

            // now let's keep things stateless, but insert a barrier so that A and B do not intermix
            slow_a.reset(0, 1);

            System.out.println(Instant.now() + ": barriers");
            final Table u = x.update(List.of(sa.withDeclaredBarriers(slow_a), sb.withRespectedBarriers(slow_a)));

            // check for swizzling
            checkMixedColumns(u, false);

            // we should have parallelized each column
            assertTrue(isOutOfOrder(u, "A"));
            assertTrue(isOutOfOrder(u, "B"));

            // check the expected sums
            checkIndividualSums(size, u);

            // now make B serial; but let A do what it wants
            slow_a.reset(2, 3);
            System.out.println(Instant.now() + ": B serial");
            final Table v = x.update(List.of(sa, sb.withSerial()));
            checkTotalSum(size, v);
            // let a float, even though b is serial
            assertTrue(isOutOfOrder(v, "A"));
            assertFalse(isOutOfOrder(v, "B"));

            // now make A serial; but let B do what it wants
            slow_a.reset(3, 4);
            System.out.println(Instant.now() + ": A serial");
            final Table w = x.update(List.of(sa.withSerial(), sb));
            checkTotalSum(size, w);

            assertFalse(isOutOfOrder(w, "A"));
            assertTrue(isOutOfOrder(w, "B"));

            System.out.println(Instant.now() + ": Done");
        }
    }

    // if you would like to convince yourself that we are reliably producing out-of-order conditions as appropriate
    @Test
    public void testRepeatedImplicitBarriers() {
        for (int ii = 0; ii < REPEATS_FOR_CONFIDENCE; ++ii) {
            System.out.println("Repetition " + ii);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testImplicitBarriers();
            }
        }
    }

    private void testImplicitBarriers() {
        final int segments = 10;
        final int size = Math.toIntExact(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS * segments);
        final SlowedAtomicInteger slow_a = new SlowedAtomicInteger(0, 2);
        QueryScope.addParam("slow_a", slow_a);
        QueryScope.addParam("size", size);
        final Table x = TableTools.emptyTable(size)
                .updateView("AtomicInt=slow_a")
                .updateView("SecondChecks=new int[]{0}")
                .updateView("FirstChecks=new int[]{1}");

        // use segments times the number of columns we are evaluating threads, so we can start off each of the
        // individual blocks of data at once to maximize "chaos" in
        // terms of the atomic integers being mixed up
        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, segments * 4);
        final ExecutionContext executionContext = ExecutionContext.getContext().withOperationInitializer(threadPool);
        try (final SafeCloseable ignored = executionContext.open();
                final SafeCloseable ignored2 = new SaveQueryTableOptions();
                final SafeCloseable ignored3 = threadPool::shutdown) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
            QueryTable.STATELESS_SELECT_BY_DEFAULT = true;
            QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS = true;

            final Selectable sa =
                    Selectable.of(ColumnName.of("A"), Method.of(ColumnName.of("AtomicInt"), "getAndIncrementSlow",
                            Literal.of(0), ColumnName.of("FirstChecks")));
            final Selectable sb =
                    Selectable.of(ColumnName.of("B"), Method.of(ColumnName.of("AtomicInt"), "getAndIncrementSlow",
                            Literal.of(1), ColumnName.of("SecondChecks")));

            final Selectable sc = Selectable.of(ColumnName.of("C"), Literal.of(1));

            slow_a.reset(0, -1);
            System.out.println(Instant.now() + ": serial");
            final Table y = x.update(List.of(sa.withSerial(), sb.withSerial()));

            final Table expected =
                    x.updateView(List.of(SelectColumn.ofStateless(SelectColumnFactory.getExpression("A=i"),
                            SelectColumnFactory.getExpression("B=size + i"))));
            assertTableEquals(expected, y);

            // now do the same thing with an extra column that is not stateless (so we certainly parallelize)
            slow_a.reset(0, -1);
            System.out.println(Instant.now() + ": serial + const");
            final Table y2 = x.update(List.of(sa.withSerial(), sb.withSerial(), sc));
            assertTableEquals(expected.update("C=1"), y2);

            // now turn off the implicit barriers
            QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS = false;
            ((QueryTable) x).clearMemoizedResults();
            slow_a.reset(0, 1);
            slow_a.setMinThreads(1);
            final Table y3 = x.update(List.of(sa.withSerial(), sb.withSerial(), sc));
            checkTotalSum(size, y3);
            checkMixedColumns(y3, true);
            assertFalse(isOutOfOrder(y3, "A"));
            assertFalse(isOutOfOrder(y3, "B"));
            System.out.println(Instant.now() + ": no barrier");
        }
    }

    // if you would like to convince yourself that we are reliably producing out-of-order conditions as appropriate
    @Test
    public void testRepeatedBarrierAcrossShift() {
        for (int ii = 0; ii < REPEATS_FOR_CONFIDENCE; ++ii) {
            System.out.println("Repetition " + ii);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testBarrierAcrossShift();
            }
        }
    }

    private void testBarrierAcrossShift() {
        final int segments = 10;
        final int size = Math.toIntExact(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS * segments);
        final Table x = TableTools.emptyTable(size);
        final SlowedAtomicInteger slow_a = new SlowedAtomicInteger(0, 3);
        QueryScope.addParam("slow_a", slow_a);
        QueryScope.addParam("size", size);

        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, segments * 4);
        final ExecutionContext executionContext = ExecutionContext.getContext().withOperationInitializer(threadPool);
        try (final SafeCloseable ignored = executionContext.open();
                final SafeCloseable ignored2 = new SaveQueryTableOptions();
                final SafeCloseable ignored3 = threadPool::shutdown) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
            QueryTable.STATELESS_SELECT_BY_DEFAULT = false;
            QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS = false;

            final SelectColumn sa =
                    SelectColumnFactory.getExpression("A=slow_a.getAndIncrementSlow(0, new int[] { 0, 1, 0, 1 })");
            final SelectColumn sb =
                    SelectColumnFactory.getExpression("B=slow_a.getAndIncrementSlow(1, new int[] { 1, 0, 0, 1 })");
            final SelectColumn sc = SelectColumnFactory.getExpression("C=B_[i-1]");
            final SelectColumn sd =
                    SelectColumnFactory.getExpression("D=slow_a.getAndIncrementSlow(2, new int[] { 2, 2, 2, -1 })");
            slow_a.reset(0, -1);
            final Table y = x.update(List.of(sa, sb, sc, sd));

            final Table expected =
                    y.updateView("A=i", "B=size + i", "C=i == 0 ? null : size - 1 + i", "D=2 * size + i");
            assertTableEquals(expected, y);

            slow_a.reset(0, 2);
            final Table z = x.update(List.of(SelectColumn.ofStateless(sa), SelectColumn.ofStateless(sb),
                    SelectColumn.ofStateless(sc), SelectColumn.ofStateless(sd)));

            // these things should happen at the same time, so we should sum to the expected value;
            // but everything should be mixed up
            checkTotalSumThree(size, z);

            // we also need to make sure that C is actually B[i - 1]
            assertTrue(z.where("C != B_[i - 1]").isEmpty());

            // now check for swizzling with A and B; but not D because the shift split this into two distinct operations
            // internally
            checkMixedAandB(z);

            // and the column itself should not be ordered either
            assertTrue(isOutOfOrder(z, "A"));
            assertTrue(isOutOfOrder(z, "B"));
            assertTrue(isOutOfOrder(z, "D"));

            // now let's keep things stateless, but insert a barrier so that A and B do not intermix
            slow_a.reset(0, 1);

            final Table u = x.update(List.of(SelectColumn.ofStateless(sa).withDeclaredBarriers(slow_a),
                    SelectColumn.ofStateless(sb).withRespectedBarriers(slow_a), SelectColumn.ofStateless(sc),
                    SelectColumn.ofStateless(sd)));

            // check for swizzling
            checkMixedColumns(u, false);

            // we should have parallelized each column
            assertTrue(isOutOfOrder(u, "A"));
            assertTrue(isOutOfOrder(u, "B"));
            assertTrue(isOutOfOrder(u, "D"));

            // we also need to make sure that C is actually B[i - 1]
            assertTrue(u.where("C != B_[i - 1]").isEmpty());

            checkTotalSumThree(size, u);

            // now let's put the barrier on a and respect it on d, which is trivially going to be enforced; but also
            // ensures we don't blow up with a missing barrier
            slow_a.reset(2, 4);
            final Table v = x.update(List.of(SelectColumn.ofStateless(sa).withDeclaredBarriers(slow_a),
                    SelectColumn.ofStateless(sb), SelectColumn.ofStateless(sc),
                    SelectColumn.ofStateless(sd).withRespectedBarriers(slow_a)));

            // check for swizzling
            checkMixedColumns(v, true);

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
    public void testRepeatedBarrierAliases() {
        for (int ii = 0; ii < REPEATS_FOR_CONFIDENCE; ++ii) {
            System.out.println("Repetition " + ii);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testBarrierAliases();
            }
        }
    }

    private void testBarrierAliases() {
        final int segments = 10;
        final int size = Math.toIntExact(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS * segments);
        final Table x = TableTools.emptyTable(size);
        final int bStart = 10_000_000;
        SlowedAtomicInteger slow_a = new SlowedAtomicInteger(0, 2);
        SlowedAtomicInteger slow_b = new SlowedAtomicInteger(bStart, 2);
        QueryScope.addParam("slow_a", slow_a);
        QueryScope.addParam("slow_b", slow_b);
        QueryScope.addParam("bStart", bStart);
        QueryScope.addParam("size", size);

        // use segments times the number of columns we are evaluating threads, so we can start off each of the
        // individual blocks of data at once to maximize "chaos" in
        // terms of the atomic integers being mixed up
        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, segments * 4);
        final ExecutionContext executionContext = ExecutionContext.getContext().withOperationInitializer(threadPool);
        try (final SafeCloseable ignored = executionContext.open();
                final SafeCloseable ignored2 = new SaveQueryTableOptions();
                final SafeCloseable ignored3 = threadPool::shutdown) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
            QueryTable.STATELESS_SELECT_BY_DEFAULT = false;
            QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS = false;

            final SelectColumn sa = SelectColumnFactory
                    .getExpression("A=slow_a.getAndIncrementSlow(0, new int[] { 0, 1 })");
            final SelectColumn sb = SelectColumnFactory
                    .getExpression("B=slow_b.getAndIncrementSlow(0, new int[] { 0, 1 })");
            final SelectColumn sc = SelectColumnFactory.getExpression("C=A");
            final SelectColumn sd = SelectColumnFactory.getExpression("D=B");
            final SelectColumn sa1 =
                    SelectColumnFactory.getExpression("A=slow_b.getAndIncrementSlow(1, new int[] { 1 })");
            final SelectColumn sb1 =
                    SelectColumnFactory.getExpression("B=slow_a.getAndIncrementSlow(1, new int[] { 1 })");

            // let a and b be fast since we are serial
            slow_a.reset(0, -1);
            slow_b.reset(0, -1);
            final Table y = x.update(List.of(sb, sa, sc, sd, sa1, sb1));

            final Table expected =
                    y.updateView("C=i", "D=bStart + i", "A=bStart + size + i", "B=size + i");
            assertTableEquals(expected, y);

            // a and b are slow until the second column starts doing work
            slow_a.reset(0, 2);
            slow_b.reset(0, 2);

            final Table z = x.update(List.of(SelectColumn.ofStateless(sb), SelectColumn.ofStateless(sa),
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
            assertTrue(getValInt(min_z, "A") < getValInt(max_z, "D"));

            // but now we add a barrier to A(1); and then respect it for B(2)
            // let A proceed fast after it gets out of order, so that we don't get hung up on a thread sleep for each
            // thing
            slow_a.reset(0, 1);
            slow_b.reset(0, 2);
            final Table u =
                    x.update(List.of(SelectColumn.ofStateless(sb),
                            SelectColumn.ofStateless(sa).withDeclaredBarriers(slow_a),
                            SelectColumn.ofStateless(sc), SelectColumn.ofStateless(sd), SelectColumn.ofStateless(sa1),
                            SelectColumn.ofStateless(sb1).withRespectedBarriers(slow_a)));

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

    @Test
    public void testBarrierErrors() {
        final Table t = TableTools.emptyTable(1);

        final IllegalArgumentException iae1 = assertThrows(IllegalArgumentException.class,
                () -> t.update(List.of(SelectColumnFactory.getExpression("A=1").withDeclaredBarriers(t))));
        assertEquals(
                "Constant values are not evaluated during select() and update() processing, therefore may not declare barriers",
                iae1.getMessage());
        final IllegalArgumentException iae2 = assertThrows(IllegalArgumentException.class,
                () -> t.update(List.of(SelectColumnFactory.getExpression("A=i").withDeclaredBarriers(t),
                        SelectColumnFactory.getExpression("B=1").withRespectedBarriers(t))));
        assertEquals(
                "Constant values are not evaluated during select() and update() processing, therefore may not respect barriers",
                iae2.getMessage());
        final IllegalArgumentException iae3 = assertThrows(IllegalArgumentException.class,
                () -> t.update(List.of(SelectColumnFactory.getExpression("A=i").withDeclaredBarriers(t),
                        SelectColumnFactory.getExpression("B=2*i").withDeclaredBarriers(t))));
        assertEquals("Duplicate barrier, emptyTable(1), declared for A and B", iae3.getMessage());
        final IllegalArgumentException iae4 = assertThrows(IllegalArgumentException.class,
                () -> t.update(List.of(SelectColumnFactory.getExpression("A=i"),
                        SelectColumnFactory.getExpression("B=2*i").withRespectedBarriers(t))));
        assertEquals("Respected barrier, emptyTable(1), is not defined for B", iae4.getMessage());

        final IllegalArgumentException iae5 = assertThrows(IllegalArgumentException.class,
                () -> t.view(List.of(SelectColumnFactory.getExpression("A=i").withDeclaredBarriers(t),
                        SelectColumnFactory.getExpression("B=i").withRespectedBarriers(t))));
        assertEquals("view and updateView cannot respect barriers", iae5.getMessage());
    }

    @Test
    public void testMergingAndCopying() {
        final Object a = new Object();
        final Object b = new Object();
        final Object c = new Object();
        final Object d = new Object();

        final SelectColumn sc = SelectColumnFactory.getExpression("A=B * 2");

        assertEquals(List.of(a), List.of(sc.withDeclaredBarriers(a).declaredBarriers()));
        assertEquals(List.of(b), List.of(sc.withDeclaredBarriers(b).declaredBarriers()));
        assertEquals(Set.of(a, b), Set.of(sc.withDeclaredBarriers(a, b).declaredBarriers()));
        assertEquals(Set.of(a, b), Set.of(sc.withDeclaredBarriers(a).withDeclaredBarriers(b).declaredBarriers()));

        assertEquals(List.of(a), List.of(sc.withRespectedBarriers(a).respectedBarriers()));
        assertEquals(List.of(b), List.of(sc.withRespectedBarriers(b).respectedBarriers()));
        assertEquals(Set.of(a, b), Set.of(sc.withRespectedBarriers(a, b).respectedBarriers()));
        assertEquals(Set.of(a, b), Set.of(sc.withRespectedBarriers(a).withRespectedBarriers(b).respectedBarriers()));


        assertEquals(List.of(a), List.of(sc.withDeclaredBarriers(a).copy().declaredBarriers()));
        assertEquals(List.of(b), List.of(sc.withDeclaredBarriers(b).copy().declaredBarriers()));
        assertEquals(Set.of(a, b), Set.of(sc.withDeclaredBarriers(a, b).copy().declaredBarriers()));
        assertEquals(Set.of(a, b),
                Set.of(sc.withDeclaredBarriers(a).withDeclaredBarriers(b).copy().declaredBarriers()));

        assertEquals(List.of(a), List.of(sc.withDeclaredBarriers(a).withRespectedBarriers(c).declaredBarriers()));
        assertEquals(List.of(c), List.of(sc.withDeclaredBarriers(a).withRespectedBarriers(c).respectedBarriers()));
        assertEquals(List.of(c),
                List.of(sc.withDeclaredBarriers(a).withRespectedBarriers(c).withDeclaredBarriers(b)
                        .respectedBarriers()));
        assertEquals(Set.of(a, b),
                Set.of(sc.withDeclaredBarriers(a).withRespectedBarriers(c).withDeclaredBarriers(b)
                        .declaredBarriers()));
        assertEquals(Set.of(a, b),
                Set.of(sc.withRespectedBarriers(c).withDeclaredBarriers(a).withRespectedBarriers(d)
                        .withDeclaredBarriers(b)
                        .declaredBarriers()));
        assertEquals(Set.of(c, d), Set
                .of(sc.withRespectedBarriers(c).withDeclaredBarriers(b).withRespectedBarriers(d).withDeclaredBarriers(b)
                        .respectedBarriers()));

        final SelectColumn sc2 = sc.withRespectedBarriers(c).withDeclaredBarriers(a).withRespectedBarriers(d)
                .withDeclaredBarriers(b).copy();
        assertEquals(Set.of(a, b), Set.of(sc2.declaredBarriers()));
        assertEquals(Set.of(c, d), Set.of(sc2.respectedBarriers()));

        final SelectColumn sc3 = SelectColumn.ofStateless(sc2);
        assertTrue(sc3.isStateless());
        final SelectColumn sc4 = sc3.withSerial().withRespectedBarriers().withDeclaredBarriers();
        assertFalse(sc4.isStateless());
        assertEquals(Set.of(a, b), Set.of(sc4.declaredBarriers()));
        assertEquals(Set.of(c, d), Set.of(sc4.respectedBarriers()));

        // deduplication
        assertEquals(Set.of(sc4.declaredBarriers()), Set.of(sc4.withDeclaredBarriers(b, b).declaredBarriers()));
        assertEquals(sc4.declaredBarriers().length, sc4.withDeclaredBarriers(b, b).declaredBarriers().length);
        assertEquals(Set.of(sc4.respectedBarriers()), Set.of(sc4.withRespectedBarriers(c, c).respectedBarriers()));
        assertEquals(sc4.respectedBarriers().length, sc4.withRespectedBarriers(c, c).respectedBarriers().length);
    }

    @Test
    public void testMergingSelectable() {
        final Object a = new Object();
        final Object b = new Object();
        final Object c = new Object();
        final Object d = new Object();

        final Selectable ss = Selectable.of(ColumnName.of("A"), RawString.of("B + 7"));
        assertNull(ss.isSerial());

        assertEquals(List.of(a), List.of(ss.withDeclaredBarriers(a).declaredBarriers()));
        assertEquals(List.of(b), List.of(ss.withDeclaredBarriers(b).declaredBarriers()));
        assertEquals(Set.of(a, b), Set.of(ss.withDeclaredBarriers(a, b).declaredBarriers()));
        assertEquals(Set.of(a, b), Set.of(ss.withDeclaredBarriers(a).withDeclaredBarriers(b).declaredBarriers()));

        assertEquals(List.of(a), List.of(ss.withRespectedBarriers(a).respectedBarriers()));
        assertEquals(List.of(b), List.of(ss.withRespectedBarriers(b).respectedBarriers()));
        assertEquals(Set.of(a, b), Set.of(ss.withRespectedBarriers(a, b).respectedBarriers()));
        assertEquals(Set.of(a, b), Set.of(ss.withRespectedBarriers(a).withRespectedBarriers(b).respectedBarriers()));

        assertEquals(List.of(a), List.of(ss.withDeclaredBarriers(a).withRespectedBarriers(c).declaredBarriers()));
        assertEquals(List.of(c), List.of(ss.withDeclaredBarriers(a).withRespectedBarriers(c).respectedBarriers()));
        assertEquals(List.of(c),
                List.of(ss.withDeclaredBarriers(a).withRespectedBarriers(c).withDeclaredBarriers(b)
                        .respectedBarriers()));
        assertEquals(Set.of(a, b),
                Set.of(ss.withDeclaredBarriers(a).withRespectedBarriers(c).withDeclaredBarriers(b)
                        .declaredBarriers()));
        assertEquals(Set.of(a, b),
                Set.of(ss.withRespectedBarriers(c).withDeclaredBarriers(a).withRespectedBarriers(d)
                        .withDeclaredBarriers(b)
                        .declaredBarriers()));

        final Selectable ss2 = ss.withRespectedBarriers(c).withSerial().withDeclaredBarriers(b).withRespectedBarriers(d)
                .withDeclaredBarriers(b);
        assertEquals(Set.of(c, d), Set.of(ss2.respectedBarriers()));
        assertEquals(List.of(b), List.of(ss2.declaredBarriers()));
        assertTrue(ss2.isSerial());
        assertTrue(ss2.withSerial().isSerial());
        assertTrue(ss2.withSerial().withRespectedBarriers(1).withDeclaredBarriers(2).isSerial());

        // deduplication
        assertEquals(Set.of(ss2.declaredBarriers()), Set.of(ss2.withDeclaredBarriers(b, b).declaredBarriers()));
        assertEquals(ss2.declaredBarriers().length, ss2.withDeclaredBarriers(b, b).declaredBarriers().length);
        assertEquals(Set.of(ss2.respectedBarriers()), Set.of(ss2.withRespectedBarriers(c, c).respectedBarriers()));
        assertEquals(ss2.respectedBarriers().length, ss2.withRespectedBarriers(c, c).respectedBarriers().length);
    }

    @Test
    public void testDH20625() {
        final Table source = TableTools.emptyTable(1_000)
                .update("a = ii % 5", "b = ii % 11", "c = ii % 17", "d = ii");

        final Object b1 = new Object();
        final Object b2 = new Object();

        Table result;

        // update with a, c passed as real columns
        result = source.update(List.of(
                Selectable.parse("a").withDeclaredBarriers(b1, b2),
                Selectable.parse("c").withRespectedBarriers(b2).withSerial(),
                Selectable.parse("Sum = a + b + c").withRespectedBarriers(b1)));
        // simple test, verify we have the same number of rows
        assertEquals(source.size(), result.size());

        // select with a, c passed as real columns
        result = source.select(List.of(
                Selectable.parse("a").withDeclaredBarriers(b1, b2),
                Selectable.parse("c").withRespectedBarriers(b2).withSerial(),
                Selectable.parse("Sum = a + b + c").withRespectedBarriers(b1)));
        // simple test, verify we have the same number of rows
        assertEquals(source.size(), result.size());
    }

    @Test
    public void testDH20625_proxy() {
        // Create a partitioned table with 4 partitions
        final Table source = TableTools.emptyTable(1_000)
                .update("a = ii % 5", "b = ii % 11", "c = ii % 17", "d = ii");
        final PartitionedTable pt = source.partitionBy("a");

        final Object b1 = new Object();
        final Object b2 = new Object();

        PartitionedTable.Proxy result_proxy;
        // update test
        result_proxy = pt.proxy().update(List.of(
                Selectable.parse("a").withDeclaredBarriers(b1, b2),
                Selectable.parse("c").withRespectedBarriers(b2).withSerial(),
                Selectable.parse("Sum = a + b + c").withRespectedBarriers(b1)));
        assertEquals(pt.constituents().length, result_proxy.target().constituents().length);
        for (int i = 0; i < pt.constituents().length; i++) {
            final Table ct = pt.constituents()[i];
            final Table rct = result_proxy.target().constituents()[i];

            // simple test, verify we have the same number of rows
            assertEquals(ct.size(), rct.size());
        }

        // select test
        result_proxy = pt.proxy().select(List.of(
                Selectable.parse("a").withDeclaredBarriers(b1, b2),
                Selectable.parse("c").withRespectedBarriers(b2).withSerial(),
                Selectable.parse("Sum = a + b + c").withRespectedBarriers(b1)));
        assertEquals(pt.constituents().length, result_proxy.target().constituents().length);
        for (int i = 0; i < pt.constituents().length; i++) {
            final Table ct = pt.constituents()[i];
            final Table rct = result_proxy.target().constituents()[i];

            // simple test, verify we have the same number of rows
            assertEquals(ct.size(), rct.size());
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

    private static void checkMixedColumns(final Table result, final boolean expectMixed) {
        final Table min = result.aggBy(AggMin("A", "B"));
        final Table max = result.aggBy(AggMax("A", "B"));

        final boolean aMixedWithB = getValInt(max, "A") > getValInt(min, "B");

        if (expectMixed) {
            assertTrue(aMixedWithB);
        } else {
            assertFalse(aMixedWithB);
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
        final long doubleSize = 2L * size;
        final long expectedSum = ((doubleSize - 1) * doubleSize) / 2;
        final Table zs = z.aggBy(AggSum("A", "B"));
        final long za = getValLong(zs, "A");
        final long zb = getValLong(zs, "B");
        assertEquals(expectedSum, za + zb);
    }

    private static void checkTotalSumThree(int size, Table z) {
        final long tripleSize = 3L * size;
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
        final boolean oldSerialBarriers = QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS;

        @Override
        public void close() {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = oldForceParallel;
            QueryTable.STATELESS_SELECT_BY_DEFAULT = oldStateless;
            QueryTable.SERIAL_SELECT_IMPLICIT_BARRIERS = oldSerialBarriers;
        }
    }

    /**
     * A utility class that wraps an AtomicInteger to provide additional synchronization and delay mechanisms for
     * testing purpose, simulating conditions that involve inter-thread interactions and timing delays.
     *
     * <p>
     * This test is designed to always produce data out-of-order when it is permitted. We can't guarantee that if it is
     * not-permitted, then the data will also be out-of-order - but it will be sometimes. We verify in both cases that
     * the data is either out-of-order or not out-of-order. If we have no bugs, then we should not be a heisentest. If
     * there are bugs, then it is possible we don't always catch them (because of the vicissitudes of concurrency).
     * </p>
     */
    public static class SlowedAtomicInteger {
        private final AtomicInteger wrapped;
        private final int resetTo;
        final Random random = new Random(0);
        private final List<Set<Thread>> columnThreads;
        int minChecks;
        int maxChecks;
        int minThreads = 2;

        /**
         *
         * @param resetTo the initial value of the wrapped AtomicInteger, and what it is set to upon
         *        {@link #reset(int, int)}
         * @param columnThreadCount the number of columns that will be using this object; each column has a different
         *        set of to track the number of threads that have accessed it
         */
        SlowedAtomicInteger(int resetTo, int columnThreadCount) {
            this.wrapped = new AtomicInteger(resetTo);
            this.resetTo = resetTo;
            columnThreads = new ArrayList<>(columnThreadCount);
            for (int ii = 0; ii < columnThreadCount; ++ii) {
                columnThreads.add(Collections.synchronizedSet(new HashSet<>()));
            }
            minChecks = 0;
            maxChecks = columnThreadCount;
        }

        /**
         * Do the get and increment on the underlying atomic integer, possibly sleeping to cause data to be interspersed
         * if there are not declared barriers.
         *
         * @param columnNumber the column number we are updating
         * @param checkThreads an array of column numbers that we should verify had at least two threads access them. -1
         *        indicates that we are not checking this position. The {@link #reset(int, int)} call enables us to
         *        control which of these array elements are actually validated. We don't want to validate all of the
         *        elements, if we have serial or barriers, because that will make the test horribly slow (since we'll
         *        never have multiple threads hit at once).
         * @return the old value of the atomic integer
         */
        public int getAndIncrementSlow(final int columnNumber,
                final int[] checkThreads) {
            final int retVal = wrapped.getAndIncrement();

            if (maxChecks >= 0) {
                mabyeWaitABit(columnNumber, checkThreads);
            }

            return retVal;
        }

        private void mabyeWaitABit(int columnNumber, int[] checkThreads) {
            if (columnNumber >= 0) {
                columnThreads.get(columnNumber).add(Thread.currentThread());
            }

            boolean sleepRequired = false;
            for (int ii = minChecks; ii < maxChecks && ii < checkThreads.length; ++ii) {
                int checkThread = checkThreads[ii];
                if (checkThreads[ii] < 0) {
                    continue;
                }
                sleepRequired = sleepRequired || columnThreads.get(checkThread).size() < minThreads;
            }

            if (sleepRequired) {
                try {
                    Thread.sleep(random.nextInt(10));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        void reset(final int minChecks, final int maxChecks) {
            wrapped.set(resetTo);
            this.minChecks = minChecks;
            this.maxChecks = maxChecks;
            columnThreads.forEach(Set::clear);
        }

        void setMinThreads(int minThreads) {
            this.minThreads = minThreads;
        }
    }
}

