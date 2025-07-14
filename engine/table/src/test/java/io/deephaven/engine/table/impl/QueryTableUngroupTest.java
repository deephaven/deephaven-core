//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.vector.*;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static io.deephaven.engine.table.impl.SnapshotTestUtils.verifySnapshotBarrageMessage;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.assertArrayEquals;

/**
 * Test of QueryTable functionality.
 * <p>
 * This test used to be a catch-all, but at over 7,000 lines became unwieldy. It is still somewhat of a catch-all, but
 * some specific classes of tests have been broken out.
 * <p>
 * See also {@link QueryTableAggregationTest}, {@link QueryTableJoinTest}, {@link QueryTableSelectUpdateTest},
 * {@link QueryTableFlattenTest}, and {@link QueryTableSortTest}.
 */
@Category(OutOfBandTest.class)
public class QueryTableUngroupTest extends QueryTableTestBase {

    public void testShifts() {
        final QueryTable source = testRefreshingTable(
                intCol("Key", 1, 2, 3),
                col("Value", new int[] {101}, new int[] {201, 202}, new int[] {301, 302, 303}));

        final Table expected =
                TableTools.newTable(intCol("Key", 1, 2, 2, 3, 3, 3), intCol("Value", 101, 201, 202, 301, 302, 303));
        TableTools.showWithRowSet(source);

        final Table ungrouped = source.ungroup();
        assertTableEquals(expected, ungrouped);

        TableTools.showWithRowSet(source);
        TableTools.showWithRowSet(ungrouped);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(source, i(2));
            TstUtils.addToTable(source, i(10), intCol("Key", 3), col("Value", new int[] {301, 302, 303}));
            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(2, 2, 8);
            final RowSetShiftData sd = sb.build();
            source.notifyListeners(new TableUpdateImpl(i(), i(), i(), sd, ModifiedColumnSet.EMPTY));
        });

        TableTools.showWithRowSet(source);
        TableTools.showWithRowSet(ungrouped);

        assertTableEquals(expected, ungrouped);
    }

    public void testUngroupWithNullSecondColumn() {
        final QueryTable qt = testRefreshingTable(
                col("C1", new int[] {1, 2, 3}, new int[0], null),
                col("C2", new int[] {3, 2, 1}, new int[0], null));

        final Table ug = qt.ungroup(false, "C1", "C2");
        setExpectError(false);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet mods = i(0, 2);
            addToTable(qt, mods,
                    col("C1", null, new int[] {4, 5, 6}),
                    col("C2", null, new int[] {6, 5, 4}));
            qt.notifyListeners(i(), i(), mods);
        });
        final QueryTable expected = testTable(
                col("C1", 4, 5, 6),
                col("C2", 6, 5, 4));
        assertTableEquals(expected, ug);
    }

    public void testUngroupingAgnostic() {
        final int[][] data1 = new int[][] {new int[] {4, 5, 6}, new int[0], new int[] {7, 8}};
        final Table table = testRefreshingTable(col("X", 1, 2, 3),
                col("Y", new String[] {"a", "b", "c"}, ArrayTypeUtils.EMPTY_STRING_ARRAY,
                        new String[] {"d", "e"}),
                col("Z", data1));

        Table t1 = table.ungroup("Y", "Z");
        assertEquals(5, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertArrayEquals(new int[] {1, 1, 1, 3, 3}, ColumnVectors.ofInt(t1, "X").toArray());
        assertArrayEquals(new String[] {"a", "b", "c", "d", "e"},
                ColumnVectors.ofObject(t1, "Y", String.class).toArray());
        assertArrayEquals(new int[] {4, 5, 6, 7, 8}, ColumnVectors.ofInt(t1, "Z").toArray());

        t1 = table.ungroup();
        assertEquals(5, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertArrayEquals(new int[] {1, 1, 1, 3, 3}, ColumnVectors.ofInt(t1, "X").toArray());
        assertArrayEquals(new String[] {"a", "b", "c", "d", "e"},
                ColumnVectors.ofObject(t1, "Y", String.class).toArray());
        assertArrayEquals(new int[] {4, 5, 6, 7, 8}, ColumnVectors.ofInt(t1, "Z").toArray());

        final int[][] data = new int[][] {new int[] {4, 5, 6}, new int[0], new int[] {7, 8}};

        final Table table2 = testRefreshingTable(col("X", 1, 2, 3),
                col("Y", new String[] {"a", "b", "c"}, new String[] {"d", "e"},
                        ArrayTypeUtils.EMPTY_STRING_ARRAY),
                col("Z", data));

        final AssertionFailure e = Assert.assertThrows(AssertionFailure.class, table2::ungroup);
        assertEquals(
                "Assertion failed: asserted sizes[idx] == Array.getLength(arrayColumn.get(idx)), instead referenceColumn == \"Y\", name == \"Z\", row == 1.",
                e.getMessage());

        final AssertionFailure e2 = Assert.assertThrows(AssertionFailure.class, () -> table2.ungroup("Y", "Z"));
        assertEquals(
                "Assertion failed: asserted sizes[idx] == Array.getLength(arrayColumn.get(idx)), instead referenceColumn == \"Y\", name == \"Z\", row == 1.",
                e2.getMessage());

        t1 = table2.ungroup("Y");
        assertEquals(5, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertArrayEquals(new int[] {1, 1, 1, 2, 2}, ColumnVectors.ofInt(t1, "X").toArray());
        assertArrayEquals(new String[] {"a", "b", "c", "d", "e"},
                ColumnVectors.ofObject(t1, "Y", String.class).toArray());
        show(t1);
        show(t1.ungroup("Z"));
        t1 = t1.ungroup("Z");
        assertEquals(9, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1}, ColumnVectors.ofInt(t1, "X").toArray());
        assertArrayEquals(new String[] {"a", "a", "a", "b", "b", "b", "c", "c", "c"},
                ColumnVectors.ofObject(t1, "Y", String.class).toArray());
        assertArrayEquals(new int[] {4, 5, 6, 4, 5, 6, 4, 5, 6}, ColumnVectors.ofInt(t1, "Z").toArray());


        t1 = table2.ungroup("Z");
        assertEquals(5, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertArrayEquals(new int[] {1, 1, 1, 3, 3}, ColumnVectors.ofInt(t1, "X").toArray());
        assertArrayEquals(new int[] {4, 5, 6, 7, 8}, ColumnVectors.ofInt(t1, "Z").toArray());
        t1 = t1.ungroup("Y");
        assertEquals(9, t1.size());
        assertEquals(Arrays.asList("X", "Y", "Z"), t1.getDefinition().getColumnNames());
        assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1, 1, 1, 1}, ColumnVectors.ofInt(t1, "X").toArray());
        assertArrayEquals(new int[] {4, 4, 4, 5, 5, 5, 6, 6, 6}, ColumnVectors.ofInt(t1, "Z").toArray());
        assertArrayEquals(new String[] {"a", "b", "c", "a", "b", "c", "a", "b", "c"},
                ColumnVectors.ofObject(t1, "Y", String.class).toArray());
    }

    public void testUngroupConstructSnapshotOfBoxedNull() {
        final Table t =
                testRefreshingTable(i(0).toTracking())
                        .update("X = new Integer[]{null, 2, 3}", "Z = new Integer[]{4, 5, null}");
        final Table ungrouped = t.ungroup();
        try (final BarrageMessage snap =
                ConstructSnapshot.constructBackplaneSnapshot(this, (BaseTable<?>) ungrouped)) {
            testUngroupConstructSnapshotBoxedNullAllColumnHelper(snap);
        }

        // Snapshot the second column for last two rows
        final BitSet columnsToSnapshot = new BitSet(2);
        columnsToSnapshot.set(1);
        final RowSequence rowsToSnapshot = RowSequenceFactory.forRange(1, 2);
        try (final BarrageMessage snap =
                ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(this, (BaseTable<?>) ungrouped,
                        columnsToSnapshot, rowsToSnapshot, null)) {
            testUngroupConstructSnapshotBoxedNullFewColumnsHelper(snap);
        }

        final Table selected = ungrouped.select(); // Will convert column sources to in memory
        try (final BarrageMessage snap =
                ConstructSnapshot.constructBackplaneSnapshot(this, (BaseTable<?>) selected)) {
            testUngroupConstructSnapshotBoxedNullAllColumnHelper(snap);
        }

        try (final BarrageMessage snap =
                ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(this, (BaseTable<?>) selected,
                        columnsToSnapshot, RowSequenceFactory.forRange(1, 2), null)) {
            testUngroupConstructSnapshotBoxedNullFewColumnsHelper(snap);
        }
    }

    private static void testUngroupConstructSnapshotBoxedNullAllColumnHelper(@NotNull final BarrageMessage snap) {
        assertEquals(snap.rowsAdded, i(0, 1, 2));
        final List<Chunk<Values>> firstColChunk = snap.addColumnData[0].data;
        final int[] firstColExpected = new int[] {QueryConstants.NULL_INT, 2, 3};
        final List<Chunk<Values>> secondColChunk = snap.addColumnData[1].data;
        final int[] secondColExpected = new int[] {4, 5, QueryConstants.NULL_INT};
        for (int i = 0; i < 3; i++) {
            assertEquals(firstColChunk.get(0).asIntChunk().get(i), firstColExpected[i]);
            assertEquals(secondColChunk.get(0).asIntChunk().get(i), secondColExpected[i]);
        }
    }

    private static void testUngroupConstructSnapshotBoxedNullFewColumnsHelper(@NotNull final BarrageMessage snap) {
        assertEquals(snap.rowsIncluded, i(1, 2));
        assertEquals(snap.addColumnData[1].data.get(0).asIntChunk().get(0), 5);
        assertEquals(snap.addColumnData[1].data.get(0).asIntChunk().get(1), QueryConstants.NULL_INT);
    }


    public void testUngroupConstructSnapshotSingleColumnTable() {
        final Table t =
                testRefreshingTable(i(0).toTracking())
                        .update("X = new Integer[]{null, 2, 3}");
        final Table ungrouped = t.ungroup();
        try (final BarrageMessage snap =
                ConstructSnapshot.constructBackplaneSnapshot(this, (BaseTable<?>) ungrouped)) {
            testUngroupConstructSnapshotSingleColumnHelper(snap);
        }

        final Table selected = ungrouped.select(); // Will convert column sources to in memory
        try (final BarrageMessage snap = ConstructSnapshot.constructBackplaneSnapshot(this, (BaseTable<?>) selected)) {
            testUngroupConstructSnapshotSingleColumnHelper(snap);
        }
    }

    private static void testUngroupConstructSnapshotSingleColumnHelper(@NotNull final BarrageMessage snap) {
        assertEquals(snap.rowsAdded, i(0, 1, 2));
        final List<Chunk<Values>> firstColChunk = snap.addColumnData[0].data;
        final int[] firstColExpected = new int[] {QueryConstants.NULL_INT, 2, 3};
        for (int i = 0; i < 3; i++) {
            assertEquals(firstColChunk.get(0).asIntChunk().get(i), firstColExpected[i]);
        }
    }

    public void testUngroupableColumnSources() {
        final Table table = testRefreshingTable(col("X", 1, 1, 2, 2, 3, 3, 4, 4), col("Int", 1, 2, 3, 4, 5, 6, 7, null),
                col("Double", 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, null, 0.45),
                col("String", "a", "b", "c", "d", "e", "f", "g", null));
        final Table t1 = table.groupBy("X");
        final Table t2 = t1.ungroup();

        assertEquals(table.size(), t2.size());

        assertTableEquals(t2, table);

        final Table t3 = t1.ungroup().sortDescending("X");
        assertTableEquals(t3, table.sortDescending("X"));

        final Table t4 = t1.update("Array=new int[]{19, 40}");
        TableTools.showWithRowSet(t4);
        final Table t5 = t4.ungroup();
        TableTools.showWithRowSet(t5);

        final Table t6 = table.update("Array=i%2==0?19:40");
        assertTableEquals(t5, t6);

        final Table t7 = t6.sortDescending("X");
        final Table t8 = t4.sortDescending("X").ungroup();
        assertTableEquals(t8, t7);

        final Table t9 = t1.update("Array=new io.deephaven.vector.IntVectorDirect(19, 40)");
        final Table t10 = t9.ungroup();
        assertTableEquals(t10, t6);

        final Table t11 = t9.sortDescending("X").ungroup();
        assertTableEquals(t11, t7);

        final int[] intDirect = (int[]) ColumnVectors.of(t2, "Int").toArray();
        System.out.println(Arrays.toString(intDirect));

        final int[] expected = new int[] {1, 2, 3, 4, 5, 6, 7, QueryConstants.NULL_INT};

        if (!Arrays.equals(expected, intDirect)) {
            System.out.println("Expected: " + Arrays.toString(expected));
            System.out.println("Direct: " + Arrays.toString(intDirect));
            fail("Expected does not match direct value!");
        }

        int[] intPrev = ColumnVectors.ofInt(t2, "Int", true).toArray();
        if (!Arrays.equals(expected, intPrev)) {
            System.out.println("Expected: " + Arrays.toString(expected));
            System.out.println("Prev: " + Arrays.toString(intPrev));
            fail("Expected does not match previous value!");
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
        });

        intPrev = ColumnVectors.ofInt(t2, "Int", true).toArray();
        if (!Arrays.equals(expected, intPrev)) {
            System.out.println("Expected: " + Arrays.toString(expected));
            System.out.println("Prev: " + Arrays.toString(intPrev));
            fail("Expected does not match previous value!");
        }
    }


    public void testUngroupOverflow() {
        try (final ErrorExpectation ignored = new ErrorExpectation()) {
            final QueryTable table = testRefreshingTable(i(5, 7).toTracking(), col("X", 1, 2),
                    col("Y", new String[] {"a", "b", "c"}, new String[] {"d", "e"}));
            final QueryTable t1 = (QueryTable) table.ungroup("Y");
            assertEquals(5, t1.size());
            assertEquals(Arrays.asList("X", "Y"), t1.getDefinition().getColumnNames());
            assertArrayEquals(new int[] {1, 1, 1, 2, 2}, ColumnVectors.ofInt(t1, "X").subVector(0, 5).toArray());
            assertArrayEquals(new String[] {"a", "b", "c", "d", "e"},
                    ColumnVectors.ofObject(t1, "Y", String.class).subVector(0, 5).toArray());

            final ErrorListener errorListener = new ErrorListener(t1);
            t1.addUpdateListener(errorListener);

            // This is too big, we should fail
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                final long bigIndex = 1L << 55;
                addToTable(table, i(bigIndex), intCol("X", 3),
                        new ColumnHolder<>("Y", String[].class, String.class, false, new String[] {"f"}));
                table.notifyListeners(i(bigIndex), i(), i());
            });
            TableTools.showWithRowSet(t1);

            if (errorListener.originalException() == null) {
                fail("errorListener.originalException == null");
            }
            if (!(errorListener.originalException() instanceof IllegalStateException)) {
                fail("!(errorListener.originalException instanceof IllegalStateException)");
            }
            if (!(errorListener.originalException().getMessage().startsWith("Key overflow detected"))) {
                fail("!errorListener.originalException.getMessage().startsWith(\"Key overflow detected\")");
            }
        }
    }


    public void testUngroupWithRebase() {
        final int minimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try {
            final QueryTable table = testRefreshingTable(i(5, 7).toTracking(),
                    col("X", 1, 2), col("Y", new String[] {"a", "b", "c"}, new String[] {"d", "e"}));
            TableTools.showWithRowSet(table);
            final QueryTable t1 = (QueryTable) table.ungroup("Y");
            TableTools.showWithRowSet(t1);
            assertEquals(5, t1.size());
            assertEquals(Arrays.asList("X", "Y"), t1.getDefinition().getColumnNames());
            assertArrayEquals(new int[] {1, 1, 1, 2, 2}, ColumnVectors.ofInt(t1, "X").toArray());
            assertArrayEquals(new String[] {"a", "b", "c", "d", "e"},
                    ColumnVectors.ofObject(t1, "Y", String.class).toArray());
            QueryTableTest.validateUpdates(t1);

            // This is too big, we should fail
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(table, i(9), col("X", 3), new ColumnHolder<>("Y", String[].class, String.class, false,
                        new String[] {"f", "g", "h", "i", "j", "k"}));
                table.notifyListeners(i(9), i(), i());
            });
            TableTools.showWithRowSet(t1);

            assertEquals(Arrays.asList("X", "Y"), t1.getDefinition().getColumnNames());
            assertArrayEquals(new int[] {1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3}, ColumnVectors.ofInt(t1, "X").toArray());
            assertArrayEquals(new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
                    ColumnVectors.ofObject(t1, "Y", String.class).toArray());

            assertArrayEquals(new int[] {1, 1, 1, 2, 2}, ColumnVectors.ofInt(t1, "X", true).toArray());
            assertArrayEquals(new String[] {"a", "b", "c", "d", "e"},
                    ColumnVectors.ofObject(t1, "Y", String.class, true).toArray());

            updateGraph.runWithinUnitTestCycle(() -> {
            });

            assertEquals(Arrays.asList("X", "Y"), t1.getDefinition().getColumnNames());
            assertArrayEquals(new int[] {1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3}, ColumnVectors.ofInt(t1, "X").toArray());
            assertArrayEquals(new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
                    ColumnVectors.ofObject(t1, "Y", String.class).toArray());

            assertArrayEquals(new int[] {1, 1, 1, 2, 2, 3, 3, 3, 3, 3, 3},
                    ColumnVectors.ofInt(t1, "X", true).toArray());
            assertArrayEquals(new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
                    ColumnVectors.ofObject(t1, "Y", String.class, true).toArray());
        } finally {
            QueryTable.setMinimumUngroupBase(minimumUngroupBase);
        }
    }

    public void testUngroupIncremental() throws ParseException {
        testUngroupIncremental(100, false, 0, 100);
        testUngroupIncremental(100, true, 0, 100);
    }

    @SuppressWarnings("SameParameterValue")
    private void testUngroupIncremental(final int tableSize, final boolean nullFill, final int seed, final int maxSteps)
            throws ParseException {
        final Random random = new Random(seed);
        QueryScope.addParam("f", new SimpleDateFormat("dd HH:mm:ss"));

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(tableSize, random,
                columnInfo = initColumnInfos(new String[] {"Date", "C1", "C2", "C3"},
                        new DateGenerator(format.parse("2011-02-02"), format.parse("2011-02-03")),
                        new SetGenerator<>("a", "b"),
                        new SetGenerator<>(10, 20, 30),
                        new SetGenerator<>(ArrayTypeUtils.EMPTY_STRING_ARRAY, new String[] {"a", "b"},
                                new String[] {"a", "b", "c"})));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                // EvalNugget.from(() -> table.groupBy().ungroup(nullFill)),
                // EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill)),
                // new UpdateValidatorNugget(table.groupBy("C1").ungroup(nullFill)),
                // EvalNugget.from(() -> table.groupBy().ungroup(nullFill, "C1")),
                // EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2")),
                // EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2").ungroup(nullFill,
                // "Date")),
                // EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2").ungroup(nullFill)),
                // EvalNugget.from(() -> table.groupBy("C1", "C2").sort("C1", "C2").ungroup(nullFill)),
                // EvalNugget
                // .from(() -> table.groupBy().update("Date=Date.toArray()", "C1=C1.toArray()", "C2=C2.toArray()")
                // .ungroup(nullFill)),
                // EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                // .ungroup(nullFill)),
                // EvalNugget
                // .from(() -> table.groupBy().update("Date=Date.toArray()", "C1=C1.toArray()", "C2=C2.toArray()")
                // .ungroup(nullFill, "C1")),
                // EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                // .ungroup(nullFill, "C2")),
                // EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                // .ungroup(nullFill, "C2").ungroup(nullFill, "Date")),
                // EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                // .ungroup(nullFill, "C2").ungroup(nullFill)),
                // EvalNugget.from(
                // () -> table.groupBy("C1", "C2").update("Date=Date.toArray()").sort("C1", "C2")
                // .ungroup(nullFill)),
                // EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                // .ungroup(nullFill)),
                EvalNugget.from(() -> table.view("C3").ungroup(nullFill))
        };

        final int stepSize = (int) Math.ceil(Math.sqrt(tableSize));
        for (int step = 0; step < maxSteps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed == " + seed + ", Step == " + step);
            }
            simulateShiftAwareStep(stepSize, random, table, columnInfo, en);
        }
    }

    public void testUngroupMismatch() {
        testUngroupMismatch(100, true);
        try {
            testUngroupMismatch(100, false);
            fail("Expected AssertionFailure");
        } catch (AssertionFailure ignored) {
        }
    }

    private void testUngroupMismatch(final int size, final boolean nullFill) {
        final Random random = new Random(0);
        final Boolean[] boolArray = {true, false};
        final Double[] doubleArray = {1.0, 2.0, 3.0};
        QueryScope.addParam("boolArray", boolArray);
        QueryScope.addParam("doubleArray", doubleArray);
        for (int q = 0; q < 10; q++) {
            final ColumnInfo<?, ?>[] columnInfo;
            final QueryTable table = getTable(size, random,
                    columnInfo = initColumnInfos(new String[] {"Sym", "intCol",},
                            new SetGenerator<>("a", "b", "c", "d"),
                            new IntGenerator(10, 100)));

            final Table mismatch =
                    table.groupBy("Sym").sort("Sym").update("MyBoolean=boolArray", "MyDouble=doubleArray");

            final EvalNugget[] en = new EvalNugget[] {
                    EvalNugget.from(() -> mismatch.ungroup(nullFill)),
            };

            for (int i = 0; i < 10; i++) {
                // show(mismatch, 100);
                simulateShiftAwareStep(size, random, table, columnInfo, en);
            }
        }
    }

    @SuppressWarnings({"RedundantCast", "unchecked"})
    public void testUngroupJoined_IDS6311() {
        final QueryTable left =
                testRefreshingTable(col("Letter", 'a', 'b', 'c', 'd'), intCol("Value", 0, 1, 2, 3));

        final QueryTable right = testRefreshingTable(col("Letter", '0', 'b'),
                byteCol("BValue", (byte) 0, (byte) 1),
                shortCol("SValue", (short) 0, (short) 1),
                intCol("EulavI", 0, 1),
                longCol("LValue", 0, 1),
                floatCol("FValue", 0.0f, 1.1f),
                doubleCol("DValue", 0.0d, 1.1d),
                charCol("CCol", 'a', 'b'),
                col("BoCol", true, false),
                col("OCol", (Pair<Integer, Integer>[]) new Pair[] {new Pair<>(0, 1), new Pair<>(2, 3)}));

        final Table leftBy = left.groupBy("Letter");
        final Table rightBy = right.groupBy("Letter");

        final Table joined = leftBy.naturalJoin(rightBy, "Letter")
                .withAttributes(Map.of(BaseTable.TEST_SOURCE_TABLE_ATTRIBUTE, true))
                .updateView("BValue = ((i%2) == 0) ? null : BValue");

        QueryTable expected = testRefreshingTable(col("Letter", 'a', 'b', 'c', 'd'),
                col("Value", (IntVector) new IntVectorDirect(0), (IntVector) new IntVectorDirect(1),
                        (IntVector) new IntVectorDirect(2), (IntVector) new IntVectorDirect(3)),
                col("BValue", null, (ByteVector) new ByteVectorDirect((byte) 1), null, null),
                col("SValue", null, (ShortVector) new ShortVectorDirect((short) 1), null, null),
                col("EulavI", null, (IntVector) new IntVectorDirect(1), null, null),
                col("LValue", null, (LongVector) new LongVectorDirect(1), null, null),
                col("FValue", null, (FloatVector) new FloatVectorDirect(1.1f), null, null),
                col("DValue", null, (DoubleVector) new DoubleVectorDirect(1.1d), null, null),
                col("CCol", null, (CharVector) new CharVectorDirect('b'), null, null),
                col("BoCol", null, (ObjectVector<Boolean>) new ObjectVectorDirect<>(false), null, null),
                col("OCol", null, (ObjectVector<Pair<Integer, Integer>>) new ObjectVectorDirect<>(new Pair<>(2, 3)),
                        null,
                        null));

        assertTableEquals(expected, joined);

        final Table ungrouped = joined.ungroup(true);

        expected = testRefreshingTable(col("Letter", 'a', 'b', 'c', 'd'),
                col("Value", 0, 1, 2, 3),
                byteCol("BValue", QueryConstants.NULL_BYTE, (byte) 1,
                        QueryConstants.NULL_BYTE, QueryConstants.NULL_BYTE),
                shortCol("SValue", QueryConstants.NULL_SHORT, (short) 1,
                        QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT),
                intCol("EulavI", QueryConstants.NULL_INT, 1,
                        QueryConstants.NULL_INT, QueryConstants.NULL_INT),
                longCol("LValue", QueryConstants.NULL_LONG, (long) 1,
                        QueryConstants.NULL_LONG, QueryConstants.NULL_LONG),
                floatCol("FValue", QueryConstants.NULL_FLOAT, 1.1f,
                        QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT),
                doubleCol("DValue", QueryConstants.NULL_DOUBLE, 1.1d,
                        QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                charCol("CCol", QueryConstants.NULL_CHAR, 'b',
                        QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR),
                col("BoCol", null, false, null, null),
                col("OCol", (Pair<Integer, Integer>[]) new Pair[] {null, new Pair<>(2, 3), null, null}));

        assertTableEquals(expected, ungrouped);

        // assertTableEquals only calls get(), we need to make sure the specialized get()s also work too.
        final long firstKey = ungrouped.getRowSet().firstRowKey();
        final long secondKey = ungrouped.getRowSet().get(1);

        assertEquals(QueryConstants.NULL_BYTE, ungrouped.getColumnSource("BValue").getByte(firstKey));
        assertEquals((byte) 1, ungrouped.getColumnSource("BValue").getByte(secondKey));

        assertEquals(QueryConstants.NULL_SHORT,
                ungrouped.getColumnSource("SValue").getShort(firstKey));
        assertEquals((short) 1, ungrouped.getColumnSource("SValue").getShort(secondKey));

        assertEquals(QueryConstants.NULL_INT, ungrouped.getColumnSource("EulavI").getInt(firstKey));
        assertEquals(1, ungrouped.getColumnSource("EulavI").getInt(secondKey));

        assertEquals(QueryConstants.NULL_LONG, ungrouped.getColumnSource("LValue").getLong(firstKey));
        assertEquals(1, ungrouped.getColumnSource("LValue").getLong(secondKey));

        assertEquals(QueryConstants.NULL_FLOAT,
                ungrouped.getColumnSource("FValue").getFloat(firstKey));
        assertEquals(1.1f, ungrouped.getColumnSource("FValue").getFloat(secondKey));

        assertEquals(QueryConstants.NULL_DOUBLE,
                ungrouped.getColumnSource("DValue").getDouble(firstKey));
        assertEquals(1.1d, ungrouped.getColumnSource("DValue").getDouble(secondKey));

        assertEquals(QueryConstants.NULL_CHAR, ungrouped.getColumnSource("CCol").getChar(firstKey));
        assertEquals('b', ungrouped.getColumnSource("CCol").getChar(secondKey));

        // repeat with prev
        assertEquals(QueryConstants.NULL_BYTE,
                ungrouped.getColumnSource("BValue").getPrevByte(firstKey));
        assertEquals((byte) 1, ungrouped.getColumnSource("BValue").getPrevByte(secondKey));

        assertEquals(QueryConstants.NULL_SHORT,
                ungrouped.getColumnSource("SValue").getPrevShort(firstKey));
        assertEquals((short) 1, ungrouped.getColumnSource("SValue").getPrevShort(secondKey));

        assertEquals(QueryConstants.NULL_INT,
                ungrouped.getColumnSource("EulavI").getPrevInt(firstKey));
        assertEquals(1, ungrouped.getColumnSource("EulavI").getPrevInt(secondKey));

        assertEquals(QueryConstants.NULL_LONG,
                ungrouped.getColumnSource("LValue").getPrevLong(firstKey));
        assertEquals(1, ungrouped.getColumnSource("LValue").getPrevLong(secondKey));

        assertEquals(QueryConstants.NULL_FLOAT,
                ungrouped.getColumnSource("FValue").getPrevFloat(firstKey));
        assertEquals(1.1f, ungrouped.getColumnSource("FValue").getPrevFloat(secondKey));

        assertEquals(QueryConstants.NULL_DOUBLE,
                ungrouped.getColumnSource("DValue").getPrevDouble(firstKey));
        assertEquals(1.1d, ungrouped.getColumnSource("DValue").getPrevDouble(secondKey));

        assertEquals(QueryConstants.NULL_CHAR, ungrouped.getColumnSource("CCol").getPrevChar(firstKey));
        assertEquals('b', ungrouped.getColumnSource("CCol").getPrevChar(secondKey));

        assertNull(ungrouped.getColumnSource("CCol").getPrev(firstKey));
        assertEquals('b', ungrouped.getColumnSource("CCol").getPrev(secondKey));

        try (final BarrageMessage snap =
                ConstructSnapshot.constructBackplaneSnapshot(this, (BaseTable<?>) ungrouped)) {
            verifySnapshotBarrageMessage(snap, expected);
        }
    }
}
