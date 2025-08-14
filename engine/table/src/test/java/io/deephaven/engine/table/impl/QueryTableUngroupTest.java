//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.table.vectors.IntVectorColumnWrapper;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.sources.ObjectTestSource;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.TickSuppressor;
import io.deephaven.qst.type.Type;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
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
import static io.deephaven.util.QueryConstants.NULL_INT;
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

        final Table ungrouped = source.ungroup();
        assertTableEquals(expected, ungrouped);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(source, i(2));
            TstUtils.addToTable(source, i(10), intCol("Key", 3), col("Value", new int[] {301, 302, 303}));
            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(2, 2, 8);
            final RowSetShiftData sd = sb.build();
            source.notifyListeners(new TableUpdateImpl(i(), i(), i(), sd, ModifiedColumnSet.EMPTY));
        });

        assertTableEquals(expected, ungrouped);

        // this one is silly, we are going to shift an empty table; which was to cover a since deleted branch
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet rowsToRemove = source.getRowSet().copy();
            TstUtils.removeRows(source, rowsToRemove);
            final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
            sb.shiftRange(100, 200, 100);
            final RowSetShiftData sd = sb.build();
            source.notifyListeners(new TableUpdateImpl(i(), rowsToRemove, i(), sd, ModifiedColumnSet.EMPTY));
        });
        TableTools.show(source);
        TableTools.show(ungrouped);
        assertTableEquals(expected.head(0), ungrouped);
    }

    public void testContiguousShift() {
        final int oldBase = QueryTable.minimumUngroupBase;
        try (final SafeCloseable ignored = () -> QueryTable.minimumUngroupBase = oldBase) {
            QueryTable.minimumUngroupBase = 2;

            final int[] a1 = {101, 102, 103, 104};
            final int[] a2 = {201, 202, 203, 204};
            final int[] a3 = {301, 302, 303, 304};
            final QueryTable source = testRefreshingTable(
                    intCol("Key", 1, 2, 3),
                    col("Value", a1, a2, a3));

            final Table expected =
                    TableTools.newTable(intCol("Key", 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3),
                            intCol("Value", 101, 102, 103, 104, 201, 202, 203, 204, 301, 302, 303, 304));

            final Table ungrouped = source.ungroup();
            TableTools.showWithRowSet(ungrouped, 14);
            assertEquals(4 * 3 - 1, ungrouped.getRowSet().lastRowKey());
            assertTableEquals(expected, ungrouped);

            // we are shifting 1 and 2, which should take up the entire table
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(source, i(1, 2));
                TstUtils.addToTable(source, i(11, 12), intCol("Key", 2, 3), col("Value", a2, a3));

                final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
                sb.shiftRange(1, 9, 10);
                final RowSetShiftData sd = sb.build();
                source.notifyListeners(new TableUpdateImpl(i(), i(), i(), sd, ModifiedColumnSet.EMPTY));
            });

            // and just for fun, shift more than our actual existing range with extra keys in front
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(source, i(11, 12));
                TstUtils.addToTable(source, i(21, 22), intCol("Key", 2, 3), col("Value", a2, a3));

                final RowSetShiftData.Builder sb = new RowSetShiftData.Builder();
                sb.shiftRange(1, 19, 10);
                final RowSetShiftData sd = sb.build();
                source.notifyListeners(new TableUpdateImpl(i(), i(), i(), sd, ModifiedColumnSet.EMPTY));
            });

            assertTableEquals(expected, ungrouped);
        }
    }

    public void testContiguousRemoves() {
        final int oldBase = QueryTable.minimumUngroupBase;
        try (final SafeCloseable ignored = () -> QueryTable.minimumUngroupBase = oldBase) {
            QueryTable.minimumUngroupBase = 2;

            final int[] a1 = {101, 102, 103, 104};
            final int[] a2 = {201, 202, 203, 204};
            final int[] a3 = {301, 302, 303, 304};
            final QueryTable source = testRefreshingTable(
                    intCol("Key", 1, 2, 3),
                    col("Value", a1, a2, a3));

            final Table expected =
                    TableTools.newTable(intCol("Key", 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3),
                            intCol("Value", 101, 102, 103, 104, 201, 202, 203, 204, 301, 302, 303, 304));

            final Table ungrouped = source.ungroup();
            TableTools.showWithRowSet(ungrouped, 14);
            assertEquals(4 * 3 - 1, ungrouped.getRowSet().lastRowKey());
            assertTableEquals(expected, ungrouped);

            // we are shifting 1 and 2, which should take up the entire table
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.removeRows(source, i(1));

                source.notifyListeners(
                        new TableUpdateImpl(i(), i(1), i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });

            final Table expected2 = TableTools.newTable(intCol("Key", 1, 1, 1, 1, 3, 3, 3, 3),
                    intCol("Value", 101, 102, 103, 104, 301, 302, 303, 304));
            assertTableEquals(expected2, ungrouped);
        }
    }

    public void testStatic() {
        final Table source = newTable(
                intCol("Key", 1, 2, 3),
                col("Value", new int[] {101}, new int[] {201, 202}, new int[] {301, 302, 303}));

        final Table expected =
                TableTools.newTable(intCol("Key", 1, 2, 2, 3, 3, 3), intCol("Value", 101, 201, 202, 301, 302, 303));

        final Table ungrouped = source.ungroup();
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

    public void testNullFillWithNull() {
        final String[][] dataWithNullStringArray = new String[][] {null};
        final Boolean[][] dataWithNullBooleanArray = new Boolean[][] {null};
        final QueryTable qt = testRefreshingTable(
                col("C1", new int[] {1, 2, 3}),
                col("C2", new int[0]),
                col("C3", new double[] {1.01, 2.02, 3.03, 4.04}),
                col("C4", dataWithNullStringArray),
                col("C5", dataWithNullBooleanArray));

        final Table ungrouped = qt.ungroup(true);

        final Table expected = newTable(
                intCol("C1", 1, 2, 3, NULL_INT),
                intCol("C2", NULL_INT, NULL_INT, NULL_INT, NULL_INT),
                doubleCol("C3", 1.01, 2.02, 3.03, 4.04),
                stringCol("C4", null, null, null, null),
                booleanCol("C5", null, null, null, null));
        assertTableEquals(expected, ungrouped);

        final QueryTable qt2 = testRefreshingTable(
                col("C1", new int[] {1}, new int[] {4}),
                col("C2", new ObjectVectorDirect<>("A"), null),
                col("C3", new ObjectVectorDirect<>(Boolean.TRUE), null));

        final Table ungrouped2 = qt2.ungroup(true);
        final Table expected2 = newTable(
                intCol("C1", 1, 4),
                stringCol("C2", "A", null),
                booleanCol("C3", true, null));

        assertTableEquals(expected2.update("C2=(Object)C2", "C3=(Object)C3"), ungrouped2);
    }

    public void testModifyToNull() {
        final QueryTable qt = testRefreshingTable(
                col("C1", new int[] {1, 2, 3}, new int[0], null, new int[] {7}),
                col("C2", new IntVectorDirect(4, 5, 6), new IntVectorDirect(), null, new IntVectorDirect(8)));

        final Table ug = qt.ungroup(false);

        final QueryTable expected1 = testTable(
                col("C1", 1, 2, 3, 7),
                col("C2", 4, 5, 6, 8));
        assertTableEquals(expected1, ug);

        setExpectError(false);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet mods = i(0, 2);
            addToTable(qt, mods,
                    col("C1", null, new int[] {10, 11, 12}),
                    col("C2", null, new IntVectorDirect(20, 21, 22)));
            qt.notifyListeners(i(), i(), mods);
        });

        final QueryTable expected2 = testTable(
                col("C1", 10, 11, 12, 7),
                col("C2", 20, 21, 22, 8));
        assertTableEquals(expected2, ug);
    }

    public void testModifyToNullFill() {
        final QueryTable qt = testRefreshingTable(
                col("C1", new int[] {1, 2, 3}, new int[0], null, new int[] {7}),
                col("C2", new IntVectorDirect(4, 5), new IntVectorDirect(), null, new IntVectorDirect(8, 9)));

        final Table ug = qt.ungroup(true);

        final QueryTable expected1 = testTable(
                col("C1", 1, 2, 3, 7, null),
                col("C2", 4, 5, null, 8, 9));
        assertTableEquals(expected1, ug);

        setExpectError(false);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet mods = i(0, 2, 3);
            addToTable(qt, mods,
                    col("C1", null, new int[] {10}, new int[] {30, 31, 33}),
                    col("C2", null, new IntVectorDirect(20, 22), new IntVectorDirect(40, 41)));
            qt.notifyListeners(i(), i(), mods);
        });

        final QueryTable expected2 = testTable(
                col("C1", 10, null, 30, 31, 33),
                col("C2", 20, 22, 40, 41, null));
        assertTableEquals(expected2, ug);
    }

    public void testNoColumns() {
        final Table table = newTable(col("X", 1, 2, 3));
        final Table result = table.ungroup();
        assertSame(table, result);

        final Table table2 = newTable(col("X", 1, 2, 3), col("Y", new int[] {1}, null, null));
        final InvalidColumnException columnException =
                Assert.assertThrows(InvalidColumnException.class, () -> table2.ungroup("X"));
        assertEquals("Column X is not an array or Vector", columnException.getMessage());
    }

    public void testSimple() {
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

        final String expectedMessage = "Array sizes differ at row key 1 (position 1), Y has size 2, Z has size 0";
        assertEquals(expectedMessage, Assert.assertThrows(IllegalStateException.class, table2::ungroup).getMessage());
        assertEquals(expectedMessage,
                Assert.assertThrows(IllegalStateException.class, () -> table2.ungroup("Y", "Z")).getMessage());
        assertEquals(expectedMessage, Assert.assertThrows(IllegalStateException.class,
                () -> table2.update("Z=new io.deephaven.vector.IntVectorDirect(Z)").ungroup("Y", "Z")).getMessage());
        assertEquals(expectedMessage, Assert.assertThrows(IllegalStateException.class,
                () -> convertToUngroupable(table2, "Z").ungroup("Y", "Z")).getMessage());

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

    public void testSomeSizesChanged() {
        final int[][] dataY1 = new int[][] {new int[] {4, 5, 6}, new int[0], new int[] {7, 8}};
        final double[][] dataZ1 =
                new double[][] {new double[] {10.1, 10.2, 10.3}, new double[0], new double[] {11.1, 11.2}};
        final QueryTable table = testRefreshingTable(col("X", 1, 2, 3),
                col("Y", dataY1),
                col("Z", dataZ1));

        final Table withUngroupable = convertToUngroupable(table, "Y", "Z");

        final Table t1 = table.ungroup();
        final Table t2 = withUngroupable.ungroup();
        final TableUpdateValidator tuv = TableUpdateValidator.make("t1", (QueryTable) t1);
        final TableUpdateValidator tuv2 = TableUpdateValidator.make("t2", (QueryTable) t2);

        final Table expected = TableTools.newTable(intCol("X", 1, 1, 1, 3, 3), intCol("Y", 4, 5, 6, 7, 8),
                doubleCol("Z", 10.1, 10.2, 10.3, 11.1, 11.2));
        assertTableEquals(expected, t1);
        assertTableEquals(expected, t2);

        // change just "Z", preserving the size
        final int[][] dataY2 = new int[][] {dataY1[0]};
        final double[][] dataZ2 = new double[][] {new double[] {20.1, 20.2, 20.3}};
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(0), intCol("X", 1), col("Y", dataY2), col("Z", dataZ2));

            final ModifiedColumnSet mcs = table.getModifiedColumnSetForUpdates();
            mcs.clear();
            mcs.setAll("Z");
            table.notifyListeners(new TableUpdateImpl(i(), i(), i(0), RowSetShiftData.EMPTY, mcs));
        });
        final Table expected2 = TableTools.newTable(intCol("X", 1, 1, 1, 3, 3), intCol("Y", 4, 5, 6, 7, 8),
                doubleCol("Z", 20.1, 20.2, 20.3, 11.1, 11.2));
        assertTableEquals(expected2, t1);
        assertTableEquals(expected2, t2);
        assertFalse(tuv.getResultTable().isFailed());
        assertFalse(tuv2.getResultTable().isFailed());

        // now do the same for "Y", leaving "Z" alone, so that we have a different reference column (but not breaking it
        // yet)
        final int[][] dataY3 = new int[][] {new int[] {9, 10}};
        final double[][] dataZ3 = new double[][] {dataZ1[2]};
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(2), intCol("X", 4), col("Y", dataY3), col("Z", dataZ3));

            final ModifiedColumnSet mcs = table.getModifiedColumnSetForUpdates();
            mcs.clear();
            mcs.setAll("X", "Y");
            table.notifyListeners(new TableUpdateImpl(i(), i(), i(2), RowSetShiftData.EMPTY, mcs));
        });
        final Table expected3 = TableTools.newTable(intCol("X", 1, 1, 1, 4, 4), intCol("Y", 4, 5, 6, 9, 10),
                doubleCol("Z", 20.1, 20.2, 20.3, 11.1, 11.2));
        assertTableEquals(expected3, t1);
        assertTableEquals(expected3, t2);
        assertFalse(tuv.getResultTable().isFailed());
        assertFalse(tuv2.getResultTable().isFailed());

        // now we'll break the "Y" column, using "Z" as a reference
        final int[][] dataY4 = new int[][] {new int[] {12}};
        final double[][] dataZ4 = new double[][] {dataZ1[1]};
        try (final ExpectingError ignored = new ExpectingError()) {
            final ErrorListener errorListener1 = new ErrorListener(t1);
            t1.addUpdateListener(errorListener1);
            final ErrorListener errorListener2 = new ErrorListener(t2);
            t2.addUpdateListener(errorListener2);
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(table, i(1), intCol("X", 5), col("Y", dataY4), col("Z", dataZ4));

                final ModifiedColumnSet mcs = table.getModifiedColumnSetForUpdates();
                mcs.clear();
                mcs.setAll("X", "Y");
                table.notifyListeners(new TableUpdateImpl(i(), i(), i(1), RowSetShiftData.EMPTY, mcs));
            });
            assertTrue(tuv.getResultTable().isFailed());
            assertTrue(tuv2.getResultTable().isFailed());
            final Throwable ex1 = errorListener1.originalException();
            final Throwable ex2 = errorListener2.originalException();

            assertTrue(ex1 instanceof IllegalStateException);
            assertTrue(ex2 instanceof IllegalStateException);
            assertEquals("Array sizes differ at row key 1 (position 1), Z has size 0, Y has size 1", ex1.getMessage());
            assertEquals("Array sizes differ at row key 1 (position 1), Z has size 0, Y has size 1", ex2.getMessage());
        }

        // now fix things up so we can actually get good data out again
        final double[][] dataZ5 = new double[][] {new double[] {30.1}};
        addToTable(table, i(1), intCol("X", 5), col("Y", dataY4), col("Z", dataZ5));
        final Table t3 = table.ungroup();
        final Table t4 = withUngroupable.ungroup();
        final TableUpdateValidator tuv3 = TableUpdateValidator.make("t4", (QueryTable) t3);
        final TableUpdateValidator tuv4 = TableUpdateValidator.make("t4", (QueryTable) t4);

        final Table expected5 = TableTools.newTable(intCol("X", 1, 1, 1, 5, 4, 4), intCol("Y", 4, 5, 6, 12, 9, 10),
                doubleCol("Z", 20.1, 20.2, 20.3, 30.1, 11.1, 11.2));
        TableTools.show(table);
        TableTools.show(t3);
        assertTableEquals(expected5, t3);
        assertTableEquals(expected5, t4);
        assertFalse(tuv3.getResultTable().isFailed());
        assertFalse(tuv4.getResultTable().isFailed());


        // now we'll break the "Z" column, using "Y" as an (unmodified) reference
        final double[][] dataZ6 = new double[][] {dataZ1[1]};
        try (final ExpectingError ignored2 = new ExpectingError()) {
            final ErrorListener errorListener3 = new ErrorListener(t3);
            t3.addUpdateListener(errorListener3);
            final ErrorListener errorListener4 = new ErrorListener(t4);
            t4.addUpdateListener(errorListener4);
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(table, i(1), intCol("X", 7), col("Y", dataY4), col("Z", dataZ6));

                final ModifiedColumnSet mcs = table.getModifiedColumnSetForUpdates();
                mcs.clear();
                mcs.setAll("X", "Z");
                table.notifyListeners(new TableUpdateImpl(i(), i(), i(1), RowSetShiftData.EMPTY, mcs));
            });
            assertTrue(tuv3.getResultTable().isFailed());
            assertTrue(tuv4.getResultTable().isFailed());
            final Throwable ex3 = errorListener3.originalException();
            final Throwable ex4 = errorListener4.originalException();
            assertTrue(ex3 instanceof IllegalStateException);
            assertTrue(ex4 instanceof IllegalStateException);
            assertEquals("Array sizes differ at row key 1 (position 1), Y has size 1, Z has size 0", ex3.getMessage());
            assertEquals("Array sizes differ at row key 1 (position 1), Y has size 1, Z has size 0", ex4.getMessage());
        }
    }

    public static Table convertToUngroupable(final Table table, final String... columns) {
        final Map<String, ColumnSource<?>> columnSources = new LinkedHashMap<>();
        for (final String column : columns) {
            columnSources.put(column, new SimulateUngroupableColumnSource<>(table.getColumnSource(column)));
        }
        final QueryTable result = ((QueryTable) table).withAdditionalColumns(columnSources);
        final ModifiedColumnSet.Transformer transformer =
                ((QueryTable) table).newModifiedColumnSetIdentityTransformer(result);
        table.addUpdateListener(new BaseTable.ListenerImpl("Add Simulated Ungroupable", table, result) {
            @Override
            public void onUpdate(final TableUpdate upstream) {
                transformer.clearAndTransform(upstream.modifiedColumnSet(), result.getModifiedColumnSetForUpdates());
                final TableUpdate downstream = TableUpdateImpl.copy(upstream, result.getModifiedColumnSetForUpdates());
                result.notifyListeners(downstream);
            }
        });
        return result;
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
        final int[] firstColExpected = new int[] {NULL_INT, 2, 3};
        final List<Chunk<Values>> secondColChunk = snap.addColumnData[1].data;
        final int[] secondColExpected = new int[] {4, 5, NULL_INT};
        for (int i = 0; i < 3; i++) {
            assertEquals(firstColChunk.get(0).asIntChunk().get(i), firstColExpected[i]);
            assertEquals(secondColChunk.get(0).asIntChunk().get(i), secondColExpected[i]);
        }
    }

    private static void testUngroupConstructSnapshotBoxedNullFewColumnsHelper(@NotNull final BarrageMessage snap) {
        assertEquals(i(1, 2), snap.rowsIncluded);
        assertEquals(5, snap.addColumnData[1].data.get(0).asIntChunk().get(0));
        assertEquals(NULL_INT, snap.addColumnData[1].data.get(0).asIntChunk().get(1));
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
        final int[] firstColExpected = new int[] {NULL_INT, 2, 3};
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

        final int[] expected = new int[] {1, 2, 3, 4, 5, 6, 7, NULL_INT};

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

    public void testUngroupMaxBase() {
        final QueryTable table = testTable(RowSetFactory.flat(1L << 62).toTracking());
        final Table withSingle = table.update("X=1");

        final Table withSingle2 = table.update("X=2");
        TableTools.showWithRowSet(withSingle);

        final IntVectorColumnWrapper cw =
                new IntVectorColumnWrapper(withSingle.getColumnSource("X"), withSingle.getRowSet(), 0, 0);
        final IntVectorColumnWrapper cw2 =
                new IntVectorColumnWrapper(withSingle2.getColumnSource("X"), withSingle.getRowSet(), 0, 0);
        final ObjectArraySource<IntVector> oas = new ObjectArraySource<>(IntVector.class);
        oas.ensureCapacity(2);
        oas.set(0, cw);
        oas.set(1, cw2);
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put("X", oas);
        final QueryTable fakeGrouped = new QueryTable(i(0).toTracking(), columns);
        TableTools.showWithRowSet(fakeGrouped);

        final QueryTable ungrouped = (QueryTable) fakeGrouped.ungroup();
        TableTools.showWithRowSet(ungrouped);
        assertEquals(1L << 62, ungrouped.size());

        // we are not going to actually check the entire table, which will take until the heat death of the universe.
        // We're just going to pick a few values
        for (int ii = 0; ii <= 61; ++ii) {
            assertEquals(1, ungrouped.getColumnSource("X").get(1L << ii));
        }
        assertEquals(1, ungrouped.getColumnSource("X").get((1L << 62) - 1L));

        final QueryTable fakeGrouped2 = new QueryTable(i(0, 1).toTracking(), columns);
        TableTools.showWithRowSet(fakeGrouped2);
        final IllegalStateException ise =
                Assert.assertThrows(IllegalStateException.class, () -> fakeGrouped2.ungroup());
        assertEquals(
                "Key overflow detected, perhaps you should flatten your table before calling ungroup: lastRowKey=1, base=62",
                ise.getMessage());
    }

    public void testBaseTooBig() {
        final QueryTable table = testTable(RowSetFactory.flat((1L << 63) - 1).toTracking());
        // noinspection SizeReplaceableByIsEmpty
        assertTrue(table.size() > 0);
        final Table withSingle = table.update("X=1");

        final IntVectorColumnWrapper cw =
                new IntVectorColumnWrapper(withSingle.getColumnSource("X"), withSingle.getRowSet(), 0, 0);
        final ObjectArraySource<IntVector> oas = new ObjectArraySource<>(IntVector.class);
        oas.ensureCapacity(1);
        oas.set(0, cw);
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put("X", oas);
        final QueryTable fakeGrouped = new QueryTable(i(0).toTracking(), columns);
        TableTools.showWithRowSet(fakeGrouped);

        final AssertionFailure af = Assert.assertThrows(AssertionFailure.class, fakeGrouped::ungroup);
        assertEquals("Assertion failed: asserted newNumShiftBits < 63, instead newNumShiftBits == 63, 63 == 63.",
                af.getMessage());
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

    public void testRebaseWithShift() {
        final int minimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try {
            final QueryTable table = testRefreshingTable(i(15, 17).toTracking(),
                    intCol("X", 1, 2), col("Y", new String[] {"a", "b", "c"}, new String[] {"d", "e"}));
            TableTools.showWithRowSet(table);
            final QueryTable t1 = (QueryTable) table.ungroup("Y");
            TableTools.showWithRowSet(t1);
            assertEquals(5, t1.size());

            final Table expected =
                    TableTools.newTable(intCol("X", 1, 1, 1, 2, 2), stringCol("Y", "a", "b", "c", "d", "e"));
            assertTableEquals(expected, t1);
            QueryTableTest.validateUpdates(t1);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                removeRows(table, i(15, 17));
                addToTable(table, i(5, 7), intCol("X", 1, 2),
                        col("Y", new String[] {"alpha", "bravo", "charlie", "delta", "echo"}, new String[] {"d", "e"}));

                final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
                shiftBuilder.shiftRange(10, 20, -10);

                final RowSetShiftData sd = shiftBuilder.build();
                table.notifyListeners(new TableUpdateImpl(i(), i(), i(5), sd, ModifiedColumnSet.ALL));
            });
            TableTools.showWithRowSet(t1);
            final Table expected2 = TableTools.newTable(intCol("X", 1, 1, 1, 1, 1, 2, 2),
                    stringCol("Y", "alpha", "bravo", "charlie", "delta", "echo", "d", "e"));
            assertTableEquals(expected2, t1);
        } finally {
            QueryTable.setMinimumUngroupBase(minimumUngroupBase);
        }
    }

    public void testRebaseWithShift2() {
        final int minimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try {
            final QueryTable table = testRefreshingTable(i(15, 25).toTracking(),
                    intCol("X", 1, 2), col("Y", new String[] {"a", "b", "c"}, new String[] {"d", "e"}));
            TableTools.showWithRowSet(table);
            final QueryTable t1 = (QueryTable) table.ungroup("Y");
            TableTools.showWithRowSet(t1);
            assertEquals(5, t1.size());

            final Table expected =
                    TableTools.newTable(intCol("X", 1, 1, 1, 2, 2), stringCol("Y", "a", "b", "c", "d", "e"));
            assertTableEquals(expected, t1);
            QueryTableTest.validateUpdates(t1);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                removeRows(table, i(15, 25));
                addToTable(table, i(5, 35), intCol("X", 1, 2),
                        col("Y", new String[] {"alpha", "bravo", "charlie", "delta", "echo"}, new String[] {"d", "e"}));

                final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
                shiftBuilder.shiftRange(10, 19, -10);
                shiftBuilder.shiftRange(20, 30, 10);

                final RowSetShiftData sd = shiftBuilder.build();
                table.notifyListeners(new TableUpdateImpl(i(), i(), i(5), sd, ModifiedColumnSet.ALL));
            });
            TableTools.showWithRowSet(t1);
            final Table expected2 = TableTools.newTable(intCol("X", 1, 1, 1, 1, 1, 2, 2),
                    stringCol("Y", "alpha", "bravo", "charlie", "delta", "echo", "d", "e"));
            assertTableEquals(expected2, t1);
        } finally {
            QueryTable.setMinimumUngroupBase(minimumUngroupBase);
        }
    }


    public void testUngroupIncrementalRebase() {
        final int minimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try (final SafeCloseable ignored2 = () -> QueryTable.setMinimumUngroupBase(minimumUngroupBase)) {
            for (int seed = 0; seed < 100; ++seed) {
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testUngroupIncrementalRebase(10, false, seed, 5);
                }
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testUngroupIncrementalRebase(10, true, seed, 5);
                }
            }
        }
    }

    public static int[] allocateInt(final int sentinel, final int size) {
        final int[] result = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            result[ii] = sentinel * 1000 + ii;
        }
        return result;
    }

    public static double[] allocateDouble(final int sentinel, final int size) {
        final double[] result = new double[size];
        for (int ii = 0; ii < size; ++ii) {
            result[ii] = ii + ((double) sentinel / 100.0);
        }
        return result;
    }

    @SuppressWarnings("SameParameterValue")
    private void testUngroupIncrementalRebase(final int tableSize,
            final boolean nullFill,
            final int seed,
            final int maxSteps) {
        final Random random = new Random(seed);
        QueryScope.addParam("f", new SimpleDateFormat("dd HH:mm:ss"));

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(tableSize, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sentinel", "MiddleArray", "BigArray", "SmallSize", "MiddleSize", "BigSize"},
                        new IntGenerator(0, 100),
                        new BooleanGenerator(0.05),
                        new BooleanGenerator(0.01),
                        new IntGenerator(0, 3),
                        new IntGenerator(4, 7),
                        new IntGenerator(8, 32)));

        final Table withArrays = table.update("Sz=BigArray ? BigSize : MiddleArray ? MiddleSize: SmallSize",
                "A1=io.deephaven.engine.table.impl.QueryTableUngroupTest.allocateInt(Sentinel, Sz)",
                "A2=io.deephaven.engine.table.impl.QueryTableUngroupTest.allocateDouble(Sentinel, Sz)");

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> withArrays.ungroup(nullFill)),
                // new UpdateValidatorNugget(withArrays.ungroup(nullFill))
        };

        final int stepSize = (int) Math.ceil(Math.sqrt(tableSize));
        for (int step = 0; step < maxSteps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed == " + seed + ", Step == " + step);
            }
            simulateShiftAwareStep(stepSize, random, table, columnInfo, en);
        }
    }

    public void testUngroupIncrementalLarge() throws ParseException {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            testUngroupIncrementalLarge(3000, false, 0, 5);
        }
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            testUngroupIncrementalLarge(3000, true, 0, 5);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void testUngroupIncrementalLarge(final int tableSize, final boolean nullFill, final int seed,
            final int maxSteps) {
        final Random random = new Random(seed);
        QueryScope.addParam("f", new SimpleDateFormat("dd HH:mm:ss"));

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(tableSize, random,
                columnInfo = initColumnInfos(new String[] {"KeyCol", "C1", "C2"},
                        new SetGenerator<>("a", "b"),
                        new SetGenerator<>(ArrayTypeUtils.EMPTY_STRING_ARRAY, new String[] {"a", "b"},
                                new String[] {"a", "b", "c"}, null),
                        new SetGenerator<>(ArrayTypeUtils.EMPTY_DOUBLE_ARRAY, new double[] {1.0, 2.0, 3.0},
                                new double[] {Math.PI, Math.pow(2, 100)})));

        TableTools.showWithRowSet(table.view("KeyCol", "C1"));
        TableTools.showWithRowSet(table.view("KeyCol", "C1").ungroup(false));

        final List<EvalNuggetInterface> nuggets = new ArrayList<>(Arrays.asList(
                EvalNugget.from(() -> table.groupBy().ungroup(nullFill)),
                new UpdateValidatorNugget(table.groupBy().ungroup(nullFill)),

                EvalNugget.from(() -> table.view("KeyCol", "C1").ungroup(nullFill)),
                EvalNugget.from(() -> table.view("KeyCol", "C2").ungroup(nullFill)),
                EvalNugget.from(() -> table
                        .select("KeyCol", "C1=C1 == null ? null : new io.deephaven.vector.ObjectVectorDirect(C1)")
                        .ungroup(nullFill)),
                EvalNugget.from(() -> table
                        .select("KeyCol", "C2=C2 == null ? null : new io.deephaven.vector.DoubleVectorDirect(C2)")
                        .ungroup(nullFill)),
                EvalNugget.from(() -> convertToUngroupable(table.view("KeyCol", "C1").ungroup(nullFill)))));
        if (nullFill) {
            nuggets.addAll(Arrays.asList(
                    EvalNugget.from(() -> table.ungroup(nullFill)),
                    EvalNugget.from(() -> table
                            .select("KeyCol", "C1=C1 == null ? null : new io.deephaven.vector.ObjectVectorDirect(C1)",
                                    "C2=C2 == null ? null : new io.deephaven.vector.DoubleVectorDirect(C2)")
                            .ungroup(nullFill)),
                    EvalNugget.from(() -> convertToUngroupable(table.view("KeyCol", "C1", "C2").ungroup(nullFill)))));
        } else {
            nuggets.add(EvalNugget.from(() -> table.view("KeyCol", "C2",
                    "C3=C2 == null ? null : io.deephaven.engine.table.impl.QueryTableUngroupTest.toString(new io.deephaven.vector.DoubleVectorDirect(C2))")
                    .ungroup(nullFill)));
        }
        final EvalNuggetInterface[] en = nuggets.toArray(new EvalNuggetInterface[0]);

        final int stepSize = (int) Math.ceil(Math.sqrt(tableSize));
        for (int step = 0; step < maxSteps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed == " + seed + ", Step == " + step);
            }
            simulateShiftAwareStep(stepSize, random, table, columnInfo, en);
        }
    }


    public void testUngroupIncrementalPartialModificationsRebase() {
        final int minimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try (final SafeCloseable ignored2 = () -> QueryTable.setMinimumUngroupBase(minimumUngroupBase)) {
            for (int seed = 0; seed < 20; ++seed) {
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testUngroupIncrementalPartialModificationsRebase(10, false, seed, 10);
                }
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testUngroupIncrementalPartialModificationsRebase(10, true, seed, 10);
                }
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void testUngroupIncrementalPartialModificationsRebase(final int tableSize,
            final boolean nullFill,
            final int seed,
            final int maxSteps) {
        final Random random = new Random(seed);
        QueryScope.addParam("f", new SimpleDateFormat("dd HH:mm:ss"));

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(tableSize, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sentinel", "MiddleArray", "SmallSize", "MiddleSize"},
                        new IntGenerator(0, 100),
                        new BooleanGenerator(0.05),
                        new IntGenerator(0, 3),
                        new IntGenerator(4, 7)));


        final QueryTable withArrays =
                addRandomizedArrays(table.update("ArraySize=MiddleArray ? MiddleSize: SmallSize"),
                        "ArraySize", List.of(ColumnDefinition.of("IA", Type.find(int[].class)),
                                ColumnDefinition.of("DA", Type.find(double[].class)),
                                ColumnDefinition.of("LA", Type.find(long[].class)),
                                ColumnDefinition.of("SA", Type.find(String[].class))),
                        seed);

        final Table validated = TableUpdateValidator.make("withArrays", withArrays).getResultTable();

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> validated.ungroup(nullFill)),
        };

        final GenerateTableUpdates.SimulationProfile FEWER_MODS =
                new GenerateTableUpdates.SimulationProfile() {
                    {
                        MOD_ADDITIONAL_COLUMN = 10;
                    }
                };

        final int stepSize = (int) Math.ceil(Math.sqrt(tableSize));
        for (int step = 0; step < maxSteps; step++) {
            simulateShiftAwareStep(FEWER_MODS, "Seed == " + seed + ", Step == " + step, stepSize, random, table,
                    columnInfo, en);
        }
    }


    public static QueryTable addRandomizedArrays(final Table parent, final String sizeColumn,
            final List<ColumnDefinition<?>> outputColumns, final int seed) {
        final Random random = new Random(seed);
        final Map<String, ColumnSource<?>> columnSources = new LinkedHashMap<>(parent.getColumnSourceMap());
        final ColumnSource<Integer> sizeColumnSource = parent.getColumnSource(sizeColumn, int.class);
        for (final ColumnDefinition<?> columnDefinition : outputColumns) {
            final ObjectTestSource<?> objectTestSource = new ObjectTestSource<>(columnDefinition.getDataType());
            if (columnDefinition.getComponentType() == int.class || columnDefinition.getComponentType() == double.class
                    || columnDefinition.getComponentType() == String.class
                    || columnDefinition.getComponentType() == long.class) {
                columnSources.put(columnDefinition.getName(), objectTestSource);
                generateArrayValue(random,
                        columnDefinition,
                        parent.getRowSet(),
                        sizeColumnSource,
                        objectTestSource);
            } else {
                throw new UnsupportedOperationException();
            }
        }
        assertEquals(parent.getColumnSourceMap().size() + outputColumns.size(), columnSources.size());
        final QueryTable result = new QueryTable(parent.getRowSet(), columnSources);
        final ModifiedColumnSet.Transformer identityTransformer = ((QueryTable) parent)
                .newModifiedColumnSetTransformer(result, parent.getColumnSourceMap().keySet().toArray(new String[0]));

        final ModifiedColumnSet sizeMcs = ((QueryTable) parent).newModifiedColumnSet(sizeColumn);
        final InstrumentedTableUpdateListener listener =
                new BaseTable.ListenerImpl("addRandomizedArrays", parent, result) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {
                        final ModifiedColumnSet modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                        identityTransformer.clearAndTransform(upstream.modifiedColumnSet(), modifiedColumnSet);

                        final boolean sizeChanged = upstream.modifiedColumnSet().containsAny(sizeMcs);

                        for (int ii = 0; ii < outputColumns.size(); ++ii) {
                            final ColumnDefinition<?> columnDefinition = outputColumns.get(ii);
                            final ObjectTestSource<?> outputSource =
                                    (ObjectTestSource<?>) columnSources.get(columnDefinition.getName());
                            outputSource.remove(upstream.removed());


                            upstream.shifted().apply(outputSource::shift);

                            generateArrayValue(random,
                                    columnDefinition,
                                    upstream.added(),
                                    sizeColumnSource,
                                    outputSource);

                            if (upstream.modified().isEmpty()) {
                                continue;
                            }

                            if (!sizeChanged && random.nextBoolean()) {
                                // occasionally skip changing the sizes for some columns
                                continue;
                            }
                            generateArrayValue(random,
                                    columnDefinition,
                                    upstream.modified(),
                                    sizeColumnSource,
                                    outputSource);
                            modifiedColumnSet.setAll(columnDefinition.getName());
                        }

                        final TableUpdateImpl downstream = new TableUpdateImpl(upstream.added().copy(),
                                upstream.removed().copy(),
                                upstream.modified().copy(),
                                upstream.shifted(),
                                modifiedColumnSet);
                        result.notifyListeners(downstream);
                    }
                };
        result.manage(listener);
        parent.addUpdateListener(listener);
        return result;
    }

    private static void generateArrayValue(final Random random,
            final ColumnDefinition<?> columnDefinition,
            final RowSet rowSet,
            final ColumnSource<Integer> sizeColumn,
            final ObjectTestSource<?> objectTestSource) {
        try (final WritableObjectChunk<Object, Values> output =
                WritableObjectChunk.makeWritableChunk(rowSet.intSize())) {
            final MutableInt pos = new MutableInt();
            rowSet.forAllRowKeys(rk -> {
                final int size = sizeColumn.getInt(rk);
                if (columnDefinition.getComponentType() == int.class) {
                    final int[] ret = new int[size];
                    for (int ii = 0; ii < size; ++ii) {
                        ret[ii] = random.nextInt();
                    }
                    output.set(pos.getAndIncrement(), ret);
                } else if (columnDefinition.getComponentType() == long.class) {
                    final long[] ret = new long[size];
                    for (int ii = 0; ii < size; ++ii) {
                        ret[ii] = random.nextInt();
                    }
                    output.set(pos.getAndIncrement(), ret);
                } else if (columnDefinition.getComponentType() == double.class) {
                    final double[] ret = new double[size];
                    for (int ii = 0; ii < size; ++ii) {
                        ret[ii] = random.nextDouble();
                    }
                    output.set(pos.getAndIncrement(), ret);
                } else if (columnDefinition.getComponentType() == String.class) {
                    final String[] ret = new String[size];
                    for (int ii = 0; ii < size; ++ii) {
                        final long value = random.nextInt();
                        ret[ii] = Long.toString(value, 'z' - 'a' + 10);
                    }
                    output.set(pos.getAndIncrement(), ret);
                }
            });
            objectTestSource.add(rowSet, output);
        }
    }

    public static ObjectVector<String> toString(final DoubleVector vector) {
        final String[] data = new String[vector.intSize()];
        for (int ii = 0; ii < vector.intSize(); ++ii) {
            data[ii] = Double.toString(vector.get(ii));
        }
        return new ObjectVectorDirect<>(data);
    }

    public void testUngroupIncremental() throws ParseException {
        testUngroupIncremental(100, false, 0, 50);
        testUngroupIncremental(100, true, 0, 50);
    }

    @SuppressWarnings("SameParameterValue")
    private void testUngroupIncremental(final int tableSize, final boolean nullFill, final int seed, final int maxSteps)
            throws ParseException {
        final Random random = new Random(seed);
        QueryScope.addParam("f", new SimpleDateFormat("dd HH:mm:ss"));

        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable table = getTable(tableSize, random,
                columnInfo = initColumnInfos(new String[] {"Date", "C1", "C2", "C3", "C4"},
                        new DateGenerator(format.parse("2011-02-02"), format.parse("2011-02-03")),
                        new SetGenerator<>("a", "b"),
                        new SetGenerator<>(10, 20, 30),
                        new SetGenerator<>(ArrayTypeUtils.EMPTY_STRING_ARRAY, new String[] {"a", "b"},
                                new String[] {"a", "b", "c"}),
                        new SetGenerator<>(null, new double[] {Math.PI}, new double[] {Math.E, Math.PI},
                                new double[] {Math.E, Math.PI, Math.PI * 2})));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> table.groupBy().ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill)),
                new UpdateValidatorNugget(table.groupBy("C1").ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy().ungroup(nullFill, "C1")),
                EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2")),
                EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2").ungroup(nullFill,
                        "Date")),
                EvalNugget.from(() -> table.groupBy("C1").sort("C1").ungroup(nullFill, "C2").ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy("C1", "C2").sort("C1", "C2").ungroup(nullFill)),
                EvalNugget
                        .from(() -> table.groupBy().update("Date=Date.toArray()", "C1=C1.toArray()", "C2=C2.toArray()")
                                .ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill)),
                EvalNugget
                        .from(() -> table.groupBy().update("Date=Date.toArray()", "C1=C1.toArray()", "C2=C2.toArray()")
                                .ungroup(nullFill, "C1")),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill, "C2")),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill, "C2").ungroup(nullFill, "Date")),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill, "C2").ungroup(nullFill)),
                EvalNugget.from(
                        () -> table.groupBy("C1", "C2").update("Date=Date.toArray()").sort("C1", "C2")
                                .ungroup(nullFill)),
                EvalNugget.from(() -> table.groupBy("C1").update("Date=Date.toArray()", "C2=C2.toArray()").sort("C1")
                        .ungroup(nullFill)),
                EvalNugget.from(() -> table.view("C3").ungroup(nullFill)),
                EvalNugget.from(
                        () -> table.select("C3=new io.deephaven.vector.ObjectVectorDirect(C3)").ungroup(nullFill)),
                EvalNugget.from(() -> table.view("C4").ungroup(nullFill)),
                EvalNugget.from(() -> table.select(
                        "C5=C4 == null ? null : new io.deephaven.vector.DoubleVectorDirect(C4)",
                        "C6=C4==null ? null : io.deephaven.engine.table.impl.QueryTableUngroupTest.toString(C5)",
                        "C7=C6==null ? null : C6.toArray()").ungroup(nullFill)),
                EvalNugget.from(() -> convertToUngroupable(table.select("C4", "C5=C4"), "C4", "C5").ungroup(nullFill)),
        };

        final int stepSize = (int) Math.ceil(Math.sqrt(tableSize));
        for (int step = 0; step < maxSteps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed == " + seed + ", Step == " + step);
            }
            simulateShiftAwareStep(stepSize, random, table, columnInfo, en);
        }
    }

    public void testUngroupBlink() throws ParseException {
        testUngroupBlink(100, false, 0, 20);
        testUngroupBlink(100, true, 0, 20);
    }

    @SuppressWarnings("SameParameterValue")
    private void testUngroupBlink(final int tableSize, final boolean nullFill, final int seed, final int maxSteps)
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
        table.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(
                        () -> TickSuppressor.convertModificationsToAddsAndRemoves(table.removeBlink().groupBy("C1"))
                                .assertBlink().sort("C1").ungroup(nullFill)),
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
            fail("Expected IllegalStateException");
        } catch (IllegalStateException ignored) {
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
                    EvalNugget.from(() -> convertToUngroupable(mismatch, "MyBoolean", "MyDouble").ungroup(nullFill)),
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
                intCol("EulavI", NULL_INT, 1,
                        NULL_INT, NULL_INT),
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

        assertEquals(NULL_INT, ungrouped.getColumnSource("EulavI").getInt(firstKey));
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

        assertEquals(NULL_INT,
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

    public void testUngroupRebase() {
        final int oldMinimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try {
            ColumnHolder<?> arrayColumnHolder = col("Y", new int[] {10, 20}, new int[] {110, 120, 130});
            final QueryTable table = TstUtils.testRefreshingTable(col("X", 1, 3), arrayColumnHolder);

            EvalNugget[] en = new EvalNugget[] {
                    EvalNugget.from(table::ungroup)
            };

            // don't remove or add anything, let's just do one step
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                    .startCycleForUnitTests();
            RowSet keysToAdd = RowSetFactory.empty();
            RowSet keysToRemove = RowSetFactory.empty();
            RowSet keysToModify = RowSetFactory.empty();
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                    .completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase base", en);

            // Now let's modify the first row, but not cause a rebase
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                    .startCycleForUnitTests();
            keysToAdd = RowSetFactory.empty();
            keysToRemove = RowSetFactory.empty();
            keysToModify = RowSetFactory.fromKeys(0);
            ColumnHolder<?> keyModifications = col("X", 1);
            ColumnHolder<?> valueModifications = col("Y", new int[] {10, 20, 30});
            TstUtils.addToTable(table, keysToModify, keyModifications, valueModifications);
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                    .completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase add no rebase", en);


            // Now let's modify the first row such that we will cause a rebasing operation
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                    .startCycleForUnitTests();
            keysToAdd = RowSetFactory.empty();
            keysToRemove = RowSetFactory.empty();
            keysToModify = RowSetFactory.fromKeys(0);
            valueModifications = col("Y", new int[] {10, 20, 30, 40, 50, 60});
            TstUtils.addToTable(table, keysToModify, keyModifications, valueModifications);
            table.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                    .completeCycleForUnitTests();
            TableTools.show(table);
            TstUtils.validate("ungroupRebase rebase", en);

            // Time to start fresh again, so we can do an addition operation,
            // without having such a high base for the table.
            arrayColumnHolder =
                    col("Y", new int[] {10, 20}, new int[] {200}, new int[] {110, 120, 130}, new int[] {310});
            final QueryTable table2 = TstUtils.testRefreshingTable(col("X", 1, 2, 3, 4), arrayColumnHolder);

            en = new EvalNugget[] {
                    EvalNugget.from(table2::ungroup)
            };

            // let's remove the second row, so that we can add something to it on the next step
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                    .startCycleForUnitTests();
            keysToAdd = RowSetFactory.fromKeys();
            keysToRemove = RowSetFactory.fromKeys(1);
            keysToModify = RowSetFactory.fromKeys();
            TstUtils.removeRows(table2, keysToRemove);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase remove", en);

            // now we want to add it back, causing a rebase, and modify another
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
            keysToAdd = RowSetFactory.fromKeys(1);
            keysToRemove = RowSetFactory.fromKeys();
            keysToModify = RowSetFactory.fromKeys(2, 3);

            final ColumnHolder<?> keyAdditions = col("X", 2);
            final ColumnHolder<?> valueAdditions = col("Y", new int[] {210, 220, 230, 240, 250, 260});
            valueModifications = col("Y", new int[] {110, 120, 140}, new int[] {320});
            keyModifications = col("X", 3, 4);

            TstUtils.addToTable(table2, keysToAdd, keyAdditions, valueAdditions);
            TstUtils.addToTable(table2, keysToModify, keyModifications, valueModifications);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add rebase", en);

            // an empty step
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
            keysToAdd = RowSetFactory.fromKeys();
            keysToRemove = RowSetFactory.fromKeys();
            keysToModify = RowSetFactory.fromKeys();
            TstUtils.addToTable(table2, keysToModify, intCol("X"), col("Y"));
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add post rebase", en);

            // and another step, to make sure everything is fine post rebase
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
            keysToAdd = RowSetFactory.fromKeys();
            keysToRemove = RowSetFactory.fromKeys();
            keysToModify = RowSetFactory.fromKeys(2, 3);
            TstUtils.addToTable(table2, keysToModify, keyModifications, valueModifications);
            table2.notifyListeners(keysToAdd, keysToRemove, keysToModify);
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
            TableTools.show(table2);
            TstUtils.validate("ungroupRebase add post rebase 2", en);

        } finally {
            QueryTable.setMinimumUngroupBase(oldMinimumUngroupBase);
        }
    }

    public void testRevertToMinimumSize() {
        final int oldMinimumUngroupBase = QueryTable.setMinimumUngroupBase(2);
        try {
            final QueryTable table = TstUtils.testRefreshingTable(intCol("X", 1, 3),
                    col("Y", new int[] {10, 20, 30, 40, 50, 60}, new int[] {110}));
            final Table ungrouped = table.ungroup();

            final Table expected =
                    TableTools.newTable(intCol("X", 1, 1, 1, 1, 1, 1, 3), intCol("Y", 10, 20, 30, 40, 50, 60, 110));
            assertTableEquals(expected, ungrouped);

            // our base is going to be 3, not 2; so the maximum key expected should be at position 8
            assertEquals(8, ungrouped.getRowSet().lastRowKey());

            // start fresh
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                final WritableRowSet toRemove = i(0, 1);
                final WritableRowSet toAdd = i(0, 1);
                table.getRowSet().writableCast().resetTo(toAdd);
                // we are being sneaky here by leaving 1 in place but calling it an add + remove
                TstUtils.addToTable(table, i(0), intCol("X", 2), col("Y", new int[] {201, 202}));
                table.notifyListeners(
                        new TableUpdateImpl(toAdd, toRemove, i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });

            final Table expected2 = TableTools.newTable(intCol("X", 2, 2, 3), intCol("Y", 201, 202, 110));
            assertTableEquals(expected2, ungrouped);

            // our base should be 2 again, not 3; so the maximum key expected should be at position 4
            assertEquals(4, ungrouped.getRowSet().lastRowKey());

            // start fresh again, but this time we are going to exceed the minimum base
            updateGraph.runWithinUnitTestCycle(() -> {
                final WritableRowSet toRemove = i(0, 1);
                final WritableRowSet toAdd = i(0, 3);
                table.getRowSet().writableCast().resetTo(toAdd);
                TstUtils.addToTable(table, i(0, 3), intCol("X", 4, 5),
                        col("Y", new int[] {401, 402, 403, 404, 405, 406, 407, 408, 409, 410}, new int[] {501}));
                table.notifyListeners(
                        new TableUpdateImpl(toAdd, toRemove, i(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });

            final Table expected3 = TableTools.newTable(intCol("X", 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5),
                    intCol("Y", 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 501));
            assertTableEquals(expected3, ungrouped);

            // our base should be 4 now, not 3; so the maximum key expected should be at position 3 (our last row key) *
            // 16
            assertEquals(3 * 16, ungrouped.getRowSet().lastRowKey());

        } finally {
            QueryTable.setMinimumUngroupBase(oldMinimumUngroupBase);
        }
    }
}
