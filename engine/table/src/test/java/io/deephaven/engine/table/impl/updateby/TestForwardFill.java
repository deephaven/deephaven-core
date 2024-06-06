//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.function.Basic;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.function.ThrowingRunnable;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.engine.testutil.junit4.EngineCleanup.printTableUpdates;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category(OutOfBandTest.class)
public class TestForwardFill extends BaseUpdateByTest {

    // region Zero Key Tests
    @Test
    public void testStaticZeroKey() {
        final QueryTable t = createTestTable(100000, true, false, false, 0x507A70,
                new String[] {"charCol"}, new TestDataGenerator[] {new CharGenerator('A', 'Z', .1)}).t;

        final Table filled = t.updateBy(UpdateByOperation.Fill());
        for (String col : t.getDefinition().getColumnNamesArray()) {
            assertWithForwardFill(ColumnVectors.of(t, col).toArray(), ColumnVectors.of(filled, col).toArray());
        }
    }

    @Test
    public void testNoKeyAddOnly() throws Exception {
        final QueryTable src = testRefreshingTable(
                i(1, 3, 5, 6, 7, 9, 10, 11).toTracking(),
                stringCol("Sentinel", null, null, null, "G", "H", null, "K", "L"),
                intCol("Val", 1, 3, 5, NULL_INT, NULL_INT, 9, 10, NULL_INT));

        final Table result = src.updateBy(UpdateByOperation.Fill());
        final TableUpdateValidator validator = TableUpdateValidator.make((QueryTable) result);
        final FailureListener failureListener = new FailureListener();
        validator.getResultTable().addUpdateListener(failureListener);
        assertEquals(8, result.intSize());

        for (int ii = 0; ii < 2; ii++) {
            assertWithForwardFill(
                    ColumnVectors.of(src, ((Table) src).getDefinition().getColumns().get(ii).getName()).toArray(),
                    ColumnVectors.of(result, result.getDefinition().getColumns().get(ii).getName()).toArray());
        }

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(0, 8);
            addToTable(src, idx,
                    stringCol("Sentinel", "A", "I"),
                    intCol("Val", 0, 8));
            src.notifyListeners(idx, i(), i());
        });

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(4, 13);
            addToTable(src, idx,
                    stringCol("Sentinel", "E", null),
                    intCol("Val", 4, NULL_INT));
            src.notifyListeners(idx, i(), i());
        });

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(2, 12);
            addToTable(src, idx,
                    stringCol("Sentinel", "C", "M"),
                    intCol("Val", 2, 12));
            src.notifyListeners(idx, i(), i());
        });

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(14, 16);
            addToTable(src, idx,
                    stringCol("Sentinel", null, null),
                    intCol("Val", 14, 16));
            src.notifyListeners(idx, i(), i());
        });

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(15);
            addToTable(src, idx,
                    stringCol("Sentinel", "O"),
                    intCol("Val", 15));
            src.notifyListeners(idx, i(), i());
        });
    }

    @Test
    public void testNoKeyAddAndRemoves() throws Exception {
        final QueryTable src = testRefreshingTable(
                i(1, 3, 5, 6, 7, 9, 10, 11).toTracking(),
                stringCol("Sentinel", null, null, null, "G", "H", null, "K", "L"),
                intCol("Val", 1, 3, 5, NULL_INT, NULL_INT, 9, 10, NULL_INT));

        final Table result = src.updateBy(UpdateByOperation.Fill());
        final TableUpdateValidator validator = TableUpdateValidator.make((QueryTable) result);
        final FailureListener failureListener = new FailureListener();
        validator.getResultTable().addUpdateListener(failureListener);
        assertEquals(8, result.intSize());

        for (int ii = 0; ii < 2; ii++) {
            assertWithForwardFill(
                    ColumnVectors.of(src, ((Table) src).getDefinition().getColumns().get(ii).getName()).toArray(),
                    ColumnVectors.of(result, result.getDefinition().getColumns().get(ii).getName()).toArray());
        }

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(0, 12);
            addToTable(src, idx,
                    stringCol("Sentinel", "A", null),
                    intCol("Val", 0, NULL_INT));
            src.notifyListeners(idx, i(), i());
        });

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(3, 7);
            removeRows(src, idx);
            src.notifyListeners(i(), idx, i());
        });

        updateAndValidate(src, result, () -> {
            final RowSet ridx = i(0);
            final RowSet aidx = i(8);
            removeRows(src, ridx);
            addToTable(src, aidx,
                    stringCol("Sentinel", "I"),
                    intCol("Val", 8));
            src.notifyListeners(aidx, ridx, i());
        });

        updateAndValidate(src, result, () -> {
            final RowSet ridx = i(8);
            final RowSet aidx = i(7);
            removeRows(src, ridx);
            addToTable(src, aidx,
                    stringCol("Sentinel", "H"),
                    intCol("Val", 7));
            src.notifyListeners(aidx, ridx, i());
        });
    }

    @Test
    public void testNoKeyGeneral() throws Exception {
        final QueryTable src = testRefreshingTable(
                i(1, 3, 5, 6, 7, 9, 10, 11).toTracking(),
                stringCol("Sentinel", null, null, null, "G", "H", null, "time toK", "L"),
                intCol("Val", 1, 3, 5, NULL_INT, NULL_INT, 9, 10, NULL_INT));

        final Table result = src.updateBy(UpdateByOperation.Fill());
        final TableUpdateValidator validator = TableUpdateValidator.make((QueryTable) result);
        final FailureListener failureListener = new FailureListener();
        validator.getResultTable().addUpdateListener(failureListener);
        assertEquals(8, result.intSize());

        for (int ii = 0; ii < 2; ii++) {
            assertWithForwardFill(
                    ColumnVectors.of(src, ((Table) src).getDefinition().getColumns().get(ii).getName()).toArray(),
                    ColumnVectors.of(result, result.getDefinition().getColumns().get(ii).getName()).toArray());
        }

        // Add a key at the beginning and end, but null the end so it should fill as an L
        updateAndValidate(src, result, () -> {
            final RowSet idx = i(0, 12);
            addToTable(src, idx,
                    stringCol("Sentinel", "A", null),
                    intCol("Val", 0, NULL_INT));
            src.notifyListeners(idx, i(), i());
        });

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(3, 11);
            addToTable(src, idx,
                    stringCol("Sentinel", "D", null),
                    intCol("Val", 3, 11));
            src.notifyListeners(i(), i(), idx);
        });

        updateAndValidate(src, result, () -> {
            final RowSet idx = i(3, 7);
            final RowSet mIdx = i(1, 6);
            removeRows(src, idx);
            addToTable(src, mIdx,
                    stringCol("Sentinel", "B", "-H"),
                    intCol("Val", 1, 6));
            src.notifyListeners(i(), idx, mIdx);
        });

        updateAndValidate(src, result, () -> {
            final RowSet ridx = i(0);
            final RowSet aidx = i(8);
            final RowSet mIdx = i(11);
            removeRows(src, ridx);
            addToTable(src, i(8, 11),
                    stringCol("Sentinel", "I", "-L"),
                    intCol("Val", 8, -11));
            src.notifyListeners(aidx, ridx, mIdx);
        });

        updateAndValidate(src, result, () -> {
            final RowSet mIdx = i(8, 10);
            addToTable(src, mIdx,
                    stringCol("Sentinel", null, null),
                    intCol("Val", NULL_INT, NULL_INT));
            src.notifyListeners(i(), i(), mIdx);
        });
    }

    /**
     * This test specifically tests the case when you receive an update where there are a handful of consecutive
     * modified rows where some N of them modify values that were previously non-null and remain non-null, followed by
     * one that because null immediately subsequently.
     *
     * The update model will remove the modified index from the set of valid rows, but must track that a modified row
     * stayed valid, or we end up redirecting to something that was previously unmodified.
     */
    @Test
    public void testNoKeyModifyWithPreviousNoChange() throws Exception {
        final QueryTable src = testRefreshingTable(
                RowSetFactory.flat(7).toTracking(),
                stringCol("Sentinel", "A", "B", "C", "D", "E", null, "G"),
                intCol("Val", 0, 1, 2, 3, 4, 5, 6));

        final Table result = src.updateBy(UpdateByOperation.Fill());
        updateAndValidate(src, result, () -> {
            final RowSet idx = i(2, 3, 4);
            addToTable(src, idx,
                    stringCol("Sentinel", "CC", "DD", null),
                    intCol("Val", 2, 3, 4));
            src.notifyListeners(i(), i(), idx);
        });
    }

    @Test
    public void testNoKeyIncremental() {
        for (int size = 10; size <= 10000; size *= 10) {
            for (int seed = 10; seed < 30; seed++) {
                doTestNoKeyIncremental("size-" + size + "-seed-" + seed, 10, size, seed);
            }
        }
    }

    private void doTestNoKeyIncremental(String context, int steps, int size, int seed) {
        final CreateResult result = createTestTable(size, true, false, true, seed,
                new String[] {"charCol"}, new TestDataGenerator[] {new CharGenerator('A', 'Z', .1)});
        final QueryTable queryTable = result.t;

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new EvalNugget() {
                    public Table e() {
                        return queryTable.updateBy(UpdateByOperation.Fill());
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return ((TableDefaults) queryTable.sort("intCol")).updateBy(UpdateByOperation.Fill());
                    }
                },
        };

        for (int step = 0; step < steps; step++) {
            if (printTableUpdates()) {
                System.out.println(context + " Step = " + step);
            }
            try {
                simulateShiftAwareStep(size, result.random, queryTable, result.infos, en);
            } catch (Throwable t) {
                System.out
                        .println("Crapped out on step " + step + " steps " + steps + " size " + size + " seed " + seed);
                throw t;
            }
        }
    }

    void updateAndValidate(QueryTable src, Table result, ThrowingRunnable<?> updateFunc)
            throws Exception {
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(updateFunc);

        try {
            for (int ii = 0; ii < 2; ii++) {
                assertWithForwardFill(
                        ColumnVectors.of(src, src.getDefinition().getColumns().get(ii).getName()).toArray(),
                        ColumnVectors.of(result, result.getDefinition().getColumns().get(ii).getName()).toArray());
            }
        } catch (Throwable ex) {
            System.out.println("ERROR: Source table:");
            TableTools.showWithRowSet(src, src.size());
            System.out.println("=========================================");
            TableTools.showWithRowSet(result, result.size());

            throw ex;
        }
    }

    final void assertWithForwardFill(@NotNull final Object expected, @NotNull final Object actual) {
        if (expected instanceof char[]) {
            assertArrayEquals(Basic.forwardFill((char[]) expected), (char[]) actual);
        } else if (expected instanceof byte[]) {
            assertArrayEquals(Basic.forwardFill((byte[]) expected), (byte[]) actual);
        } else if (expected instanceof short[]) {
            assertArrayEquals(Basic.forwardFill((short[]) expected), (short[]) actual);
        } else if (expected instanceof int[]) {
            assertArrayEquals(Basic.forwardFill((int[]) expected), (int[]) actual);
        } else if (expected instanceof long[]) {
            assertArrayEquals(Basic.forwardFill((long[]) expected), (long[]) actual);
        } else if (expected instanceof float[]) {
            assertArrayEquals(Basic.forwardFill((float[]) expected), (float[]) actual, .001f);
        } else if (expected instanceof double[]) {
            assertArrayEquals(Basic.forwardFill((double[]) expected), (double[]) actual, .001d);
        } else {
            assertArrayEquals(Basic.forwardFillObj((Object[]) expected), (Object[]) actual);
        }
    }
    // endregion

    // region Bucketed Tests
    @Test
    public void testStaticBucketed() {
        final QueryTable t = createTestTable(100000, true, false, false, 0x507A70,
                new String[] {"charCol"}, new TestDataGenerator[] {new CharGenerator('A', 'Z', .1)}).t;

        final Table filled = t.updateBy(UpdateByOperation.Fill(), "Sym");

        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = filled.partitionBy("Sym");

        String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithForwardFill(ColumnVectors.of(source, col).toArray(), ColumnVectors.of(actual, col).toArray());
            });
            return source;
        });
    }

    @Test
    public void testStaticGroupedBucketed() {
        final QueryTable t = createTestTable(100000, true, true, false, 0x507A70,
                new String[] {"charCol"}, new TestDataGenerator[] {new CharGenerator('A', 'Z', .1)}).t;

        final Table filled = t.updateBy(UpdateByOperation.Fill(), "Sym");

        final PartitionedTable preOp = t.partitionBy("Sym");
        final PartitionedTable postOp = filled.partitionBy("Sym");

        String[] columns = Arrays.stream(t.getDefinition().getColumnNamesArray())
                .filter(col -> !col.equals("Sym") && !col.equals("boolCol")).toArray(String[]::new);

        preOp.partitionedTransform(postOp, (source, actual) -> {
            Arrays.stream(columns).forEach(col -> {
                assertWithForwardFill(ColumnVectors.of(source, col).toArray(), ColumnVectors.of(actual, col).toArray());
            });
            return source;
        });
    }
    // endregion

    @Test
    public void testZeroKeyAppendOnly() {
        doTestAppendOnly(false);
    }

    @Test
    public void testBucketedAppendOnly() {
        doTestAppendOnly(true);
    }

    private void doTestAppendOnly(boolean bucketed) {
        final CreateResult result = createTestTable(10000, bucketed, false, true, 0x31313131);
        final QueryTable t = result.t;
        t.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed ? t.updateBy(UpdateByOperation.Fill(), "Sym")
                                : t.updateBy(UpdateByOperation.Fill());
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < 100; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(100, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }
}
