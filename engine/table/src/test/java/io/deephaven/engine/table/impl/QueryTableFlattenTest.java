/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.showWithRowSet;
import static io.deephaven.engine.table.impl.TstUtils.*;

public class QueryTableFlattenTest extends QueryTableTestBase {

    private void testFlatten(int size) {
        final Random random = new Random(0);
        final TstUtils.ColumnInfo columnInfo[];
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                        new TstUtils.IntGenerator(10, 100, 0.1),
                        new TstUtils.SetGenerator<>(10.1, 20.1, 30.1)));
        if (printTableUpdates) {
            showWithRowSet(queryTable);
        }
        final EvalNuggetInterface en[] = new EvalNuggetInterface[] {
                new FlatChecker(queryTable.flatten()),
                new TableComparator(queryTable, queryTable.flatten()),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.flatten();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.updateView("i2=intCol*2").flatten();
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.flatten().updateView("i2=intCol*2");
                    }
                },
                new TableComparator(queryTable.updateView("i2=intCol*2").flatten(),
                        queryTable.flatten().updateView("i2=intCol*2")),
        };
        for (int i = 0; i < 100; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    @Test
    public void testFlatten() {
        final int[] sizes = {10, 20, 30};
        for (int size : sizes) {
            testFlatten(size);
        }
    }

    @Test
    public void testLegacyFlatten3() {
        final long[] data = new long[10];
        data[0] = 12;
        data[1] = 20;
        data[2] = 24;
        data[3] = 46;
        data[4] = 74;
        data[5] = 76;
        data[6] = 100;
        data[7] = 101;
        data[8] = 102;
        data[9] = 104;
        final QueryTable queryTable =
                TstUtils.testRefreshingTable(i(data).toTracking(), longCol("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleShiftObliviousListener::new);

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(70, 71, 78, 81), longCol("intCol", 70, 71, 78, 81));
            queryTable.notifyListeners(i(70, 71, 78, 81), i(), i(24, 46, 74, 100, 104));
        }, indexByRange(10, 13), i(), indexByRange(2, 9));
    }

    @Test
    public void testLegacyFlattenModifications() {
        QueryTableTest.testLegacyFlattenModifications(arg -> (QueryTable) arg.flatten());
    }

    @Test
    public void testRemoveWithLowMod() {
        Integer[] data = new Integer[10];
        for (int ii = 0; ii < data.length; ++ii) {
            data[ii] = ii * 10;
        }
        final QueryTable queryTable = TstUtils.testRefreshingTable(indexByRange(0, 9).toTracking(),
                c("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleListener::new);

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(0), c("intCol", 1));
            TstUtils.removeRows(queryTable, i(2));
            queryTable.notifyListeners(i(), i(2), i(0));
        }, i(), i(2), i(0), shiftDataByValues(3, 9, -1));
    }

    @Test
    public void testLegacyRemoveWithLowMod() {
        Integer[] data = new Integer[10];
        for (int ii = 0; ii < data.length; ++ii) {
            data[ii] = ii * 10;
        }
        final QueryTable queryTable = TstUtils.testRefreshingTable(indexByRange(0, 9).toTracking(),
                c("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleShiftObliviousListener::new);

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(0), c("intCol", 1));
            TstUtils.removeRows(queryTable, i(2));
            queryTable.notifyListeners(i(), i(2), i(0));
        }, i(), i(9), i(0, 2, 3, 4, 5, 6, 7, 8));
    }

    @Test
    public void testRemoveWithHighMod() {
        Integer[] data = new Integer[10];
        for (int ii = 0; ii < data.length; ++ii) {
            data[ii] = ii * 10;
        }
        final QueryTable queryTable = TstUtils.testRefreshingTable(indexByRange(0, 9).toTracking(),
                c("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleListener::new);

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(8), c("intCol", 1));
            TstUtils.removeRows(queryTable, i(2));
            queryTable.notifyListeners(i(), i(2), i(8));
        }, i(), i(2), i(7), shiftDataByValues(3, 9, -1));
    }


    @Test
    public void testLegacyRemoveWithHighMod() {
        Integer[] data = new Integer[10];
        for (int ii = 0; ii < data.length; ++ii) {
            data[ii] = ii * 10;
        }
        final QueryTable queryTable = TstUtils.testRefreshingTable(indexByRange(0, 9).toTracking(),
                c("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleShiftObliviousListener::new);

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(8), c("intCol", 1));
            TstUtils.removeRows(queryTable, i(2));
            queryTable.notifyListeners(i(), i(2), i(8));
        }, i(), i(9), i(2, 3, 4, 5, 6, 7, 8));
    }

    @Test
    public void testFlatten3() {
        final long[] data = new long[10];
        data[0] = 12;
        data[1] = 20;
        data[2] = 24;
        data[3] = 46;
        data[4] = 74;
        data[5] = 76;
        data[6] = 100;
        data[7] = 101;
        data[8] = 102;
        data[9] = 104;
        final QueryTable queryTable =
                TstUtils.testRefreshingTable(i(data).toTracking(), longCol("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleListener::new);

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(70, 71, 78, 81), longCol("intCol", 70, 71, 78, 81));
            TstUtils.removeRows(queryTable, i());
            queryTable.notifyListeners(i(70, 71, 78, 81), i(), i(24, 46, 74, 100, 104));
        }, i(4, 5, 8, 9), i(), i(2, 3, 6, 10, 13), shiftDataByValues(4, 5, 2, 6, 9, 4));
    }

    @Test
    public void testFlattenModifications() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                c("intCol", 10, 20, 40, 60));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleListener::new);

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(3), c("intCol", 30));
            TstUtils.removeRows(queryTable, i(2));
            queryTable.notifyListeners(i(3), i(2), i());
        }, i(1), i(1), i(), shiftDataByValues());

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(3), c("intCol", 30));
            queryTable.notifyListeners(i(), i(), i(3));
        }, i(), i(), i(1), shiftDataByValues());

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(3, 5), c("intCol", 30, 50));
            queryTable.notifyListeners(i(5), i(), i(3));
        }, i(3), i(), i(1), shiftDataByValues(3, 3, 1));
    }

    private static class TestHelper<T extends TableListener> {
        final QueryTable sourceTable;
        final TableUpdateValidator validator;
        final T listener;

        long updateCount = 0;

        public interface ListenerFactory<T extends TableListener> {
            T newListener(QueryTable queryTable);
        }

        TestHelper(final Table sourceTable, final ListenerFactory<T> factory) {
            this.sourceTable = (QueryTable) sourceTable;
            listener = factory.newListener(this.sourceTable);
            if (listener instanceof ShiftObliviousListener) {
                this.sourceTable.listenForUpdates((ShiftObliviousListener) listener);
            } else if (listener instanceof SimpleListener) {
                this.sourceTable.listenForUpdates((TableUpdateListener) listener);
            } else {
                throw new IllegalArgumentException("Listener type unsupported: " + listener.getClass().getName());
            }

            validator = TableUpdateValidator.make(this.sourceTable);
            final QueryTable validatorTable = validator.getResultTable();
            final TableUpdateListener validatorTableListener =
                    new InstrumentedTableUpdateListenerAdapter(validatorTable, false) {
                        @Override
                        public void onUpdate(TableUpdate upstream) {}

                        @Override
                        public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                            TestCase.fail(originalException.getMessage());
                        }
                    };
            validatorTable.listenForUpdates(validatorTableListener);

            showWithRowSet(sourceTable);
        }

        void modAndValidate(final Runnable modTable, final RowSet added, final RowSet removed, final RowSet modified) {
            modAndValidate(modTable, added, removed, modified, RowSetShiftData.EMPTY);
        }

        void modAndValidate(final Runnable modTable, final RowSet added, final RowSet removed, final RowSet modified,
                final RowSetShiftData shifted) {
            ++updateCount;

            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(modTable::run);
            showWithRowSet(sourceTable);

            if (listener instanceof SimpleShiftObliviousListener) {
                Assert.assertEquals(0, shifted.size());
                validate((SimpleShiftObliviousListener) listener, updateCount, added, removed, modified);
            } else if (listener instanceof SimpleListener) {
                validate((SimpleListener) listener, updateCount, added, removed, modified, shifted);
            }
        }
    }

    private static WritableRowSet indexByRange(long firstKey, long lastKey) {
        return RowSetFactory.fromRange(firstKey, lastKey);
    }

    private static RowSetShiftData shiftDataByValues(long... values) {
        if (values.length % 3 != 0) {
            throw new IllegalArgumentException("shift data is defined by triplets {start, end, shift}");
        }
        RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        for (int idx = 0; idx < values.length; idx += 3) {
            builder.shiftRange(values[idx], values[idx + 1], values[idx + 2]);
        }
        return builder.build();
    }

    private static void validate(final SimpleShiftObliviousListener listener, final long count, final RowSet added,
            final RowSet removed, final RowSet modified) {
        Assert.assertEquals("simpleListener.getCount()", count, listener.getCount());
        Assert.assertEquals("simpleListener.added", added, listener.added);
        Assert.assertEquals("simpleListener.removed", removed, listener.removed);
        Assert.assertEquals("simpleListener.modified", modified, listener.modified);
    }

    private static void validate(final SimpleListener listener, final long count, final RowSet added,
            final RowSet removed, final RowSet modified, final RowSetShiftData shifted) {
        Assert.assertEquals("simpleListener.getCount()", count, listener.getCount());
        Assert.assertEquals("simpleListener.added", added, listener.update.added());
        Assert.assertEquals("simpleListener.removed", removed, listener.update.removed());
        Assert.assertEquals("simpleListener.modified", modified, listener.update.modified());
        Assert.assertEquals("simpleListener.shifted", shifted, listener.update.shifted());
    }

    /**
     * Makes sure that the row set of our table is actually contiguous.
     */
    protected static class FlatChecker implements EvalNuggetInterface {
        private Table t1;

        FlatChecker(Table t1) {
            this.t1 = t1;
        }

        @Override
        public void validate(String msg) {
            if (t1.getRowSet().size() > 0) {
                Assert.assertEquals(msg, t1.getRowSet().size() - 1, t1.getRowSet().lastRowKey());
            }
        }

        @Override
        public void show() throws IOException {
            showWithRowSet(t1);
        }
    }

    public void testFlattenFollowedBySumBy() {
        // TODO: Write a test that just makes a RedirectedColumnSource with a wrapper, and fill/query it.
        final QueryTable upstream = TstUtils.testRefreshingTable(ir(0, 100_000).toTracking());
        final Table input = upstream.updateView("A=ii", "B=ii % 1000", "C=ii % 2 == 0");
        final Table odds = input.where("!C");
        final Table expected = odds.sumBy("B");
        final Table actual = odds.flatten().sumBy("B");
        assertTableEquals(expected, actual);
        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(upstream, ir(100_001, 200_000));
            upstream.notifyListeners(ir(100_001, 200_000), i(), ir(0, 100_000));
        });
        assertTableEquals(expected, actual);
    }
}
