/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static io.deephaven.db.tables.utils.TableTools.longCol;
import static io.deephaven.db.tables.utils.TableTools.showWithIndex;
import static io.deephaven.db.v2.TstUtils.*;

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
            showWithIndex(queryTable);
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
            TstUtils.testRefreshingTable(i(data), longCol("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleListener::new);

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
        final QueryTable queryTable =
            TstUtils.testRefreshingTable(indexByRange(0, 9), c("intCol", data));

        final TestHelper helper =
            new TestHelper<>(queryTable.flatten(), SimpleShiftAwareListener::new);

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
        final QueryTable queryTable =
            TstUtils.testRefreshingTable(indexByRange(0, 9), c("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleListener::new);

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
        final QueryTable queryTable =
            TstUtils.testRefreshingTable(indexByRange(0, 9), c("intCol", data));

        final TestHelper helper =
            new TestHelper<>(queryTable.flatten(), SimpleShiftAwareListener::new);

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
        final QueryTable queryTable =
            TstUtils.testRefreshingTable(indexByRange(0, 9), c("intCol", data));

        final TestHelper helper = new TestHelper<>(queryTable.flatten(), SimpleListener::new);

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
            TstUtils.testRefreshingTable(i(data), longCol("intCol", data));

        final TestHelper helper =
            new TestHelper<>(queryTable.flatten(), SimpleShiftAwareListener::new);

        helper.modAndValidate(() -> {
            addToTable(queryTable, i(70, 71, 78, 81), longCol("intCol", 70, 71, 78, 81));
            TstUtils.removeRows(queryTable, i());
            queryTable.notifyListeners(i(70, 71, 78, 81), i(), i(24, 46, 74, 100, 104));
        }, i(4, 5, 8, 9), i(), i(2, 3, 6, 10, 13), shiftDataByValues(4, 5, 2, 6, 9, 4));
    }

    @Test
    public void testFlattenModifications() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
            c("intCol", 10, 20, 40, 60));

        final TestHelper helper =
            new TestHelper<>(queryTable.flatten(), SimpleShiftAwareListener::new);

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

    private static class TestHelper<T extends ListenerBase> {
        final QueryTable sourceTable;
        final TableUpdateValidator validator;
        final T listener;

        long updateCount = 0;

        public interface ListenerFactory<T extends ListenerBase> {
            T newListener(QueryTable queryTable);
        }

        TestHelper(final Table sourceTable, final ListenerFactory<T> factory) {
            this.sourceTable = (QueryTable) sourceTable;
            listener = factory.newListener(this.sourceTable);
            if (listener instanceof Listener) {
                this.sourceTable.listenForUpdates((Listener) listener);
            } else if (listener instanceof ShiftAwareListener) {
                this.sourceTable.listenForUpdates((ShiftAwareListener) listener);
            } else {
                throw new IllegalArgumentException(
                    "Listener type unsupported: " + listener.getClass().getName());
            }

            validator = TableUpdateValidator.make(this.sourceTable);
            final QueryTable validatorTable = validator.getResultTable();
            final ShiftAwareListener validatorTableListener =
                new InstrumentedShiftAwareListenerAdapter(validatorTable, false) {
                    @Override
                    public void onUpdate(Update upstream) {}

                    @Override
                    public void onFailureInternal(Throwable originalException,
                        UpdatePerformanceTracker.Entry sourceEntry) {
                        TestCase.fail(originalException.getMessage());
                    }
                };
            validatorTable.listenForUpdates(validatorTableListener);

            showWithIndex(sourceTable);
        }

        void modAndValidate(final Runnable modTable, final Index added, final Index removed,
            final Index modified) {
            modAndValidate(modTable, added, removed, modified, IndexShiftData.EMPTY);
        }

        void modAndValidate(final Runnable modTable, final Index added, final Index removed,
            final Index modified,
            final IndexShiftData shifted) {
            ++updateCount;

            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(modTable::run);
            showWithIndex(sourceTable);

            if (listener instanceof SimpleListener) {
                Assert.assertEquals(0, shifted.size());
                validate((SimpleListener) listener, updateCount, added, removed, modified);
            } else if (listener instanceof SimpleShiftAwareListener) {
                validate((SimpleShiftAwareListener) listener, updateCount, added, removed, modified,
                    shifted);
            }
        }
    }

    private static Index indexByRange(long firstKey, long lastKey) {
        return Index.FACTORY.getIndexByRange(firstKey, lastKey);
    }

    private static IndexShiftData shiftDataByValues(long... values) {
        if (values.length % 3 != 0) {
            throw new IllegalArgumentException(
                "shift data is defined by triplets {start, end, shift}");
        }
        IndexShiftData.Builder builder = new IndexShiftData.Builder();
        for (int idx = 0; idx < values.length; idx += 3) {
            builder.shiftRange(values[idx], values[idx + 1], values[idx + 2]);
        }
        return builder.build();
    }

    private static void validate(final SimpleListener listener, final long count, final Index added,
        final Index removed, final Index modified) {
        Assert.assertEquals("simpleListener.getCount()", count, listener.getCount());
        Assert.assertEquals("simpleListener.added", added, listener.added);
        Assert.assertEquals("simpleListener.removed", removed, listener.removed);
        Assert.assertEquals("simpleListener.modified", modified, listener.modified);
    }

    private static void validate(final SimpleShiftAwareListener listener, final long count,
        final Index added,
        final Index removed, final Index modified, final IndexShiftData shifted) {
        Assert.assertEquals("simpleListener.getCount()", count, listener.getCount());
        Assert.assertEquals("simpleListener.added", added, listener.update.added);
        Assert.assertEquals("simpleListener.removed", removed, listener.update.removed);
        Assert.assertEquals("simpleListener.modified", modified, listener.update.modified);
        Assert.assertEquals("simpleListener.shifted", shifted, listener.update.shifted);
    }

    /**
     * Makes sure that the index of our table is actually contiguous.
     */
    protected static class FlatChecker implements EvalNuggetInterface {
        private Table t1;

        FlatChecker(Table t1) {
            this.t1 = t1;
        }

        @Override
        public void validate(String msg) {
            if (t1.getIndex().size() > 0) {
                Assert.assertEquals(msg, t1.getIndex().size() - 1, t1.getIndex().lastKey());
            }
        }

        @Override
        public void show() throws IOException {
            io.deephaven.db.tables.utils.TableTools.showWithIndex(t1);
        }
    }

    public void testFlattenFollowedBySumBy() {
        // TODO: Write a test that just makes a RedirectedColumnSource with a wrapper, and
        // fill/query it.
        final QueryTable upstream = TstUtils.testRefreshingTable(ir(0, 100_000));
        final Table input = upstream.updateView("A=ii", "B=ii % 1000", "C=ii % 2 == 0");
        final Table odds = input.where("!C");
        final Table expected = odds.sumBy("B");
        final Table actual = odds.flatten().sumBy("B");
        assertTableEquals(expected, actual);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(upstream, ir(100_001, 200_000));
            upstream.notifyListeners(ir(100_001, 200_000), i(), ir(0, 100_000));
        });
        assertTableEquals(expected, actual);
    }
}
