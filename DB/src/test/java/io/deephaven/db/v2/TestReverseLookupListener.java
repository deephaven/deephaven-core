package io.deephaven.db.v2;

import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import gnu.trove.iterator.TObjectLongIterator;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static io.deephaven.db.v2.TstUtils.*;

public class TestReverseLookupListener extends LiveTableTestCase {
    public void testSimple() {
        final BaseTable source = TstUtils.testRefreshingTable(
            i(2, 4, 6, 8),
            TstUtils.c("Sentinel", "A", "B", "C", "D"),
            TstUtils.c("Sentinel2", "H", "I", "J", "K"));
        io.deephaven.db.tables.utils.TableTools.show(source);

        final ReverseLookupListener reverseLookupListener =
            ReverseLookupListener.makeReverseLookupListenerWithSnapshot(source, "Sentinel");

        assertEquals(2, reverseLookupListener.get("A"));
        assertEquals(4, reverseLookupListener.get("B"));
        assertEquals(6, reverseLookupListener.get("C"));
        assertEquals(8, reverseLookupListener.get("D"));
        assertEquals(reverseLookupListener.getNoEntryValue(), reverseLookupListener.get("E"));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index keysToModify = Index.FACTORY.getIndexByValues(4);
            TstUtils.addToTable(source, keysToModify, TstUtils.c("Sentinel", "E"),
                TstUtils.c("Sentinel2", "L"));
            source.notifyListeners(i(), i(), keysToModify);
        });

        assertEquals(2, reverseLookupListener.get("A"));
        assertEquals(4, reverseLookupListener.get("E"));
        assertEquals(6, reverseLookupListener.get("C"));
        assertEquals(8, reverseLookupListener.get("D"));
        assertEquals(reverseLookupListener.getNoEntryValue(), reverseLookupListener.get("B"));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index keysToSwap = Index.FACTORY.getIndexByValues(4, 6);
            TstUtils.addToTable(source, keysToSwap, TstUtils.c("Sentinel", "C", "E"),
                TstUtils.c("Sentinel2", "M", "N"));
            source.notifyListeners(i(), i(), keysToSwap);
        });

        TableTools.showWithIndex(source);

        assertEquals(2, reverseLookupListener.get("A"));
        assertEquals(4, reverseLookupListener.get("C"));
        assertEquals(6, reverseLookupListener.get("E"));
        assertEquals(8, reverseLookupListener.get("D"));
        assertEquals(reverseLookupListener.getNoEntryValue(), reverseLookupListener.get("B"));
    }

    private static class ReverseLookupEvalNugget implements EvalNuggetInterface {
        private final ReverseLookupListener listener;
        private final DynamicTable source;
        private final ColumnSource[] columnSources;

        ReverseLookupEvalNugget(DynamicTable source, String... columns) {
            listener = ReverseLookupListener.makeReverseLookupListenerWithLock(source, columns);
            this.columnSources =
                Arrays.stream(columns).map(source::getColumnSource).toArray(ColumnSource[]::new);
            this.source = source;
        }

        @Override
        public void validate(String msg) {
            for (final TObjectLongIterator<Object> it = listener.iterator(); it.hasNext();) {
                it.advance();
                final Object checkKey = TableTools.getKey(columnSources, it.value());
                assertEquals(it.key(), checkKey);
            }

            final Map<Object, Long> currentMap = new HashMap<>();
            final Map<Object, Long> prevMap = new HashMap<>();

            for (final Index.Iterator it = source.getIndex().iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object expectedKey = TableTools.getKey(columnSources, row);
                final long checkRow = listener.get(expectedKey);
                if (row != checkRow) {
                    TestCase.fail("invalid row for " + expectedKey + " expected=" + row
                        + ", actual=" + checkRow);
                }
                currentMap.put(expectedKey, row);
            }

            for (final Index.Iterator it = source.getIndex().getPrevIndex().iterator(); it
                .hasNext();) {
                final long row = it.nextLong();
                final Object expectedKey = TableTools.getPrevKey(columnSources, row);
                final long checkRow = listener.getPrev(expectedKey);
                assertEquals(row, checkRow);
                prevMap.put(expectedKey, row);
            }


            final Index removedRows = source.getIndex().getPrevIndex().minus(source.getIndex());
            for (final Index.Iterator it = removedRows.iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object expectedKey = TableTools.getPrevKey(columnSources, row);
                final long checkRow = listener.get(expectedKey);

                final Long currentRow = currentMap.get(expectedKey);
                if (currentRow != null) {
                    assertEquals((long) currentRow, checkRow);
                } else {
                    assertEquals(listener.getNoEntryValue(), checkRow);
                }
            }
            final Index addedRows = source.getIndex().minus(source.getIndex().getPrevIndex());
            for (final Index.Iterator it = addedRows.iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object expectedKey = TableTools.getKey(columnSources, row);
                final long checkRow = listener.getPrev(expectedKey);

                final Long prevRow = prevMap.get(expectedKey);
                if (prevRow != null) {
                    assertEquals((long) prevRow, checkRow);
                } else {
                    assertEquals(listener.getNoEntryValue(), checkRow);
                }
            }
        }

        @Override
        public void show() {
            for (final TObjectLongIterator<Object> it = listener.iterator(); it.hasNext();) {
                it.advance();
                System.out.println(it.key() + "=" + it.value());
            }
        }
    }

    public void testIncremental() {
        for (int i = 0; i < 10; i++) {
            final Random random = new Random(i == 0 ? 0 : System.currentTimeMillis());

            final TstUtils.ColumnInfo[] columnInfo;
            final int size = 100;
            final QueryTable table = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"C1", "C2"},
                    new TstUtils.UniqueStringGenerator(),
                    new TstUtils.UniqueIntGenerator(1, 1000)));

            final EvalNuggetInterface en[] = LiveTableMonitor.DEFAULT.exclusiveLock()
                .computeLocked(() -> new EvalNuggetInterface[] {
                        new ReverseLookupEvalNugget(table, "C1"),
                        new ReverseLookupEvalNugget(table, "C2"),
                        new ReverseLookupEvalNugget(table, "C1", "C2")
                });

            final int updateSize = (int) Math.ceil(Math.sqrt(size));
            for (int step = 0; step < 100; ++step) {
                if (LiveTableTestCase.printTableUpdates) {
                    System.out.println("step = " + step);
                }
                simulateShiftAwareStep(updateSize, random, table, columnInfo, en);
            }
        }
    }
}
