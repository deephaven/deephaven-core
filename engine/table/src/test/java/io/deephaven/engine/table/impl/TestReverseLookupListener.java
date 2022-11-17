/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.UniqueIntGenerator;
import io.deephaven.engine.testutil.generator.UniqueStringGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import gnu.trove.iterator.TObjectLongIterator;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.*;

public class TestReverseLookupListener extends RefreshingTableTestCase {
    public void testSimple() {
        final BaseTable source = TstUtils.testRefreshingTable(
                i(2, 4, 6, 8).toTracking(),
                TstUtils.c("Sentinel", "A", "B", "C", "D"),
                TstUtils.c("Sentinel2", "H", "I", "J", "K"));
        TableTools.show(source);

        final ReverseLookupListener reverseLookupListener =
                ReverseLookupListener.makeReverseLookupListenerWithSnapshot(source, "Sentinel");

        assertEquals(2, reverseLookupListener.get("A"));
        assertEquals(4, reverseLookupListener.get("B"));
        assertEquals(6, reverseLookupListener.get("C"));
        assertEquals(8, reverseLookupListener.get("D"));
        assertEquals(reverseLookupListener.noEntryValue(), reverseLookupListener.get("E"));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final RowSet keysToModify = RowSetFactory.fromKeys(4);
            TstUtils.addToTable(source, keysToModify, TstUtils.c("Sentinel", "E"), TstUtils.c("Sentinel2", "L"));
            source.notifyListeners(i(), i(), keysToModify);
        });

        assertEquals(2, reverseLookupListener.get("A"));
        assertEquals(4, reverseLookupListener.get("E"));
        assertEquals(6, reverseLookupListener.get("C"));
        assertEquals(8, reverseLookupListener.get("D"));
        assertEquals(reverseLookupListener.noEntryValue(), reverseLookupListener.get("B"));

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
            final RowSet keysToSwap = RowSetFactory.fromKeys(4, 6);
            TstUtils.addToTable(source, keysToSwap, TstUtils.c("Sentinel", "C", "E"),
                    TstUtils.c("Sentinel2", "M", "N"));
            source.notifyListeners(i(), i(), keysToSwap);
        });

        TableTools.showWithRowSet(source);

        assertEquals(2, reverseLookupListener.get("A"));
        assertEquals(4, reverseLookupListener.get("C"));
        assertEquals(6, reverseLookupListener.get("E"));
        assertEquals(8, reverseLookupListener.get("D"));
        assertEquals(reverseLookupListener.noEntryValue(), reverseLookupListener.get("B"));
    }

    private static class ReverseLookupEvalNugget implements EvalNuggetInterface {
        private final ReverseLookupListener listener;
        private final Table source;
        private final ColumnSource[] columnSources;

        ReverseLookupEvalNugget(Table source, String... columns) {
            listener = ReverseLookupListener.makeReverseLookupListenerWithLock(source, columns);
            this.columnSources = Arrays.stream(columns).map(source::getColumnSource).toArray(ColumnSource[]::new);
            this.source = source;
        }

        @Override
        public void validate(String msg) {
            for (final TObjectLongIterator<Object> it = listener.iterator(); it.hasNext();) {
                it.advance();
                final Object checkKey = ReverseLookupListener.getKey(columnSources, it.value());
                assertEquals(it.key(), checkKey);
            }

            final Map<Object, Long> currentMap = new HashMap<>();
            final Map<Object, Long> prevMap = new HashMap<>();

            for (final RowSet.Iterator it = source.getRowSet().iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object expectedKey = ReverseLookupListener.getKey(columnSources, row);
                final long checkRow = listener.get(expectedKey);
                if (row != checkRow) {
                    TestCase.fail("invalid row for " + expectedKey + " expected=" + row + ", actual=" + checkRow);
                }
                currentMap.put(expectedKey, row);
            }

            for (final RowSet.Iterator it = source.getRowSet().copyPrev().iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object expectedKey = ReverseLookupListener.getPrevKey(columnSources, row);
                final long checkRow = listener.getPrev(expectedKey);
                assertEquals(row, checkRow);
                prevMap.put(expectedKey, row);
            }


            final RowSet removedRows = source.getRowSet().copyPrev().minus(source.getRowSet());
            for (final RowSet.Iterator it = removedRows.iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object expectedKey = ReverseLookupListener.getPrevKey(columnSources, row);
                final long checkRow = listener.get(expectedKey);

                final Long currentRow = currentMap.get(expectedKey);
                if (currentRow != null) {
                    assertEquals((long) currentRow, checkRow);
                } else {
                    assertEquals(listener.noEntryValue(), checkRow);
                }
            }
            final RowSet addedRows = source.getRowSet().minus(source.getRowSet().copyPrev());
            for (final RowSet.Iterator it = addedRows.iterator(); it.hasNext();) {
                final long row = it.nextLong();
                final Object expectedKey = ReverseLookupListener.getKey(columnSources, row);
                final long checkRow = listener.getPrev(expectedKey);

                final Long prevRow = prevMap.get(expectedKey);
                if (prevRow != null) {
                    assertEquals((long) prevRow, checkRow);
                } else {
                    assertEquals(listener.noEntryValue(), checkRow);
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

            final ColumnInfo[] columnInfo;
            final int size = 100;
            final QueryTable table = getTable(size, random, columnInfo = initColumnInfos(new String[] {"C1", "C2"},
                    new UniqueStringGenerator(),
                    new UniqueIntGenerator(1, 1000)));

            final EvalNuggetInterface en[] = UpdateGraphProcessor.DEFAULT.exclusiveLock()
                    .computeLocked(() -> new EvalNuggetInterface[] {
                            new ReverseLookupEvalNugget(table, "C1"),
                            new ReverseLookupEvalNugget(table, "C2"),
                            new ReverseLookupEvalNugget(table, "C1", "C2")
                    });

            final int updateSize = (int) Math.ceil(Math.sqrt(size));
            for (int step = 0; step < 100; ++step) {
                if (RefreshingTableTestCase.printTableUpdates) {
                    System.out.println("step = " + step);
                }
                simulateShiftAwareStep(updateSize, random, table, columnInfo, en);
            }
        }
    }
}
