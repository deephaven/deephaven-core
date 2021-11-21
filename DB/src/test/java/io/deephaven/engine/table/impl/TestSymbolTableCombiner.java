package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerSparseArraySource;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.table.impl.TstUtils.*;

@Category(OutOfBandTest.class)
public class TestSymbolTableCombiner extends RefreshingTableTestCase {
    public void testSymbolTableCombiner() {
        for (int seed = 0; seed < 3; ++seed) {
            testSymbolTableCombiner(seed);
        }
    }

    private void testSymbolTableCombiner(int seed) {
        final int size = 1000;
        final Random random = new Random(seed);

        final TstUtils.ColumnInfo[] columnInfo;
        final QueryTable symbolTable = getTable(size, random,
                columnInfo = initColumnInfos(
                        new String[] {SymbolTableSource.ID_COLUMN_NAME, SymbolTableSource.SYMBOL_COLUMN_NAME},
                        new TstUtils.UniqueLongGenerator(1, 10000000),
                        new TstUtils.StringGenerator(34000)));

        // noinspection unchecked
        final ColumnSource<String> symbolSource = symbolTable.getColumnSource(SymbolTableSource.SYMBOL_COLUMN_NAME);
        // noinspection unchecked
        final ColumnSource<Long> idSource = symbolTable.getColumnSource(SymbolTableSource.ID_COLUMN_NAME);
        final SymbolTableCombiner combiner = new SymbolTableCombiner(new ColumnSource[] {symbolSource}, 128);

        final IntegerSparseArraySource symbolMapper = new IntegerSparseArraySource();
        combiner.addSymbols(symbolTable, symbolMapper);

        final Map<String, Integer> uniqueIdMap = new HashMap<>();
        checkAdditions(symbolTable, symbolSource, idSource, symbolMapper, uniqueIdMap);

        final IntegerSparseArraySource symbolMapper2 = new IntegerSparseArraySource();
        combiner.lookupSymbols(symbolTable, symbolMapper2, -2);

        for (final RowSet.Iterator it = symbolTable.getRowSet().iterator(); it.hasNext();) {
            final long key = it.nextLong();
            final String symbol = symbolSource.get(key);
            final long id = idSource.getLong(key);

            final int uniqueId = symbolMapper2.get(id);
            final int expected = uniqueIdMap.get(symbol);
            assertEquals(expected, uniqueId);
        }

        final TableUpdateListener symbolTableListener =
                new InstrumentedTableUpdateListenerAdapter("SymbolTableCombiner Adapter", symbolTable, false) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {
                        assertIndexEquals(i(), upstream.removed());
                        assertIndexEquals(i(), upstream.modified());
                        assertTrue(upstream.shifted().empty());
                        combiner.addSymbols(symbolTable, upstream.added(), symbolMapper);
                        checkAdditions(symbolTable, symbolSource, idSource, symbolMapper, uniqueIdMap);
                    }

                    @Override
                    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                        originalException.printStackTrace();
                        TestCase.fail(originalException.getMessage());
                        super.onFailureInternal(originalException, sourceEntry);
                    }
                };
        symbolTable.listenForUpdates(symbolTableListener);

        for (int step = 0; step < 750; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + step + ", size=" + symbolTable.size());
            }
            UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(() -> {
                final RowSet[] updates = GenerateTableUpdates.computeTableUpdates(size / 10, random, symbolTable,
                        columnInfo, true, false, false);
                symbolTable.notifyListeners(updates[0], updates[1], updates[2]);
            });
        }
    }

    private static void checkAdditions(QueryTable symbolTable, ColumnSource<String> symbolSource,
            ColumnSource<Long> idSource, IntegerSparseArraySource symbolMapper, Map<String, Integer> uniqueIdMap) {
        for (final RowSet.Iterator it = symbolTable.getRowSet().iterator(); it.hasNext();) {
            final long key = it.nextLong();
            final String symbol = symbolSource.get(key);
            final long id = idSource.getLong(key);

            final int uniqueId = symbolMapper.get(id);
            final Integer old = uniqueIdMap.put(symbol, uniqueId);
            if (old != null && old != uniqueId) {
                throw new IllegalStateException("Inconsistent IDs for " + symbol + ", found " + uniqueId
                        + " previous value was " + old + ", row=" + key);
            }
        }
    }
}
