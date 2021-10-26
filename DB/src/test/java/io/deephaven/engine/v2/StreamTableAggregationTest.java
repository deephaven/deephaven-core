package io.deephaven.engine.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.util.SortedBy;
import io.deephaven.engine.v2.Listener.Update;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.engine.v2.utils.*;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.test.junit4.EngineCleanup;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.deephaven.engine.v2.by.ComboAggregateFactory.*;

/**
 * Unit tests that exercise special aggregation semantics for stream tables.
 */
public class StreamTableAggregationTest {

    private static final long INPUT_SIZE = 100_000L;
    private static final long MAX_RANDOM_ITERATION_SIZE = 10_000;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private final Table source = Table.of(EmptyTable.of(INPUT_SIZE)
            .update("Sym = Long.toString(ii % 1000) + `_Sym`")
            .update("Price = ii / 100 - (ii % 100)")
            .update("Size = (long) (ii / 50 - (ii % 50))"));

    /**
     * Execute a table operator ending in an aggregation.
     *
     * @param operator The operator to apply
     * @param windowed Whether the stream table rowSet should be a sliding window (if {@code true}) or zero-based (if
     *        {@code false})
     */
    private void doOperatorTest(@NotNull final UnaryOperator<Table> operator, final boolean windowed) {
        final QueryTable normal = new QueryTable(RowSetFactoryImpl.INSTANCE.getEmptyRowSet(), source.getColumnSourceMap());
        normal.setRefreshing(true);

        final QueryTable addOnly = (QueryTable) normal.copy();
        addOnly.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);

        final TrackingMutableRowSet streamInternalRowSet;
        final Map<String, ? extends ColumnSource<?>> streamSources;
        if (windowed) {
            streamInternalRowSet = null;
            streamSources = source.getColumnSourceMap();
        } else {
            // Redirecting so we can present a zero-based TrackingMutableRowSet from the stream table
            streamInternalRowSet = RowSetFactoryImpl.INSTANCE.getEmptyRowSet();
            final RedirectionIndex streamRedirections = new WrappedIndexRedirectionIndexImpl(streamInternalRowSet);
            streamSources = source.getColumnSourceMap().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    (entry -> new ReadOnlyRedirectedColumnSource<>(streamRedirections, entry.getValue())),
                    Assert::neverInvoked,
                    LinkedHashMap::new));
        }
        final QueryTable stream = new QueryTable(RowSetFactoryImpl.INSTANCE.getEmptyRowSet(), streamSources);
        stream.setRefreshing(true);
        stream.setAttribute(Table.STREAM_TABLE_ATTRIBUTE, true);

        TstUtils.assertTableEquals(normal, stream);

        final Table expected = operator.apply(normal);
        final Table addOnlyExpected = operator.apply(addOnly);
        final Table streamExpected = operator.apply(stream);
        TstUtils.assertTableEquals(expected, addOnlyExpected);
        TstUtils.assertTableEquals(expected, streamExpected);
        TestCase.assertFalse(((BaseTable) streamExpected).isStream()); // Aggregation results are never stream tables

        final PrimitiveIterator.OfLong refreshSizes = LongStream.concat(
                LongStream.of(100, 0, 1, 2, 50, 0, 1000, 1, 0),
                new Random().longs(0, MAX_RANDOM_ITERATION_SIZE)).iterator();

        int step = 0;
        long usedSize = 0;
        RowSet streamLastInserted = RowSetFactoryImpl.INSTANCE.getEmptyRowSet();
        while (usedSize < INPUT_SIZE) {
            final long refreshSize = Math.min(INPUT_SIZE - usedSize, refreshSizes.nextLong());
            final RowSet normalStepInserted = refreshSize == 0
                    ? RowSetFactoryImpl.INSTANCE.getEmptyRowSet()
                    : RowSetFactoryImpl.INSTANCE.getRowSetByRange(usedSize, usedSize + refreshSize - 1);
            final RowSet streamStepInserted = streamInternalRowSet == null ? normalStepInserted
                    : refreshSize == 0
                            ? RowSetFactoryImpl.INSTANCE.getEmptyRowSet()
                            : RowSetFactoryImpl.INSTANCE.getRowSetByRange(0, refreshSize - 1);

            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            try {
                LiveTableMonitor.DEFAULT.refreshLiveTableForUnitTests(() -> {
                    if (normalStepInserted.isNonempty()) {
                        normal.getRowSet().insert(normalStepInserted);
                        normal.notifyListeners(new Update(normalStepInserted, RowSetFactoryImpl.INSTANCE.getEmptyRowSet(),
                                RowSetFactoryImpl.INSTANCE.getEmptyRowSet(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
                final RowSet finalStreamLastInserted = streamLastInserted;
                LiveTableMonitor.DEFAULT.refreshLiveTableForUnitTests(() -> {
                    if (streamStepInserted.isNonempty() || finalStreamLastInserted.isNonempty()) {
                        if (streamInternalRowSet != null) {
                            streamInternalRowSet.clear();
                            streamInternalRowSet.insert(normalStepInserted);
                        }
                        stream.getRowSet().clear();
                        stream.getRowSet().insert(streamStepInserted);
                        stream.notifyListeners(new Update(streamStepInserted, finalStreamLastInserted,
                                RowSetFactoryImpl.INSTANCE.getEmptyRowSet(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
            } finally {
                LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            }
            try {
                TstUtils.assertTableEquals(expected, addOnlyExpected);
                TstUtils.assertTableEquals(expected, streamExpected);
            } catch (ComparisonFailure e) {
                System.err.printf("FAILURE: step %d, previousUsedSize %d, refreshSize %d%n", step, usedSize,
                        refreshSize);
                throw e;
            }

            ++step;
            usedSize += refreshSize;
            streamLastInserted = streamStepInserted;
        }
    }

    @Test
    public void testFirstBy() {
        doOperatorTest(table -> table.firstBy("Sym"), false);
    }

    @Test
    public void testFirstByNoKeys() {
        doOperatorTest(Table::firstBy, false);
    }

    @Test
    public void testFirstByWindowed() {
        doOperatorTest(table -> table.firstBy("Sym"), true);
    }

    @Test
    public void testFirstByNoKeysWindowed() {
        doOperatorTest(Table::firstBy, true);
    }

    @Test
    public void testLastBy() {
        doOperatorTest(table -> table.lastBy("Sym"), false);
    }

    @Test
    public void testLastByNoKeys() {
        doOperatorTest(Table::lastBy, false);
    }

    @Test
    public void testLastByWindowed() {
        doOperatorTest(table -> table.lastBy("Sym"), true);
    }

    @Test
    public void testLastByNoKeysWindowed() {
        doOperatorTest(Table::lastBy, true);
    }

    @Test
    public void testMinBy() {
        doOperatorTest(table -> table.minBy("Sym"), false);
    }

    @Test
    public void testMinByNoKeys() {
        doOperatorTest(Table::minBy, false);
    }

    @Test
    public void testMinByWindowed() {
        doOperatorTest(table -> table.minBy("Sym"), true);
    }

    @Test
    public void testMinByNoKeysWindowed() {
        doOperatorTest(Table::minBy, true);
    }

    @Test
    public void testMaxBy() {
        doOperatorTest(table -> table.maxBy("Sym"), false);
    }

    @Test
    public void testMaxByNoKeys() {
        doOperatorTest(Table::maxBy, false);
    }

    @Test
    public void testMaxByWindowed() {
        doOperatorTest(table -> table.maxBy("Sym"), true);
    }

    @Test
    public void testMaxByNoKeysWindowed() {
        doOperatorTest(Table::maxBy, true);
    }

    @Test
    public void testMedianBy() {
        doOperatorTest(table -> table.medianBy("Sym"), false);
    }

    @Test
    public void testMedianByNoKeys() {
        doOperatorTest(Table::medianBy, false);
    }

    @Test
    public void testMedianByWindowed() {
        doOperatorTest(table -> table.medianBy("Sym"), true);
    }

    @Test
    public void testMedianByNoKeysWindowed() {
        doOperatorTest(Table::medianBy, true);
    }

    @Test
    public void testSortedFirstBy() {
        doOperatorTest(table -> SortedBy.sortedFirstBy(table, "Price", "Sym"), false);
    }

    @Test
    public void testSortedFirstByNoKeys() {
        doOperatorTest(table -> SortedBy.sortedFirstBy(table, "Price"), false);
    }

    @Test
    public void testSortedFirstByWindowed() {
        doOperatorTest(table -> SortedBy.sortedFirstBy(table, "Price", "Sym"), true);
    }

    @Test
    public void testSortedFirstByNoKeysWindowed() {
        doOperatorTest(table -> SortedBy.sortedFirstBy(table, "Price"), true);
    }

    @Test
    public void testSortedLastBy() {
        doOperatorTest(table -> SortedBy.sortedLastBy(table, "Price", "Sym"), false);
    }

    @Test
    public void testSortedLastByNoKeys() {
        doOperatorTest(table -> SortedBy.sortedLastBy(table, "Price"), false);
    }

    @Test
    public void testSortedLastByWindowed() {
        doOperatorTest(table -> SortedBy.sortedLastBy(table, "Price", "Sym"), true);
    }

    @Test
    public void testSortedLastByNoKeysWindowed() {
        doOperatorTest(table -> SortedBy.sortedLastBy(table, "Price"), true);
    }

    @Test
    public void testSortedFirstByObject() {
        doOperatorTest(table -> SortedBy.sortedFirstBy(table, "Sym", "Price"), false);
    }

    @Test
    public void testSortedFirstByNoKeysObject() {
        doOperatorTest(table -> SortedBy.sortedFirstBy(table, "Sym"), false);
    }

    @Test
    public void testSortedFirstByWindowedObject() {
        doOperatorTest(table -> SortedBy.sortedFirstBy(table, "Sym", "Price"), true);
    }

    @Test
    public void testSortedFirstByNoKeysWindowedObject() {
        doOperatorTest(table -> SortedBy.sortedFirstBy(table, "Sym"), true);
    }

    @Test
    public void testSortedLastByObject() {
        doOperatorTest(table -> SortedBy.sortedLastBy(table, "Sym", "Price"), false);
    }

    @Test
    public void testSortedLastByNoKeysObject() {
        doOperatorTest(table -> SortedBy.sortedLastBy(table, "Sym"), false);
    }

    @Test
    public void testSortedLastByWindowedObject() {
        doOperatorTest(table -> SortedBy.sortedLastBy(table, "Sym", "Price"), true);
    }

    @Test
    public void testSortedLastByNoKeysWindowedObject() {
        doOperatorTest(table -> SortedBy.sortedLastBy(table, "Sym"), true);
    }

    @Test
    public void testComboBy() {
        doOperatorTest(table -> table.by(AggCombo(
                AggFirst("FirstPrice=Price", "FirstSize=Size"),
                AggLast("LastPrice=Price", "LastSize=Size"),
                AggMin("MinPrice=Price", "MinSize=Size"),
                AggMax("MaxPrice=Price", "MaxSize=Size"),
                AggMed("MedPrice=Price", "MedSize=Size"),
                AggSortedFirst("Price", "PriceSortedFirstSym=Sym", "PriceSortedFirstSize=Size"),
                AggSortedLast("Price", "PriceSortedLastSym=Sym", "PriceSortedLastSize=Size"),
                AggSortedFirst("Sym", "SymSortedFirstPrice=Price", "SymSortedFirstSize=Size"),
                AggSortedLast("Sym", "SymSortedLastPrice=Price", "SymSortedLastSize=Size")), "Sym"), false);
    }

    @Test
    public void testComboByNoKeys() {
        doOperatorTest(table -> table.by(AggCombo(
                AggFirst("FirstSym=Sym", "FirstPrice=Price", "FirstSize=Size"),
                AggLast("LastSym=Sym", "LastPrice=Price", "LastSize=Size"),
                AggMin("MinSym=Sym", "MinPrice=Price", "MinSize=Size"),
                AggMax("MaxSym=Sym", "MaxPrice=Price", "MaxSize=Size"),
                AggMed("MedSym=Sym", "MedPrice=Price", "MedSize=Size"),
                AggSortedFirst("Price", "PriceSortedFirstSym=Sym", "PriceSortedFirstSize=Size"),
                AggSortedLast("Price", "PriceSortedLastSym=Sym", "PriceSortedLastSize=Size"),
                AggSortedFirst("Sym", "SymSortedFirstPrice=Price", "SymSortedFirstSize=Size"),
                AggSortedLast("Sym", "SymSortedLastPrice=Price", "SymSortedLastSize=Size"))), false);
    }

    @Test
    public void testComboByWindowed() {
        doOperatorTest(table -> table.by(AggCombo(
                AggFirst("FirstPrice=Price", "FirstSize=Size"),
                AggLast("LastPrice=Price", "LastSize=Size"),
                AggMin("MinPrice=Price", "MinSize=Size"),
                AggMax("MaxPrice=Price", "MaxSize=Size"),
                AggMed("MedPrice=Price", "MedSize=Size"),
                AggSortedFirst("Price", "PriceSortedFirstSym=Sym", "PriceSortedFirstSize=Size"),
                AggSortedLast("Price", "PriceSortedLastSym=Sym", "PriceSortedLastSize=Size"),
                AggSortedFirst("Sym", "SymSortedFirstPrice=Price", "SymSortedFirstSize=Size"),
                AggSortedLast("Sym", "SymSortedLastPrice=Price", "SymSortedLastSize=Size")), "Sym"), true);
    }

    @Test
    public void testComboByNoKeysWindowed() {
        doOperatorTest(table -> table.by(AggCombo(
                AggFirst("FirstSym=Sym", "FirstPrice=Price", "FirstSize=Size"),
                AggLast("LastSym=Sym", "LastPrice=Price", "LastSize=Size"),
                AggMin("MinSym=Sym", "MinPrice=Price", "MinSize=Size"),
                AggMax("MaxSym=Sym", "MaxPrice=Price", "MaxSize=Size"),
                AggMed("MedSym=Sym", "MedPrice=Price", "MedSize=Size"),
                AggSortedFirst("Price", "PriceSortedFirstSym=Sym", "PriceSortedFirstSize=Size"),
                AggSortedLast("Price", "PriceSortedLastSym=Sym", "PriceSortedLastSize=Size"),
                AggSortedFirst("Sym", "SymSortedFirstPrice=Price", "SymSortedFirstSize=Size"),
                AggSortedLast("Sym", "SymSortedLastPrice=Price", "SymSortedLastSize=Size"))), true);
    }
}
