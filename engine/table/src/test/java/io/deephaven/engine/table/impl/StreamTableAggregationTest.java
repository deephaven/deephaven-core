/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.SortedBy;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import junit.framework.ComparisonFailure;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static io.deephaven.api.agg.Aggregation.*;

/**
 * Unit tests that exercise special aggregation semantics for stream tables.
 */
public class StreamTableAggregationTest {

    private static final long INPUT_SIZE = 100_000L;
    private static final long MAX_RANDOM_ITERATION_SIZE = 10_000;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private Table source;

    @Before
    public void setUp() {
        source = TableCreatorImpl.create(EmptyTable.of(INPUT_SIZE)
                .update("Sym = Long.toString(ii % 1000) + `_Sym`")
                .update("Price = ii / 100 - (ii % 100)")
                .update("Size = (long) (ii / 50 - (ii % 50))"));
    }

    /**
     * Execute a table operator ending in an aggregation.
     *
     * @param operator The operator to apply
     * @param windowed Whether the stream table RowSet should be a sliding window (if {@code true}) or zero-based (if
     *        {@code false})
     */
    private void doOperatorTest(@NotNull final UnaryOperator<Table> operator, final boolean windowed) {
        final QueryTable normal = new QueryTable(RowSetFactory.empty().toTracking(),
                source.getColumnSourceMap());
        normal.setRefreshing(true);

        final QueryTable addOnly = normal.copy();
        addOnly.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);

        final TrackingWritableRowSet streamInternalRowSet;
        final Map<String, ? extends ColumnSource<?>> streamSources;
        if (windowed) {
            streamInternalRowSet = null;
            streamSources = source.getColumnSourceMap();
        } else {
            // Redirecting so we can present a zero-based RowSet from the stream table
            streamInternalRowSet = RowSetFactory.empty().toTracking();
            final RowRedirection streamRedirections = new WrappedRowSetRowRedirection(streamInternalRowSet);
            streamSources = source.getColumnSourceMap().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    (entry -> RedirectedColumnSource.maybeRedirect(streamRedirections, entry.getValue())),
                    Assert::neverInvoked,
                    LinkedHashMap::new));
        }
        final QueryTable stream = new QueryTable(RowSetFactory.empty().toTracking(), streamSources);
        stream.setRefreshing(true);
        stream.setAttribute(Table.STREAM_TABLE_ATTRIBUTE, true);

        TstUtils.assertTableEquals(normal, stream);

        final Table expected = operator.apply(normal);
        final Table addOnlyExpected = operator.apply(addOnly);
        final Table streamExpected = operator.apply(stream);
        TstUtils.assertTableEquals(expected, addOnlyExpected);
        TstUtils.assertTableEquals(expected, streamExpected);
        TestCase.assertFalse(((BaseTable<?>) streamExpected).isStream()); // Aggregation results are never stream tables

        final PrimitiveIterator.OfLong refreshSizes = LongStream.concat(
                LongStream.of(100, 0, 1, 2, 50, 0, 1000, 1, 0),
                new Random().longs(0, MAX_RANDOM_ITERATION_SIZE)).iterator();

        int step = 0;
        long usedSize = 0;
        RowSet streamLastInserted = RowSetFactory.empty();
        while (usedSize < INPUT_SIZE) {
            final long refreshSize = Math.min(INPUT_SIZE - usedSize, refreshSizes.nextLong());
            final RowSet normalStepInserted = refreshSize == 0
                    ? RowSetFactory.empty()
                    : RowSetFactory.fromRange(usedSize, usedSize + refreshSize - 1);
            final RowSet streamStepInserted = streamInternalRowSet == null ? normalStepInserted.copy()
                    : refreshSize == 0
                            ? RowSetFactory.empty()
                            : RowSetFactory.fromRange(0, refreshSize - 1);

            UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
            try {
                UpdateGraphProcessor.DEFAULT.refreshUpdateSourceForUnitTests(() -> {
                    if (normalStepInserted.isNonempty()) {
                        normal.getRowSet().writableCast().insert(normalStepInserted);
                        normal.notifyListeners(
                                new TableUpdateImpl(normalStepInserted, RowSetFactory.empty(),
                                        RowSetFactory.empty(), RowSetShiftData.EMPTY,
                                        ModifiedColumnSet.EMPTY));
                    }
                });
                final RowSet finalStreamLastInserted = streamLastInserted;
                UpdateGraphProcessor.DEFAULT.refreshUpdateSourceForUnitTests(() -> {
                    if (streamStepInserted.isNonempty() || finalStreamLastInserted.isNonempty()) {
                        if (streamInternalRowSet != null) {
                            streamInternalRowSet.clear();
                            streamInternalRowSet.insert(normalStepInserted);
                        }
                        stream.getRowSet().writableCast().clear();
                        stream.getRowSet().writableCast().insert(streamStepInserted);
                        stream.notifyListeners(new TableUpdateImpl(streamStepInserted.copy(), finalStreamLastInserted,
                                RowSetFactory.empty(), RowSetShiftData.EMPTY,
                                ModifiedColumnSet.EMPTY));
                    }
                });
            } finally {
                UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
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
        doOperatorTest(table -> table.aggBy(List.of(
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
        doOperatorTest(table -> table.aggBy(List.of(
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
        doOperatorTest(table -> table.aggBy(List.of(
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
        doOperatorTest(table -> table.aggBy(List.of(
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
