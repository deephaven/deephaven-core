//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Unit tests that exercise optimized operations for blink tables.
 */
public class BlinkTableOperationsTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final long INPUT_SIZE = 100_000L;
    private static final long MAX_RANDOM_ITERATION_SIZE = 10_000;

    private Table source;

    @Before
    public void setUp() {
        source = TableCreatorImpl.create(EmptyTable.of(INPUT_SIZE)
                .update("Sym = Long.toString(ii % 1000) + `_Sym`")
                .update("Price = ii / 100 - (ii % 100)")
                .update("Size = (long) (ii / 50 - (ii % 50))"));
    }

    /**
     * Execute a table operator.
     *
     * @param operator The operator to apply
     * @param windowed Whether the blink table RowSet should be a sliding window (if {@code true}) or zero-based (if
     *        {@code false})
     * @param expectBlinkResult Whether the result is expected to be a blink table
     */
    private void doOperatorTest(
            @NotNull final UnaryOperator<Table> operator,
            final boolean windowed,
            final boolean expectBlinkResult) {
        final QueryTable normal = new QueryTable(RowSetFactory.empty().toTracking(),
                source.getColumnSourceMap());
        normal.setRefreshing(true);

        final TrackingWritableRowSet blinkInternalRowSet;
        final Map<String, ? extends ColumnSource<?>> blinkSources;
        if (windowed) {
            blinkInternalRowSet = null;
            blinkSources = source.getColumnSourceMap();
        } else {
            // Redirecting so we can present a zero-based RowSet from the blink table
            blinkInternalRowSet = RowSetFactory.empty().toTracking();
            final RowRedirection blinkRedirections = new WrappedRowSetRowRedirection(blinkInternalRowSet);
            blinkSources = source.getColumnSourceMap().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    (entry -> RedirectedColumnSource.maybeRedirect(blinkRedirections, entry.getValue())),
                    Assert::neverInvoked,
                    LinkedHashMap::new));
        }
        final QueryTable blink = new QueryTable(RowSetFactory.empty().toTracking(), blinkSources);
        blink.setRefreshing(true);
        blink.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);

        TstUtils.assertTableEquals(normal, blink);

        final Table expected = operator.apply(normal);
        final Table blinkExpected = operator.apply(blink);
        TstUtils.assertTableEquals(expected, blinkExpected);
        TestCase.assertEquals(expectBlinkResult, ((BaseTable<?>) blinkExpected).isBlink());

        final PrimitiveIterator.OfLong refreshSizes = LongStream.concat(
                LongStream.of(100, 0, 1, 2, 50, 0, 1000, 1, 0),
                new Random().longs(0, MAX_RANDOM_ITERATION_SIZE)).iterator();

        int step = 0;
        long usedSize = 0;
        RowSet normalLastInserted = RowSetFactory.empty();
        RowSet blinkLastInserted = RowSetFactory.empty();
        while (usedSize < INPUT_SIZE) {
            final long refreshSize = Math.min(INPUT_SIZE - usedSize, refreshSizes.nextLong());
            final RowSet normalStepInserted = refreshSize == 0
                    ? RowSetFactory.empty()
                    : RowSetFactory.fromRange(usedSize, usedSize + refreshSize - 1);
            final RowSet blinkStepInserted = blinkInternalRowSet == null ? normalStepInserted.copy()
                    : refreshSize == 0
                            ? RowSetFactory.empty()
                            : RowSetFactory.fromRange(0, refreshSize - 1);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.startCycleForUnitTests();
            try {
                final RowSet finalNormalLastInserted = normalLastInserted;
                updateGraph.refreshUpdateSourceForUnitTests(() -> {
                    if (normalStepInserted.isNonempty() || finalNormalLastInserted.isNonempty()) {
                        normal.getRowSet().writableCast().update(normalStepInserted, finalNormalLastInserted);
                        normal.notifyListeners(new TableUpdateImpl(normalStepInserted.copy(), finalNormalLastInserted,
                                RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
                final RowSet finalBlinkLastInserted = blinkLastInserted;
                updateGraph.refreshUpdateSourceForUnitTests(() -> {
                    if (blinkStepInserted.isNonempty() || finalBlinkLastInserted.isNonempty()) {
                        if (blinkInternalRowSet != null) {
                            blinkInternalRowSet.clear();
                            blinkInternalRowSet.insert(normalStepInserted);
                        }
                        blink.getRowSet().writableCast().clear();
                        blink.getRowSet().writableCast().insert(blinkStepInserted);
                        blink.notifyListeners(new TableUpdateImpl(blinkStepInserted.copy(), finalBlinkLastInserted,
                                RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
            } finally {
                updateGraph.completeCycleForUnitTests();
            }
            try {
                TstUtils.assertTableEquals(expected, blinkExpected);
            } catch (ComparisonFailure e) {
                System.err.printf("FAILURE: step %d, previousUsedSize %d, refreshSize %d%n", step, usedSize,
                        refreshSize);
                throw e;
            }

            ++step;
            usedSize += refreshSize;
            normalLastInserted = normalStepInserted;
            blinkLastInserted = blinkStepInserted;
        }
    }

    @Test
    public void testSortOneColumn() {
        doOperatorTest(table -> table.sort("Sym"), false, true);
    }

    @Test
    public void testSortMultipleColumns() {
        doOperatorTest(table -> table.sort("Price", "Sym"), false, true);
    }

    @Test
    public void testSortOneColumnWindowed() {
        doOperatorTest(table -> table.sort("Sym"), true, true);
    }

    @Test
    public void testSortMultipleColumnsWindowed() {
        doOperatorTest(table -> table.sort("Price", "Sym"), true, true);
    }
}
