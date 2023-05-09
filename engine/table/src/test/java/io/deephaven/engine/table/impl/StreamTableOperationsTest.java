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
 * Unit tests that exercise optimized operations for stream tables.
 */
public class StreamTableOperationsTest {

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
     * @param windowed Whether the stream table RowSet should be a sliding window (if {@code true}) or zero-based (if
     *        {@code false})
     * @param expectStreamResult Whether the result is expected to be a stream table
     */
    private void doOperatorTest(
            @NotNull final UnaryOperator<Table> operator,
            final boolean windowed,
            final boolean expectStreamResult) {
        final QueryTable normal = new QueryTable(RowSetFactory.empty().toTracking(),
                source.getColumnSourceMap());
        normal.setRefreshing(true);

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
        final Table streamExpected = operator.apply(stream);
        TstUtils.assertTableEquals(expected, streamExpected);
        TestCase.assertEquals(expectStreamResult, ((BaseTable<?>) streamExpected).isStream());

        final PrimitiveIterator.OfLong refreshSizes = LongStream.concat(
                LongStream.of(100, 0, 1, 2, 50, 0, 1000, 1, 0),
                new Random().longs(0, MAX_RANDOM_ITERATION_SIZE)).iterator();

        int step = 0;
        long usedSize = 0;
        RowSet normalLastInserted = RowSetFactory.empty();
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
                final RowSet finalNormalLastInserted = normalLastInserted;
                UpdateGraphProcessor.DEFAULT.refreshUpdateSourceForUnitTests(() -> {
                    if (normalStepInserted.isNonempty() || finalNormalLastInserted.isNonempty()) {
                        normal.getRowSet().writableCast().update(normalStepInserted, finalNormalLastInserted);
                        normal.notifyListeners(new TableUpdateImpl(normalStepInserted.copy(), finalNormalLastInserted,
                                RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
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
                                RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
                    }
                });
            } finally {
                UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
            }
            try {
                TstUtils.assertTableEquals(expected, streamExpected);
            } catch (ComparisonFailure e) {
                System.err.printf("FAILURE: step %d, previousUsedSize %d, refreshSize %d%n", step, usedSize,
                        refreshSize);
                throw e;
            }

            ++step;
            usedSize += refreshSize;
            normalLastInserted = normalStepInserted;
            streamLastInserted = streamStepInserted;
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
