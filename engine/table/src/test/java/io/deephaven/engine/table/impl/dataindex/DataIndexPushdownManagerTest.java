//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.Pair;
import io.deephaven.api.RawString;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.DataIndexOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BasePushdownFilterContextImpl;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterMatcher;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DataIndexPushdownManager}.
 */
public class DataIndexPushdownManagerTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final int KEYS = 100;
    /**
     * Twice the rows per key at which the index threshold is reached, so that a full selection uses the index and the
     * threshold falls inside the table.
     */
    private static final int ROWS_PER_KEY = 2 * (int) Math.ceil(1 / QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD);

    private static Table indexedTable() {
        return TableTools.emptyTable((long) KEYS * ROWS_PER_KEY).update("A = ii % " + KEYS);
    }

    private static WhereFilter initializedFilter(final Table table, final String expression) {
        final WhereFilter filter = WhereFilter.of(RawString.of(expression));
        filter.init(table.getDefinition());
        return filter;
    }

    /**
     * A matcher that hands out contexts we can inspect; it never estimates or executes anything.
     */
    private static final class RecordingMatcher implements PushdownFilterMatcher {
        final AtomicReference<PushdownFilterContext> lastContext = new AtomicReference<>();

        @Override
        public PushdownFilterContext makePushdownFilterContext(
                final WhereFilter filter,
                final List<ColumnSource<?>> filterSources) {
            final PushdownFilterContext context = new BasePushdownFilterContextImpl(filter, filterSources);
            lastContext.set(context);
            return context;
        }
    }

    /** An {@link IntegerArraySource} whose pushdown contexts record whether they were closed. */
    private static final class CloseTrackingSource extends IntegerArraySource {
        CloseTrackingContext lastContext;

        @Override
        public PushdownFilterContext makePushdownFilterContext(
                final WhereFilter filter,
                final List<ColumnSource<?>> filterSources) {
            return lastContext = new CloseTrackingContext(filter, filterSources);
        }
    }

    private static final class CloseTrackingContext extends BasePushdownFilterContextImpl {
        boolean closed;

        CloseTrackingContext(final WhereFilter filter, final List<ColumnSource<?>> columnSources) {
            super(filter, columnSources);
        }

        @Override
        public void close() {
            closed = true;
            super.close();
        }
    }

    private static <T extends IntegerArraySource> T fill(final T source, final int size) {
        source.ensureCapacity(size, false);
        for (int ii = 0; ii < size; ii++) {
            source.set(ii, ii % 3);
        }
        return source;
    }

    /**
     * The wrapped matcher's context is created before the data-index context that will own it. If the outer
     * construction fails, nothing owns the wrapped context yet, so the factory must close it rather than leak it.
     */
    @Test
    public void testWrappedContextClosedWhenOuterConstructionFails() {
        final int size = 10;
        final CloseTrackingSource filterSource = fill(new CloseTrackingSource(), size);
        final IntegerArraySource indexedSource = fill(new IntegerArraySource(), size);
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(),
                Map.of("X", filterSource, "A", indexedSource));

        // The index is on A, but the filter is on X: the data-index context's constructor rejects a filter source it
        // has no index column for. WhereListener never pairs them this way, so this is the broken-invariant path.
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final WhereFilter filter = WhereFilter.of(RawString.of("X > 1"));
        filter.init(table.getDefinition());

        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, filterSource);
        assertThrows(IllegalStateException.class,
                () -> matcher.makePushdownFilterContext(filter, List.of(filterSource)));

        assertNotNull("sanity: the wrapped matcher built a context", filterSource.lastContext);
        assertTrue("the wrapped context must be closed when the outer construction fails",
                filterSource.lastContext.closed);
    }

    /**
     * The driver records the cost ceiling a pushdown ran under on the context it handed out. The data index context
     * must pass that on to the wrapped matcher's context, which is the one that consults it.
     */
    @Test
    public void executedFilterCostReachesWrappedContext() {
        final Table table = indexedTable();
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final WhereFilter filter = initializedFilter(table, "A == 5");
        final List<ColumnSource<?>> sources = List.of(table.getColumnSource("A"));

        final RecordingMatcher wrapped = new RecordingMatcher();
        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, wrapped);
        assertThat(matcher).isInstanceOf(DataIndexPushdownManager.class);

        try (final PushdownFilterContext context = matcher.makePushdownFilterContext(filter, sources)) {
            final PushdownFilterContext wrappedContext = wrapped.lastContext.get();
            assertThat(wrappedContext).isNotNull();

            context.updateExecutedFilterCost(1234L);

            assertThat(context.executedFilterCost()).isEqualTo(1234L);
            assertThat(wrappedContext.executedFilterCost()).isEqualTo(1234L);
        }
    }

    /**
     * A matcher that reports every row as a maybe match, recording each result it hands out; it fails its call number
     * {@code failingCall}, counting from zero, if that is set.
     */
    private static final class ResultRecordingMatcher implements PushdownFilterMatcher {
        final List<PushdownResult> results = new ArrayList<>();
        int failingCall = -1;
        int calls;

        @Override
        public void pushdownFilter(
                final WhereFilter filter,
                final RowSet selection,
                final boolean usePrev,
                final PushdownFilterContext context,
                final long costCeiling,
                final JobScheduler jobScheduler,
                final Consumer<PushdownResult> onComplete,
                final Consumer<Exception> onError) {
            if (calls++ == failingCall) {
                onError.accept(new IllegalStateException("injected pushdown failure"));
                return;
            }
            final PushdownResult result = PushdownResult.allMaybeMatch(selection);
            results.add(result);
            onComplete.accept(result);
        }

        @Override
        public PushdownFilterContext makePushdownFilterContext(
                final WhereFilter filter,
                final List<ColumnSource<?>> filterSources) {
            return new BasePushdownFilterContextImpl(filter, filterSources);
        }
    }

    private static PushdownResult pushdown(
            final PushdownFilterMatcher matcher,
            final WhereFilter filter,
            final List<ColumnSource<?>> sources,
            final RowSet selection) {
        return pushdown(matcher, filter, sources, selection, false);
    }

    private static PushdownResult pushdown(
            final PushdownFilterMatcher matcher,
            final WhereFilter filter,
            final List<ColumnSource<?>> sources,
            final RowSet selection,
            final boolean usePrev) {
        final AtomicReference<PushdownResult> delivered = new AtomicReference<>();
        try (final PushdownFilterContext context = matcher.makePushdownFilterContext(filter, sources)) {
            matcher.pushdownFilter(filter, selection, usePrev, context, Long.MAX_VALUE, new ImmediateJobScheduler(),
                    delivered::set, e -> {
                        throw new AssertionError(e);
                    });
        }
        assertNotNull("the pushdown must deliver a result", delivered.get());
        return delivered.get();
    }

    private static void assertClosed(final PushdownResult result) {
        assertThat(((WritableRowSetImpl) result.match()).getInnerSet()).as("match closed").isNull();
        assertThat(((WritableRowSetImpl) result.maybeMatch()).getInnerSet()).as("maybeMatch closed").isNull();
    }

    /**
     * When the selection is large enough to use the index, the wrapped matcher's result feeds the index filter and must
     * then be closed.
     */
    @Test
    public void wrappedResultClosedWhenIndexUsed() {
        final Table table = indexedTable();
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final WhereFilter filter = initializedFilter(table, "A == 5");
        final List<ColumnSource<?>> sources = List.of(table.getColumnSource("A"));

        final ResultRecordingMatcher wrapped = new ResultRecordingMatcher();
        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, wrapped);

        try (final PushdownResult result = pushdown(matcher, filter, sources, table.getRowSet())) {
            assertThat(result.match().size()).isEqualTo(ROWS_PER_KEY);
            assertThat(result.maybeMatch().isEmpty()).isTrue();
        }
        assertThat(wrapped.results).hasSize(1);
        assertClosed(wrapped.results.get(0));
    }

    /**
     * When the selection is too small to use the index, the wrapped matcher runs again on the first result's maybe
     * rows; the first result must be closed, and the second is the one delivered.
     */
    @Test
    public void wrappedResultClosedWhenIndexSkipped() {
        final Table table = indexedTable();
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final WhereFilter filter = initializedFilter(table, "A == 5");
        final List<ColumnSource<?>> sources = List.of(table.getColumnSource("A"));

        final ResultRecordingMatcher wrapped = new ResultRecordingMatcher();
        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, wrapped);

        try (final RowSet selection = RowSetFactory.flat(100);
                final PushdownResult result = pushdown(matcher, filter, sources, selection)) {
            assertThat(wrapped.results).hasSize(2);
            assertThat(result).isSameAs(wrapped.results.get(1));
        }
        assertClosed(wrapped.results.get(0));
    }

    /**
     * When the second round of the wrapped matcher fails, the first round's result must still be closed.
     */
    @Test
    public void wrappedResultClosedWhenSecondPushdownFails() {
        final Table table = indexedTable();
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final WhereFilter filter = initializedFilter(table, "A == 5");
        final List<ColumnSource<?>> sources = List.of(table.getColumnSource("A"));

        final ResultRecordingMatcher wrapped = new ResultRecordingMatcher();
        wrapped.failingCall = 1;
        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, wrapped);

        final AtomicReference<Exception> error = new AtomicReference<>();
        try (final RowSet selection = RowSetFactory.flat(100);
                final PushdownFilterContext context = matcher.makePushdownFilterContext(filter, sources)) {
            matcher.pushdownFilter(filter, selection, false, context, Long.MAX_VALUE, new ImmediateJobScheduler(),
                    result -> {
                        throw new AssertionError("the pushdown must fail");
                    }, error::set);
        }
        assertThat(error.get()).hasMessage("injected pushdown failure");
        assertThat(wrapped.results).hasSize(1);
        assertClosed(wrapped.results.get(0));
    }

    /**
     * Without a wrapped matcher there is nothing to pass the cost on to, and that must not be an error.
     */
    @Test
    public void executedFilterCostWithoutWrappedContext() {
        final Table table = indexedTable();
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final WhereFilter filter = initializedFilter(table, "A == 5");
        final List<ColumnSource<?>> sources = List.of(table.getColumnSource("A"));

        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, null);
        try (final PushdownFilterContext context = matcher.makePushdownFilterContext(filter, sources)) {
            context.updateExecutedFilterCost(1234L);
            assertThat(context.executedFilterCost()).isEqualTo(1234L);
        }
    }

    /**
     * The estimate advertises the data index cost exactly when the pushdown will use the index. At the threshold, a
     * supported estimate followed by a pushdown that declines to use the index is a scheduled round that resolves
     * nothing.
     */
    @Test
    public void estimateAgreesWithPushdownAtThreshold() {
        checkEstimateAgreesWithPushdownAtThreshold(null);
    }

    /**
     * As {@link #estimateAgreesWithPushdownAtThreshold()}, through the branch that runs a wrapped matcher before the
     * index. The wrapped matcher resolves nothing, so the index sees the whole selection.
     */
    @Test
    public void estimateAgreesWithPushdownAtThresholdWithWrappedMatcher() {
        checkEstimateAgreesWithPushdownAtThreshold(new RecordingMatcher());
    }

    /**
     * Building the manager happens during filter setup on every {@code where()}, before anyone knows whether the index
     * will be used. It needs only the index table's row count, so it must not force a lazily merged index to read and
     * merge every location's row sets.
     */
    @Test
    public void constructionDoesNotRequestFullIndexTable() {
        final Table table = indexedTable();
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final List<DataIndexOptions> requests = new ArrayList<>();
        final DataIndex recording = (DataIndex) Proxy.newProxyInstance(
                DataIndex.class.getClassLoader(),
                new Class<?>[] {DataIndex.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("table")) {
                        final DataIndexOptions options = args == null ? DataIndexOptions.DEFAULT
                                : (DataIndexOptions) args[0];
                        requests.add(options);
                        return dataIndex.table(options);
                    }
                    return method.invoke(dataIndex, args);
                });

        assertThat(DataIndexPushdownManager.wrap(recording, null)).isInstanceOf(DataIndexPushdownManager.class);
        assertThat(requests).allMatch(DataIndexOptions::operationUsesPartialTable);
    }

    /**
     * A filter that reads an indexed column and an alias of that column names one index column twice.
     */
    @Test
    public void filterOnIndexedColumnAndAlias() {
        final Table table = indexedTable();
        checkIndexedFilter(table, DataIndexer.getOrCreateDataIndex(table, "A"), table.updateView("A2 = A"),
                "A == 5 && A2 == 5");
    }

    /**
     * A filter that reads two aliases of an indexed column names one index column twice, under names that are both
     * different from the index column's.
     */
    @Test
    public void filterOnTwoAliasesOfIndexedColumn() {
        final Table table = indexedTable();
        checkIndexedFilter(table, DataIndexer.getOrCreateDataIndex(table, "A"),
                table.updateView("A2 = A", "A3 = A").dropColumns("A"), "A2 == 5 && A3 == 5");
    }

    /**
     * A filter that reads indexed columns A and B under swapped names must still see each index column under the
     * filter's name for the indexed column.
     */
    @Test
    public void filterOnSwappedNamesOfIndexedColumns() {
        // B divides A's period, so the index has no more keys than an index on A alone.
        final Table table = indexedTable().update("B = ii % 4");
        checkIndexedFilter(table, DataIndexer.getOrCreateDataIndex(table, "A", "B"),
                table.renameColumns(List.of(Pair.parse("A = B"), Pair.parse("B = A"))), "A == 1 && B == 5");
    }

    /**
     * A filter that reads swapped names of indexed columns A and B, and a second name for the index's B, must see each
     * index column under every name the filter reads the index column by.
     */
    @Test
    public void filterOnSwappedNamesAndAliasOfIndexedColumns() {
        // B divides A's period, so the index has no more keys than an index on A alone.
        final Table table = indexedTable().update("B = ii % 4");
        checkIndexedFilter(table, DataIndexer.getOrCreateDataIndex(table, "A", "B"),
                table.renameColumns(List.of(Pair.parse("A = B"), Pair.parse("B = A"))).updateView("A2 = A"),
                "A == 1 && A2 == 1 && B == 5");
    }

    /**
     * A filter that reads indexed column A as X and as B, and indexed column B as Y, gives the index's A the name of
     * the index's B, and must still see the index's B as Y.
     */
    @Test
    public void filterOnAliasNamedLikeAnotherIndexedColumn() {
        // B divides A's period, so the index has no more keys than an index on A alone.
        final Table table = indexedTable().update("B = ii % 4");
        checkIndexedFilter(table, DataIndexer.getOrCreateDataIndex(table, "A", "B"),
                table.renameColumns(List.of(Pair.parse("X = A"), Pair.parse("Y = B"))).updateView("B = X"),
                "X == 5 && B == 5 && Y == 1");
    }

    /**
     * A filter that reads an alias of an indexed column, and filters previous values after the index has updated the
     * index's row sets in place, must see the index's previous state under the alias.
     */
    @Test
    public void filterOnAliasReadsPreviousIndexState() {
        final int size = KEYS * ROWS_PER_KEY;
        final QueryTable table = TstUtils.testRefreshingTable(RowSetFactory.flat(size).toTracking(),
                TableTools.intCol("A", IntStream.range(0, size).map(ii -> ii % KEYS).toArray()));
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final Table aliased = table.updateView("A2 = A");
        final WhereFilter filter = initializedFilter(aliased, "A2 == 5");
        final List<ColumnSource<?>> sources = List.of(aliased.getColumnSource("A2"));
        assertThat(dataIndex.keyColumnNamesByIndexedColumn()).containsKey(sources.get(0));
        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, null);

        try (final RowSet previousMatches =
                filter.filter(aliased.getRowSet(), aliased.getRowSet(), aliased, false)) {
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                // Row 5 leaves key 5, and row 6 joins key 5.
                TstUtils.addToTable(table, TstUtils.i(5, 6), TableTools.intCol("A", 6, 5));
                table.notifyListeners(TstUtils.i(), TstUtils.i(), TstUtils.i(5, 6));
                // The index updates the index's row sets in place.
                updateGraph.flushAllNormalNotificationsForUnitTests();

                try (final RowSet previousRows = table.getRowSet().copyPrev();
                        final PushdownResult previous = pushdown(matcher, filter, sources, previousRows, true);
                        final PushdownResult current = pushdown(matcher, filter, sources, table.getRowSet(), false);
                        final RowSet currentMatches =
                                filter.filter(aliased.getRowSet(), aliased.getRowSet(), aliased, false)) {
                    assertThat(currentMatches).isNotEqualTo(previousMatches);
                    assertThat(previous.maybeMatch().isEmpty()).isTrue();
                    assertThat(previous.match()).isEqualTo(previousMatches);
                    assertThat(current.maybeMatch().isEmpty()).isTrue();
                    assertThat(current.match()).isEqualTo(currentMatches);
                }
            });
        }
    }

    /**
     * Push {@code expression} down to {@code dataIndex} as a filter of {@code filtered}, a table whose filter columns
     * share sources with the indexed {@code table}, and check that the index matches what the filter itself matches.
     */
    private static void checkIndexedFilter(
            final Table table,
            final DataIndex dataIndex,
            final Table filtered,
            final String expression) {
        final WhereFilter filter = initializedFilter(filtered, expression);
        final List<ColumnSource<?>> sources = new ArrayList<>();
        filter.getColumns().forEach(column -> sources.add(filtered.getColumnSource(column)));
        assertThat(sources).allMatch(source -> dataIndex.keyColumnNamesByIndexedColumn().containsKey(source));

        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, null);
        try (final PushdownResult result = pushdown(matcher, filter, sources, table.getRowSet());
                // Apply the filter itself, since filtered.where could push down to the same index.
                final RowSet expected = filter.filter(filtered.getRowSet(), filtered.getRowSet(), filtered, false)) {
            assertThat(result.maybeMatch().isEmpty()).isTrue();
            assertThat(expected.isEmpty()).isFalse();
            assertThat(result.match()).isEqualTo(expected);
        }
    }

    private static void checkEstimateAgreesWithPushdownAtThreshold(final PushdownFilterMatcher wrapped) {
        final Table table = indexedTable();
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(table, "A");
        final WhereFilter filter = initializedFilter(table, "A == 5");
        final List<ColumnSource<?>> sources = List.of(table.getColumnSource("A"));
        final long threshold = (long) (dataIndex.table().size() / QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD);
        assertThat(threshold).isBetween(2L, table.size() - 1);

        final PushdownFilterMatcher matcher = DataIndexPushdownManager.wrap(dataIndex, wrapped);
        for (final long size : new long[] {threshold - 1, threshold, threshold + 1}) {
            try (final PushdownFilterContext context = matcher.makePushdownFilterContext(filter, sources);
                    final RowSet selection = RowSetFactory.flat(size)) {
                final AtomicLong cost = new AtomicLong(-1);
                matcher.estimatePushdownFilterCost(filter, selection, false, context, new ImmediateJobScheduler(),
                        cost::set, e -> {
                            throw new AssertionError(e);
                        });

                final AtomicReference<PushdownResult> result = new AtomicReference<>();
                matcher.pushdownFilter(filter, selection, false, context, cost.get(), new ImmediateJobScheduler(),
                        result::set, e -> {
                            throw new AssertionError(e);
                        });
                try (final PushdownResult pushed = result.get()) {
                    assertNotNull(pushed);
                    final boolean supported = cost.get() == PushdownResult.TABLE_IN_MEMORY_DATA_INDEX_COST;
                    final boolean usedIndex = pushed.maybeMatch().size() < selection.size();
                    assertThat(usedIndex)
                            .as("selection size %d, threshold %d, estimated cost %d", size, threshold, cost.get())
                            .isEqualTo(supported);
                }
            }
        }
    }
}
