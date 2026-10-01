//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.TableAlreadyFailedException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndexOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.WouldMatchPair;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.FailureRecordingListener;
import io.deephaven.engine.table.impl.ForcedParallelWhere;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.dataindex.AbstractDataIndex;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.sources.IntTestSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.booleanCol;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the lock-free {@link DynamicWhereFilter}: how it snapshots against a set that may change underneath it, and
 * how its shared set listener fans changes and failures out to every result.
 * <p>
 * The race tests park a table operation at a known point on a worker thread (a {@link Gate} inside a column source or a
 * table's listener registration), drive the update graph from the test thread while the operation is parked, and then
 * let the operation continue. The update graph is a {@link ControlledUpdateGraph}, so the set table's listener runs
 * only when the test flushes it.
 */
@Category(OutOfBandTest.class)
public class TestDynamicWhereFilter {

    private static final long TIMEOUT_SECONDS = 10;

    /** The set key column on both the source and the set table. */
    private static final String KEY = "Z";

    /**
     * The {@link RowSet} column name every {@link AbstractDataIndex} uses; asserted against the index in
     * {@link #setUp}.
     */
    private static final String ROW_SET_COLUMN = "dh_row_set";

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private ControlledUpdateGraph updateGraph;
    private ExecutorService pool;

    @Before
    public void setUp() {
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ThreadFactory threadFactory = runnable -> {
            final Thread thread = new Thread(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    runnable.run();
                }
            });
            thread.setDaemon(true);
            return thread;
        };
        pool = Executors.newFixedThreadPool(2, threadFactory);
    }

    @After
    public void tearDown() {
        pool.shutdownNow();
    }

    private static MatchPair[] pairs() {
        return MatchPairFactory.getExpressions(KEY);
    }

    /**
     * Finish the unit test cycle if a test failed part way through one, so that the failure is reported against this
     * test rather than the next.
     */
    private void endCycleIfOpen() {
        if (updateGraph.clock().currentState() != LogicalClock.State.Updating) {
            return;
        }
        try {
            if (!updateGraph.satisfied(updateGraph.clock().currentStep())) {
                updateGraph.markSourcesRefreshedForUnitTests();
            }
            updateGraph.completeCycleForUnitTests();
        } catch (RuntimeException e) {
            // Cleanup only; a failure here must not hide the test's own failure.
            System.err.println("Ignoring failure while ending the unit test cycle: " + e);
        }
    }

    // region Test scaffolding

    /**
     * A one-shot gate. The first {@link #passThrough()} after {@link #arm()} reports that it was reached and blocks
     * until {@link #release()}; every other call is a no-op, so retries and later reads are never blocked.
     */
    private static final class Gate {
        private final AtomicBoolean armed = new AtomicBoolean();
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void arm() {
            armed.set(true);
        }

        void passThrough() {
            if (!armed.compareAndSet(true, false)) {
                return;
            }
            reached.countDown();
            try {
                if (!released.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Gate was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while parked at gate", e);
            }
        }

        boolean awaitReached(final long millis) throws InterruptedException {
            return reached.await(millis, TimeUnit.MILLISECONDS);
        }

        void release() {
            released.countDown();
        }
    }

    /**
     * An int column whose chunk reads pass through a {@link Gate}, and are counted. This is the filter's key column on
     * the source table, so it parks a {@code where} or {@code wouldMatch} exactly between the filter's
     * {@link SharedSetKernel#beginRead()} and its first look at the kernel.
     */
    private static final class GatedIntegerArraySource extends IntegerArraySource {
        private final Gate gate;
        final AtomicInteger chunkReads = new AtomicInteger();

        GatedIntegerArraySource(final Gate gate) {
            this.gate = gate;
        }

        @Override
        public Chunk<Values> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
            gate.passThrough();
            chunkReads.incrementAndGet();
            return super.getChunk(context, rowSequence);
        }

        @Override
        public Chunk<? extends Values> getPrevChunk(@NotNull final GetContext context,
                @NotNull final RowSequence rowSequence) {
            gate.passThrough();
            chunkReads.incrementAndGet();
            return super.getPrevChunk(context, rowSequence);
        }
    }

    /** Add one row to a refreshing table built by {@link #sourceTable}, recording previous values as a tick would. */
    private static void addSourceRow(final QueryTable source, final GatedIntegerArraySource key, final long rowKey,
            final int value) {
        key.ensureCapacity(rowKey + 1, false);
        key.set(rowKey, value);
        source.getRowSet().writableCast().insert(rowKey);
    }

    private static QueryTable sourceTable(final GatedIntegerArraySource key, final boolean refreshing,
            final int... values) {
        key.ensureCapacity(values.length, false);
        for (int ii = 0; ii < values.length; ++ii) {
            key.set(ii, values[ii]);
        }
        final TrackingRowSet rowSet = RowSetFactory.flat(values.length).toTracking();
        final QueryTable table = new QueryTable(rowSet, Map.of(KEY, key));
        if (refreshing) {
            table.setRefreshing(true);
            key.startTrackingPrevValues();
        }
        return table;
    }

    /**
     * A refreshing table whose {@link #addUpdateListener(TableUpdateListener)} passes through a {@link Gate} after the
     * listener is subscribed. Used as a data index table so that the {@link SharedSetKernel} installs its set listener
     * here, parking the kernel's snapshot attempt after the listener is subscribed and before the attempt is judged.
     */
    private static final class GatedListenerTable extends QueryTable {
        private final Gate gate;

        GatedListenerTable(
                @NotNull final TrackingRowSet rowSet,
                @NotNull final Map<String, ? extends ColumnSource<?>> columns,
                @NotNull final Gate gate) {
            super(rowSet, columns);
            this.gate = gate;
            setRefreshing(true);
        }

        @Override
        public void addUpdateListener(@NotNull final TableUpdateListener listener) {
            super.addUpdateListener(listener);
            gate.passThrough();
        }
    }

    /**
     * A data index over {@code setTable}'s key column whose index table is supplied by the test. The set side of a
     * {@link DynamicWhereFilter} only ever reads the index table, so no row key lookup is needed.
     */
    private static final class TestSetIndex extends AbstractDataIndex {
        private final ColumnSource<?> indexedColumn;
        private final QueryTable indexTable;

        TestSetIndex(@NotNull final QueryTable setTable, @NotNull final QueryTable indexTable) {
            this.indexedColumn = setTable.getColumnSource(KEY);
            this.indexTable = indexTable;
            manage(indexTable);
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public @NotNull List<String> keyColumnNames() {
            return List.of(KEY);
        }

        @Override
        public @NotNull Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
            return Map.of(indexedColumn, KEY);
        }

        @Override
        public boolean tableIsCached() {
            return true;
        }

        @Override
        public @NotNull Table table(final DataIndexOptions options) {
            return indexTable;
        }

        @Override
        public @NotNull RowKeyLookup rowKeyLookup(final DataIndexOptions options) {
            throw new UnsupportedOperationException("Not used by the set side of a DynamicWhereFilter");
        }

        @Override
        public boolean isRefreshing() {
            return indexTable.isRefreshing();
        }
    }

    /**
     * Register a data index for {@code setTable}'s key column whose (refreshing) index table holds the single key
     * {@code 1} and whose listener registration passes through {@code gate}. A {@link DynamicWhereFilter} over
     * {@code setTable} then builds its {@link SharedSetKernel} from, and subscribes its set listener to, that table.
     */
    private static GatedListenerTable installGatedSetIndex(final QueryTable setTable, final Gate gate) {
        final QueryTable indexColumns = TstUtils.testRefreshingTable(i(0).toTracking(),
                intCol(KEY, 1), col(ROW_SET_COLUMN, (RowSet) RowSetFactory.fromKeys(0)));
        final GatedListenerTable indexTable =
                new GatedListenerTable(indexColumns.getRowSet(), indexColumns.getColumnSourceMap(), gate);
        final TestSetIndex index = new TestSetIndex(setTable, indexTable);
        assertEquals(ROW_SET_COLUMN, index.rowSetColumnName());
        DataIndexer.of(setTable.getRowSet()).addDataIndex(index);
        return indexTable;
    }

    /**
     * An int column whose chunk reads pass through a {@link Gate}, and which {@link TstUtils#addToTable} can write to.
     * This is the key column of a set index table, so it parks the set listener midway through a kernel mutation.
     */
    private static final class GatedIntTestSource extends IntTestSource {
        private final Gate gate;

        GatedIntTestSource(final Gate gate) {
            this.gate = gate;
        }

        @Override
        public Chunk<? extends Values> getChunk(
                @NotNull final GetContext context,
                @NotNull final RowSequence rowSequence) {
            gate.passThrough();
            return super.getChunk(context, rowSequence);
        }
    }

    /**
     * Register a data index for {@code setTable}'s key column, holding the single key {@code 1}, whose index table
     * reads that key column through {@code keyReadGate}. The {@link SharedSetKernel} builds its kernel from, and
     * subscribes its set listener to, that table, so an armed gate parks the set listener midway through a kernel
     * mutation.
     *
     * @return The index table, which the caller ticks to change the set
     */
    private static QueryTable registerSetIndex(final QueryTable setTable, final Gate keyReadGate) {
        final QueryTable rowSetColumnTable = TstUtils.testRefreshingTable(i(0).toTracking(),
                col(ROW_SET_COLUMN, (RowSet) RowSetFactory.fromKeys(0)));
        final GatedIntTestSource keySource = new GatedIntTestSource(keyReadGate);
        try (final RowSet initialKeyRows = RowSetFactory.fromKeys(0)) {
            keySource.add(initialKeyRows, intCol(KEY, 1).getChunk());
        }
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        columns.put(KEY, keySource);
        columns.put(ROW_SET_COLUMN, rowSetColumnTable.getColumnSource(ROW_SET_COLUMN));
        final QueryTable indexTable = new QueryTable(rowSetColumnTable.getRowSet(), columns);
        indexTable.setRefreshing(true);
        final TestSetIndex index = new TestSetIndex(setTable, indexTable);
        assertEquals(ROW_SET_COLUMN, index.rowSetColumnName());
        DataIndexer.of(setTable.getRowSet()).addDataIndex(index);
        return indexTable;
    }

    /**
     * A {@link DynamicWhereFilter} that passes through a {@link Gate} once it has been given its recompute listener.
     * That parks a lock-free {@code where} between the moment a set change can reach its result and the moment that
     * result has a {@code WhereListener} to notify.
     */
    private static final class GateAfterRecomputeListenerFilter extends DynamicWhereFilter {

        private final Gate gate;

        private GateAfterRecomputeListenerFilter(@NotNull final Table setTable, final Gate gate) {
            super(setTable, true, pairs());
            this.gate = gate;
        }

        @Override
        public void setRecomputeListener(final RecomputeListener listener) {
            super.setRecomputeListener(listener);
            gate.passThrough();
        }
    }

    /**
     * A {@link DynamicWhereFilter} that passes through a {@link Gate} when a snapshot attempt asks when the set last
     * changed. An attempt asks twice: as it begins, to record what its commit must find unchanged, and at its
     * completion check. Arm the gate once the attempt has begun its read and it parks the attempt at that check, after
     * its result has a {@code WhereListener} and before the attempt is judged, subscribed and, if rejected, released.
     */
    private static final class GateInCompletionCheckFilter extends DynamicWhereFilter {

        private final Gate gate;

        private GateInCompletionCheckFilter(@NotNull final Table setTable, final Gate gate) {
            super(setTable, true, pairs());
            this.gate = gate;
        }

        @Override
        public long lastStateChangeStep() {
            gate.passThrough();
            return super.lastStateChangeStep();
        }
    }

    /**
     * A {@link DynamicWhereFilter} that passes through a {@link Gate} as its snapshot attempt commits, just before that
     * attempt subscribes to the set. The attempt's verdict is already given by then, so a set change or failure made
     * while it is parked is one the verdict did not see, and the subscription is all that can still refuse it.
     */
    private static final class GateBeforeSubscribeFilter extends DynamicWhereFilter {

        private final Gate gate;

        private GateBeforeSubscribeFilter(@NotNull final Table setTable, final Gate gate) {
            super(setTable, true, pairs());
            this.gate = gate;
        }

        @Override
        public boolean subscribe(final long requiredLastStateChangeStep) {
            gate.passThrough();
            return super.subscribe(requiredLastStateChangeStep);
        }
    }

    /**
     * A {@link DynamicWhereFilter} that passes through a {@link Gate} once its snapshot attempt has subscribed to the
     * set, and before that attempt subscribes its result to the source. It is the one window in which a set change can
     * reach a result whose attempt is still going to be rejected.
     */
    private static final class GateAfterSubscribeFilter extends DynamicWhereFilter {

        private final Gate gate;
        /** The result of the first attempt, which is the one these tests reject. */
        private volatile QueryTable firstResult;

        private GateAfterSubscribeFilter(@NotNull final Table setTable, final Gate gate) {
            super(setTable, true, pairs());
            this.gate = gate;
        }

        @Override
        public void setRecomputeListener(final RecomputeListener listener) {
            super.setRecomputeListener(listener);
            if (firstResult == null) {
                firstResult = listener.getTable();
            }
        }

        @Override
        public boolean subscribe(final long requiredLastStateChangeStep) {
            final boolean subscribed = super.subscribe(requiredLastStateChangeStep);
            gate.passThrough();
            return subscribed;
        }
    }

    /**
     * A {@link DynamicWhereFilter} that passes through a {@link Gate} once a snapshot attempt has asked whether the set
     * is satisfied and been told it is not. Arm the gate before the attempt begins, and it parks the attempt after it
     * has recorded when the set last changed and found that it must wait for the set listener, and before it waits.
     */
    private static final class GateWhenUnsatisfiedFilter extends DynamicWhereFilter {

        private final Gate gate;

        private GateWhenUnsatisfiedFilter(@NotNull final Table setTable, final Gate gate) {
            super(setTable, true, pairs());
            this.gate = gate;
        }

        @Override
        public boolean satisfied(final long step) {
            final boolean satisfied = super.satisfied(step);
            if (!satisfied) {
                gate.passThrough();
            }
            return satisfied;
        }
    }

    /**
     * A full data index over a source table's key column whose row key lookup passes through a {@link Gate}, parking a
     * {@code where} inside {@link DynamicWhereFilter}'s index filtering rather than its linear filtering.
     */
    private static final class GatedSourceIndex extends AbstractDataIndex {

        private final ColumnSource<?> indexedColumn;
        private final QueryTable indexTable;
        private final Map<Object, Long> indexRowKeyByKey;
        private final Gate gate;
        final AtomicInteger lookups = new AtomicInteger();

        private GatedSourceIndex(
                @NotNull final QueryTable sourceTable,
                @NotNull final QueryTable indexTable,
                @NotNull final Map<Object, Long> indexRowKeyByKey,
                @NotNull final Gate gate) {
            this.indexedColumn = sourceTable.getColumnSource(KEY);
            this.indexTable = indexTable;
            this.indexRowKeyByKey = indexRowKeyByKey;
            this.gate = gate;
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public @NotNull List<String> keyColumnNames() {
            return List.of(KEY);
        }

        @Override
        public @NotNull Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
            return Map.of(indexedColumn, KEY);
        }

        @Override
        public boolean tableIsCached() {
            return true;
        }

        @Override
        public @NotNull Table table(final DataIndexOptions options) {
            return indexTable;
        }

        @Override
        public @NotNull RowKeyLookup rowKeyLookup(final DataIndexOptions options) {
            return (final Object key, final boolean usePrev) -> {
                gate.passThrough();
                lookups.incrementAndGet();
                final Long indexRowKey = indexRowKeyByKey.get(key);
                return indexRowKey == null ? RowSequence.NULL_ROW_KEY : indexRowKey;
            };
        }

        @Override
        public boolean isRefreshing() {
            return false;
        }
    }

    /**
     * A source table of {@code 5} rows for each of the keys {@code 1} and {@code 2}, with a {@link GatedSourceIndex}
     * over its key column. The row count is deliberately more than
     * {@value io.deephaven.engine.table.impl.QueryTable#DATA_INDEX_FOR_WHERE_THRESHOLD} times the index table's size,
     * so that {@link DynamicWhereFilter} filters through the index rather than linearly.
     */
    private static QueryTable indexedSourceTable(final GatedSourceIndex[] indexOut, final Gate lookupGate) {
        final int[] values = new int[10];
        for (int ii = 0; ii < values.length; ++ii) {
            values[ii] = ii < 5 ? 1 : 2;
        }
        final QueryTable source = sourceTable(new GatedIntegerArraySource(new Gate()), true, values);
        final QueryTable indexTable = TstUtils.testTable(i(0, 1).toTracking(), intCol(KEY, 1, 2),
                col(ROW_SET_COLUMN,
                        (RowSet) RowSetFactory.fromRange(0, 4),
                        (RowSet) RowSetFactory.fromRange(5, 9)));
        final GatedSourceIndex index =
                new GatedSourceIndex(source, indexTable, Map.of(1, 0L, 2, 1L), lookupGate);
        assertEquals(ROW_SET_COLUMN, index.rowSetColumnName());
        DataIndexer.of(source.getRowSet()).addDataIndex(index);
        indexOut[0] = index;
        return source;
    }

    /**
     * Construct a {@link DynamicWhereFilter} over {@code setTable} on a worker thread, mid-cycle and lock-free, and
     * fail its first {@link SharedSetKernel} snapshot attempt by ticking the index table after the attempt has
     * subscribed its set listener. The attempt's listener therefore has a notification queued when it is discarded, and
     * the retry commits against current values without any notification of its own.
     *
     * @return The filter, whose committed set is {@code {1, 2}}
     */
    private DynamicWhereFilter buildFilterWithDiscardedListener(
            final QueryTable setTable,
            final GatedListenerTable indexTable,
            final Gate listenerGate) throws Exception {
        listenerGate.arm();
        final Future<DynamicWhereFilter> filterFuture =
                pool.submit(() -> new DynamicWhereFilter(setTable, true, pairs()));
        assertTrue("first kernel snapshot attempt subscribed its listener",
                listenerGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

        // The attempt used previous values and has just subscribed. Tick the index table: the attempt must now fail,
        // and the notification for this tick is queued for the listener it leaves behind.
        TstUtils.addToTable(indexTable, i(1), intCol(KEY, 2), col(ROW_SET_COLUMN, (RowSet) RowSetFactory.fromKeys(1)));
        indexTable.notifyListeners(i(1), i(), i());
        listenerGate.release();

        return filterFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // endregion

    /**
     * A set listener left behind by a discarded {@link SharedSetKernel} snapshot attempt is superseded by the retry's
     * listener. When a notification queued for the superseded listener runs, it leaves the shared state alone: the
     * committed kernel's generation does not move, and no state change is reported for the step.
     */
    @Test
    public void testDiscardedSetListenerLeavesSharedStateAlone() throws Exception {
        final Gate listenerGate = new Gate();
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final GatedListenerTable indexTable = installGatedSetIndex(setTable, listenerGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            final DynamicWhereFilter filter = buildFilterWithDiscardedListener(setTable, indexTable, listenerGate);
            final SharedSetKernel shared = filter.sharedSet();

            // The committed kernel was built from current values, after the tick, and nothing has changed it.
            assertNotEquals(step, filter.lastStateChangeStep());
            final long generation = shared.beginRead();

            // The only queued notification belongs to the discarded attempt's listener. Let it run.
            updateGraph.markSourcesRefreshedForUnitTests();
            assertTrue(updateGraph.flushOneNotificationForUnitTests());

            assertNotEquals("a discarded attempt's listener must not report a state change for the committed kernel",
                    step, filter.lastStateChangeStep());
            assertEquals("a discarded attempt's listener must not move the committed kernel's generation",
                    generation, shared.beginRead());
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A {@code where} that reads the committed kernel with current values while a superseded listener's notification
     * runs completes successfully, with the result the committed set implies.
     */
    @Test
    public void testDiscardedSetListenerDoesNotFailConcurrentWhere() throws Exception {
        final Gate listenerGate = new Gate();
        final Gate sourceGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final GatedListenerTable indexTable = installGatedSetIndex(setTable, listenerGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            final DynamicWhereFilter filter = buildFilterWithDiscardedListener(setTable, indexTable, listenerGate);

            // The source is a quiet root and the index table has ticked, so the where below uses current values.
            updateGraph.markSourcesRefreshedForUnitTests();
            assertTrue(source.satisfied(step));
            assertTrue(filter.satisfied(step));

            sourceGate.arm();
            final Future<Table> whereFuture = pool.submit(() -> source.where(filter));
            assertTrue("where began reading the kernel",
                    sourceGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            // Run the discarded attempt's listener underneath the where, then let the where finish its read.
            updateGraph.flushOneNotificationForUnitTests();
            sourceGate.release();

            final Table result;
            try {
                result = whereFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new AssertionError("where failed although the committed set never changed: " + e.getCause(),
                        e.getCause());
            }
            assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
            updateGraph.completeCycleForUnitTests();
            assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * Runs {@code operation} on a static table concurrently with a set change on the same step, arranging for the set
     * listener to run underneath the operation's kernel read when the operation gets there first. A static source with
     * an unsatisfied set filter is the "nothing satisfied" case: the operation reads the set as it stands rather than
     * waiting, the aware check rejects the read if the set moved underneath it, and the retry reads current values. The
     * result must reflect the set as of the step either way.
     */
    private void assertStaticOperationSurvivesSetTick(
            final BiFunction<QueryTable, DynamicWhereFilter, Table> operation,
            final Table expected) throws Exception {
        final Gate sourceGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, false, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            sourceGate.arm();
            final Future<Table> resultFuture = pool.submit(() -> operation.apply(source, filter));

            // Change the set on this step. The set listener has not run yet.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();

            if (sourceGate.awaitReached(1_000)) {
                // The operation reached the kernel before the set listener ran. Run the listener under it, so the
                // aware check must reject this attempt and the retry must read the changed set.
                while (!filter.satisfied(step)) {
                    assertTrue(updateGraph.flushOneNotificationForUnitTests());
                }
            } else {
                // The operation has not reached the kernel yet. Run the set listener first, then let it get there; it
                // then finds every dependency satisfied and reads current values.
                while (!filter.satisfied(step)) {
                    assertTrue(updateGraph.flushOneNotificationForUnitTests());
                }
                while (!sourceGate.awaitReached(10)) {
                    updateGraph.flushOneNotificationForUnitTests();
                }
            }
            sourceGate.release();

            final Table result;
            try {
                result = resultFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new AssertionError("operation on a static table failed when its set ticked: " + e.getCause(),
                        e.getCause());
            }
            updateGraph.completeCycleForUnitTests();
            assertTableEquals(expected, result);
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * {@code where} on a static table with a {@link DynamicWhereFilter} over a refreshing set: see
     * {@link #assertStaticOperationSurvivesSetTick}.
     */
    @Test
    public void testStaticWhereIsProtectedFromRefreshingSet() throws Exception {
        assertStaticOperationSurvivesSetTick(
                (source, filter) -> source.where(filter),
                newTable(intCol(KEY, 1, 2)));
    }

    /**
     * {@code wouldMatch} on a static table initializes under the same snapshot control as {@code where} when a filter
     * has refreshing dependencies: see {@link #assertStaticOperationSurvivesSetTick}.
     */
    @Test
    public void testStaticWouldMatchIsProtectedFromRefreshingSet() throws Exception {
        assertStaticOperationSurvivesSetTick(
                (source, filter) -> source.wouldMatch(new WouldMatchPair("M", filter)),
                newTable(intCol(KEY, 1, 2, 3), booleanCol("M", true, true, false)));
    }

    /**
     * A kernel read made on behalf of a concurrent snapshot attempt abandons that attempt with
     * {@link ConstructSnapshot.SnapshotInconsistentException}, whichever thread performs the read: the attempt's own
     * thread, or an operation initializer thread running a parallel {@code where} for it.
     */
    @Test
    public void testKernelReadOnWorkerThreadAbandonsAttemptCleanly() throws Exception {
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());
        final SharedSetKernel shared = filter.sharedSet();
        // Any value other than the current generation stands for "the set moved since this read began".
        final long staleGeneration = shared.beginRead() - 2;

        final MutableObject<Throwable> onCaller = new MutableObject<>();
        final MutableObject<Throwable> onWorker = new MutableObject<>();
        ConstructSnapshot.callDataSnapshotFunction("TestDynamicWhereFilter",
                ConstructSnapshot.makeSnapshotControl(false, true, setTable),
                (usePrev, beforeClockValue) -> {
                    try {
                        shared.failIfChangedSince(staleGeneration);
                    } catch (Throwable t) {
                        onCaller.setValue(t);
                    }
                    final Future<?> workerRead = ExecutionContext.getContext().getOperationInitializer()
                            .submit(() -> shared.failIfChangedSince(staleGeneration));
                    try {
                        workerRead.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        onWorker.setValue(e.getCause());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return true;
                });

        assertTrue("on the attempt's own thread: " + onCaller.getValue(),
                onCaller.getValue() instanceof ConstructSnapshot.SnapshotInconsistentException);
        assertTrue("on an operation initializer thread: " + onWorker.getValue(),
                onWorker.getValue() instanceof ConstructSnapshot.SnapshotInconsistentException);
    }

    /**
     * A set change that lands while an attempt that will be rejected is still running reaches nothing: that attempt
     * subscribes to its set only when it commits, which it never does. The rejected attempt's read and the retry's read
     * are the only reads the source sees.
     */
    @Test
    public void testDiscardedWhereAttemptIsNotRefilteredBySetChange() throws Exception {
        final Gate sourceGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Nothing is satisfied yet, so this attempt uses previous values.
            sourceGate.arm();
            final Future<Table> whereFuture = pool.submit(() -> source.where(filter));
            assertTrue("where began its previous-values read",
                    sourceGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            // Tick the source under it. The read stays consistent, so the attempt builds its result table and
            // WhereListener, registers the filter against that result, and is only then rejected.
            addSourceRow(source, sourceKey, 3, 4);
            source.notifyListeners(i(3), i(), i());
            sourceGate.release();

            // The retry cannot proceed until the set listener has run for this step. Change the set first, so the
            // listener's recompute request lands while the filter still points at the discarded result.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }

            // Let the retry finish; it may be parked on a wait notification that only a flush can satisfy.
            final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (!whereFuture.isDone()) {
                if (System.nanoTime() > deadlineNanos) {
                    fail("where did not complete after its dependencies were satisfied");
                }
                if (!updateGraph.flushOneNotificationForUnitTests()) {
                    // noinspection BusyWait
                    Thread.sleep(1);
                }
            }
            final Table result = whereFuture.get();

            // Anything still queued, including a wake-up for the discarded attempt, runs here.
            updateGraph.completeCycleForUnitTests();

            assertEquals("only the discarded attempt and the retry may read the source; a refilter of the discarded"
                    + " attempt's result is a third read", 2, sourceKey.chunkReads.get());
            assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * The same guarantee at the latest moment a rejected attempt can reach: parked at its completion check, with its
     * result and where listener built and nothing released yet. It has still not subscribed to its set, so a set change
     * made there reaches nothing, and only the rejected attempt's read and the retry's read reach the source.
     */
    @Test
    public void testSetChangeBeforeDiscardedAttemptIsReleasedDoesNotRefilterIt() throws Exception {
        final Gate sourceGate = new Gate();
        final Gate verdictGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new GateInCompletionCheckFilter(setTable, verdictGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            sourceGate.arm();
            final Future<Table> whereFuture = pool.submit(() -> source.where(filter));
            assertTrue("where began its previous-values read",
                    sourceGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            // Tick the source under the read so the attempt will be rejected, then park it at its completion check:
            // its result and where listener exist, and nothing has been released yet.
            verdictGate.arm();
            addSourceRow(source, sourceKey, 3, 4);
            source.notifyListeners(i(3), i(), i());
            sourceGate.release();
            assertTrue("attempt reached its completion check",
                    verdictGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            // Change the set now. The set listener finds the doomed result alive and queues a recompute for it.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }

            // Let the attempt be judged, rejected and released; the retry then reads current values.
            verdictGate.release();
            final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (!whereFuture.isDone()) {
                if (System.nanoTime() > deadlineNanos) {
                    fail("where did not complete after its dependencies were satisfied");
                }
                if (!updateGraph.flushOneNotificationForUnitTests()) {
                    // noinspection BusyWait
                    Thread.sleep(1);
                }
            }
            final Table result = whereFuture.get();

            // The recompute queued for the released result runs here, and must not read the source.
            updateGraph.completeCycleForUnitTests();

            assertEquals("only the discarded attempt and the retry may read the source", 2,
                    sourceKey.chunkReads.get());
            assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A {@code where} that reads previous values while its set ticks must be retried, because the set kernel keeps no
     * previous values for it to have read. This is what {@link NotificationAwareDependency} exists for, and the only
     * thing that rejects the attempt: the kernel read itself can complete before the mutation begins and still be
     * inconsistent with the previous-value source rows it was combined with.
     * <p>
     * The same interleaving delivers a recompute request to a result that has no where listener yet, which must be
     * dropped rather than fail on the update graph thread.
     */
    @Test
    public void testSetChangeDuringAPreviousValuesWhereForcesARetry() throws Exception {
        final Gate recomputeGate = new Gate();
        // Never armed; this source only counts reads, so that the retry can be asserted.
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(new Gate());
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final GateAfterRecomputeListenerFilter filter =
                new GateAfterRecomputeListenerFilter(setTable, recomputeGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Nothing is satisfied yet, so this attempt reads previous values.
            recomputeGate.arm();
            final Future<Table> whereFuture = pool.submit(() -> source.where(filter));
            assertTrue("where finished its previous-values read and registered its filter",
                    recomputeGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));
            assertEquals("the attempt has read the source exactly once", 1, sourceKey.chunkReads.get());

            // Change the set while the attempt is parked. The filter is registered against a result that has no
            // where listener yet, so the recompute request this produces has nothing to notify.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }
            assertEquals("the set changed on this step", step, filter.lastStateChangeStep());

            recomputeGate.release();

            final Table result;
            try {
                result = whereFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new AssertionError("where failed when its set ticked mid-attempt: " + e.getCause(),
                        e.getCause());
            }

            // The rejected attempt was retried against current values, so the result reflects the new set.
            assertEquals("the previous-values attempt must be rejected and retried", 2, sourceKey.chunkReads.get());
            assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
            updateGraph.completeCycleForUnitTests();
            assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A reader that arrives while the set listener is midway through mutating the kernel abandons its attempt at once,
     * rather than reading a half-rewritten set. This is the window that the kernel's odd generation marks, and the only
     * one that {@link SharedSetKernel#beginRead()} can detect by itself.
     */
    @Test
    public void testKernelReadDuringASetMutationAbandonsTheAttempt() throws Exception {
        final Gate setReadGate = new Gate();
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final QueryTable indexTable = registerSetIndex(setTable, setReadGate);
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());
        final SharedSetKernel shared = filter.sharedSet();

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            TstUtils.addToTable(indexTable, i(1), intCol(KEY, 2),
                    col(ROW_SET_COLUMN, (RowSet) RowSetFactory.fromKeys(1)));
            indexTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();

            // Read the kernel from another thread once the set listener has parked between its two generation
            // increments, then let the listener finish.
            setReadGate.arm();
            final MutableObject<Throwable> readFailure = new MutableObject<>();
            final Future<?> readerFuture = pool.submit(() -> {
                try {
                    assertTrue("the set listener parked mid-mutation",
                            setReadGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));
                    try {
                        shared.beginRead();
                    } catch (final Throwable t) {
                        readFailure.setValue(t);
                    }
                } finally {
                    setReadGate.release();
                }
                return null;
            });

            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }
            readerFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertTrue("a read begun during the mutation must abandon its attempt: " + readFailure.getValue(),
                    readFailure.getValue() instanceof ConstructSnapshot.SnapshotInconsistentException);

            // The mutation is complete, so the generation is even again and a read proceeds normally.
            shared.beginRead();
            updateGraph.completeCycleForUnitTests();
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A set change that arrives while a {@code where} is filtering through a data index abandons that attempt too. The
     * index paths build their result incrementally, so the abandoned attempt must close what it built rather than leak
     * it, and the retry must produce the answer the new set implies.
     */
    @Test
    public void testSetChangeDuringIndexFilteringAbandonsTheAttempt() throws Exception {
        final Gate lookupGate = new Gate();
        final GatedSourceIndex[] indexOut = new GatedSourceIndex[1];
        final QueryTable source = indexedSourceTable(indexOut, lookupGate);
        final GatedSourceIndex index = indexOut[0];
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Nothing is satisfied yet, so this attempt reads previous values through the index.
            lookupGate.arm();
            final Future<Table> whereFuture = pool.submit(() -> source.where(filter));
            assertTrue("where began looking keys up in the index",
                    lookupGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }
            lookupGate.release();

            final Table result;
            try {
                result = whereFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new AssertionError("where failed when its set ticked during index filtering: " + e.getCause(),
                        e.getCause());
            }

            assertTrue("the abandoned attempt and the retry must both look keys up", index.lookups.get() >= 2);
            assertEquals("every row matches the retried set", source.size(), result.size());
            updateGraph.completeCycleForUnitTests();
            assertTableEquals(source, result);
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A failure of the set table fails every result sharing that set, not just the first. One listener now maintains
     * the set for all of them, so a failure has to be fanned back out to each filter.
     */
    @Test
    public void testSetTableFailureReachesEverySharingResult() {
        final QueryTable source = sourceTable(new GatedIntegerArraySource(new Gate()), true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final QueryTable indexTable = registerSetIndex(setTable, new Gate());
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        final Table first = source.where(filter.copy());
        final Table second = source.where(filter.copy());
        assertEquals(2, filter.sharedSet().registeredFilterCount());
        assertFalse(first.isFailed());
        assertFalse(second.isFailed());

        try (final SafeCloseable ignored = base.new ErrorExpectation()) {
            updateGraph.runWithinUnitTestCycle(() -> indexTable.notifyListenersOnError(
                    new RuntimeException("set table failure"), null));
        }

        assertTrue("the first result must fail with its set", first.isFailed());
        assertTrue("the second result must fail with its set", second.isFailed());
    }

    /**
     * A modified set key is removed and re-added, and the results refilter accordingly. An update that leaves the keys
     * alone must instead report no state change at all, so that concurrent previous-value snapshots are not retried for
     * a kernel that did not move.
     */
    @Test
    public void testSetModifyRewritesKeysAndAnIrrelevantSetUpdateDoesNot() {
        final QueryTable source = sourceTable(new GatedIntegerArraySource(new Gate()), true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final QueryTable indexTable = registerSetIndex(setTable, new Gate());
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        final Table result = source.where(filter);
        assertTableEquals(newTable(intCol(KEY, 1)), result);

        // Modifying the key replaces it in the kernel: the old key's rows leave and the new key's arrive.
        final long[] modifyStep = new long[1];
        updateGraph.runWithinUnitTestCycle(() -> {
            modifyStep[0] = updateGraph.clock().currentStep();
            TstUtils.addToTable(indexTable, i(0), intCol(KEY, 3),
                    col(ROW_SET_COLUMN, (RowSet) RowSetFactory.fromKeys(0)));
            indexTable.notifyListeners(i(), i(), i(0));
        });
        assertEquals("a key change is a kernel change", modifyStep[0], filter.lastStateChangeStep());
        assertTableEquals(newTable(intCol(KEY, 3)), result);

        // An update that touches only the row set column leaves the keys alone.
        final long[] quietStep = new long[1];
        updateGraph.runWithinUnitTestCycle(() -> {
            quietStep[0] = updateGraph.clock().currentStep();
            indexTable.notifyListeners(new TableUpdateImpl(i(), i(), i(0), RowSetShiftData.EMPTY,
                    indexTable.newModifiedColumnSet(ROW_SET_COLUMN)));
        });
        assertNotEquals("a modify that leaves every key alone must not report a kernel change",
                quietStep[0], filter.lastStateChangeStep());
        assertTableEquals(newTable(intCol(KEY, 3)), result);
    }

    /**
     * {@code wouldMatch} on a refreshing table initializes from previous values when neither the table nor the filter
     * is satisfied yet, and the result still tracks both once the cycle finishes. This is the refreshing counterpart of
     * {@link #testStaticWouldMatchIsProtectedFromRefreshingSet()}: here the parent's previous row set is read too.
     */
    @Test
    public void testRefreshingWouldMatchInitializesFromPreviousValues() throws Exception {
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(new Gate());
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Neither the source nor the filter is satisfied, so the operation reads previous values.
            final Table result = pool.submit(() -> source.wouldMatch(new WouldMatchPair("M", filter)))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertTableEquals(newTable(intCol(KEY, 1, 2, 3), booleanCol("M", true, false, false)), result);

            // The result tracks the source from there, on the same cycle it was built in.
            addSourceRow(source, sourceKey, 3, 1);
            source.notifyListeners(i(3), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!result.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }
            updateGraph.completeCycleForUnitTests();

            assertTableEquals(newTable(intCol(KEY, 1, 2, 3, 1), booleanCol("M", true, false, false, true)), result);
        } finally {
            endCycleIfOpen();
        }
    }
    // region Attempts that cannot commit over the set they read (DH-23666)

    /** @return Whether {@code thrown}, or anything it was caused by, is an instance of {@code causeType} */
    private static boolean hasCause(final Throwable thrown, final Class<? extends Throwable> causeType) {
        for (Throwable current = thrown; current != null; current = current.getCause()) {
            if (causeType.isInstance(current)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return false;
    }

    /**
     * Pump notifications until {@code future} completes, for an operation whose retry cannot proceed until the work
     * queued on this thread's update graph runs.
     */
    private void pumpUntilDone(@NotNull final Future<Table> future) {
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!future.isDone()) {
            if (System.nanoTime() > deadlineNanos) {
                fail("the operation did not complete after its dependencies were satisfied");
            }
            if (!updateGraph.flushOneNotificationForUnitTests()) {
                try {
                    // noinspection BusyWait
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while pumping notifications", e);
                }
            }
        }
    }

    /**
     * Run {@code operation} with its attempt parked between its verdict and its subscription, change the set while it
     * is parked, and return the result the caller eventually receives.
     * <p>
     * The attempt read the old set, and would begin following the new one having missed the change, so its subscription
     * refuses it. The retry reads the changed set instead, which is what the result must reflect. While the attempt is
     * parked nothing is registered with the set at all, so the change has no result to reach.
     */
    private Table assertSetChangeBeforeSubscriptionRejectsTheAttempt(
            @NotNull final BiFunction<QueryTable, DynamicWhereFilter, Table> operation) throws Exception {
        final Gate subscribeGate = new Gate();
        // Never armed; this source only counts reads, so that the retry can be asserted.
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(new Gate());
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new GateBeforeSubscribeFilter(setTable, subscribeGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Nothing is satisfied yet, so this attempt reads previous values.
            subscribeGate.arm();
            final Future<Table> future = pool.submit(() -> operation.apply(source, filter));
            assertTrue("the attempt reached its subscription",
                    subscribeGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));
            assertEquals("the attempt has read the source exactly once", 1, sourceKey.chunkReads.get());
            assertEquals("an attempt that has not committed follows nothing", 0,
                    filter.sharedSet().registeredFilterCount());

            // Change the set while the attempt is parked: its verdict is already given, so only the subscription can
            // refuse it now.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }

            subscribeGate.release();
            pumpUntilDone(future);
            final Table result = future.get();

            assertEquals("the refused attempt was retried against the changed set", 2, sourceKey.chunkReads.get());
            assertEquals("the committed attempt is the one following the set", 1,
                    filter.sharedSet().registeredFilterCount());
            updateGraph.completeCycleForUnitTests();
            return result;
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A {@code where} whose set changes between its verdict and its subscription is refused and retried, so the result
     * the caller receives already reflects the change rather than catching up to it later.
     */
    @Test
    public void testSetChangeBeforeSubscriptionRejectsTheWhereAttempt() throws Exception {
        final Table result = assertSetChangeBeforeSubscriptionRejectsTheAttempt(QueryTable::where);
        assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
    }

    /** The same guarantee for {@code wouldMatch}, which commits through the same snapshot control. */
    @Test
    public void testSetChangeBeforeSubscriptionRejectsTheWouldMatchAttempt() throws Exception {
        final Table result = assertSetChangeBeforeSubscriptionRejectsTheAttempt(
                (source, filter) -> source.wouldMatch(new WouldMatchPair("M", filter)));
        assertTableEquals(newTable(intCol(KEY, 1, 2, 3), booleanCol("M", true, true, false)), result);
    }

    /**
     * Run {@code operation} with its attempt parked between its verdict and its subscription, and fail the set while it
     * is parked.
     * <p>
     * No result over a dead set could ever follow it, so the subscription refuses outright rather than retrying, and
     * the caller is told by the operation throwing. Nothing is left registered with the set, and the caller never
     * receives a table.
     */
    private void assertSetFailureBeforeSubscriptionThrows(
            @NotNull final BiFunction<QueryTable, DynamicWhereFilter, Table> operation) throws Exception {
        final Gate subscribeGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(new Gate());
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final QueryTable indexTable = registerSetIndex(setTable, new Gate());
        final DynamicWhereFilter filter = new GateBeforeSubscribeFilter(setTable, subscribeGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            subscribeGate.arm();
            final Future<Table> future = pool.submit(() -> operation.apply(source, filter));
            assertTrue("the attempt reached its subscription",
                    subscribeGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            final RuntimeException setError = new RuntimeException("set table failure");
            try (final SafeCloseable ignored = base.new ErrorExpectation()) {
                indexTable.notifyListenersOnError(setError, null);
                updateGraph.markSourcesRefreshedForUnitTests();
                while (!filter.satisfied(step)) {
                    assertTrue(updateGraph.flushOneNotificationForUnitTests());
                }
            }

            subscribeGate.release();
            pumpUntilDone(future);

            final ExecutionException failure = assertThrows(ExecutionException.class, future::get);
            assertTrue("the operation must fail because its set failed: " + failure.getCause(),
                    hasCause(failure, TableAlreadyFailedException.class));
            assertEquals("an operation that could not be built follows nothing", 0,
                    filter.sharedSet().registeredFilterCount());
            updateGraph.completeCycleForUnitTests();

            for (final Throwable reported : base.getUpdateErrors()) {
                assertSame("only the set's own failure may be reported", setError, reported);
            }
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A {@code where} whose set fails between its verdict and its subscription throws, rather than handing back a table
     * that can never follow its set.
     */
    @Test
    public void testSetFailureBeforeSubscriptionFailsTheWhere() throws Exception {
        assertSetFailureBeforeSubscriptionThrows(QueryTable::where);
    }

    /** The same guarantee for {@code wouldMatch}. */
    @Test
    public void testSetFailureBeforeSubscriptionFailsTheWouldMatch() throws Exception {
        assertSetFailureBeforeSubscriptionThrows(
                (source, filter) -> source.wouldMatch(new WouldMatchPair("M", filter)));
    }

    /**
     * A filter whose set has already failed cannot be used to build anything: {@code where} and {@code wouldMatch} over
     * it throw, exactly as listening to the failed set directly would, rather than returning a refreshing table that
     * can never again follow its set.
     */
    @Test
    public void testOperationsOnAFilterWhoseSetHasFailedThrow() {
        final QueryTable source = sourceTable(new GatedIntegerArraySource(new Gate()), true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final QueryTable indexTable = registerSetIndex(setTable, new Gate());
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        try (final SafeCloseable ignored = base.new ErrorExpectation()) {
            updateGraph.runWithinUnitTestCycle(() -> indexTable.notifyListenersOnError(
                    new RuntimeException("set table failure"), null));
        }

        assertThrows(TableAlreadyFailedException.class, () -> source.where(filter));
        assertThrows(TableAlreadyFailedException.class,
                () -> source.wouldMatch(new WouldMatchPair("M", filter.copy())));
        assertEquals("a filter that could not be used must not follow its set",
                0, filter.sharedSet().registeredFilterCount());
    }

    /**
     * Run {@code operation} to completion on an open cycle with nothing ticking, so that its attempt commits and hands
     * out a result, and then fail the set on that same step. The attempt is over and the filter is following its set,
     * so it is the handed-out result the failure must reach. That result carries the step of the previous values it
     * read, so it can be failed at once, exactly once.
     * <p>
     * There is no current-values counterpart: an attempt reads current values only once the set listener is satisfied
     * for the step, after which the set can neither change nor fail on it.
     */
    private void assertSetFailureAfterCommitFailsResultAtOnce(
            @NotNull final BiFunction<QueryTable, DynamicWhereFilter, Table> operation,
            @NotNull final Table expected) throws Exception {
        final QueryTable source = sourceTable(new GatedIntegerArraySource(new Gate()), true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final QueryTable indexTable = registerSetIndex(setTable, new Gate());
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            final Table result =
                    pool.submit(() -> operation.apply(source, filter)).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEquals("the result carries the step of the previous values it read", step - 1,
                    ((QueryTable) result).getLastNotificationStep());
            assertTableEquals(expected, result);
            final FailureRecordingListener failures = new FailureRecordingListener(result);

            final RuntimeException setError = new RuntimeException("set table failure");
            try (final SafeCloseable ignored = base.new ErrorExpectation()) {
                indexTable.notifyListenersOnError(setError, null);
                updateGraph.markSourcesRefreshedForUnitTests();
                while (!filter.satisfied(step)) {
                    assertTrue(updateGraph.flushOneNotificationForUnitTests());
                }
                // noinspection StatementWithEmptyBody
                while (updateGraph.flushOneNotificationForUnitTests()) {
                }
            }

            assertTrue("a result carrying an earlier step fails on this one", result.isFailed());
            failures.assertFailedOnceWith(setError);
            for (final Throwable reported : base.getUpdateErrors()) {
                assertSame("only the set's own failure may be reported", setError, reported);
            }
            updateGraph.completeCycleForUnitTests();
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A set failure that lands on a committed {@code where} result's own creating step fails that result then and
     * there, exactly once and with the set's own error.
     */
    @Test
    public void testSetFailureAfterCommitFailsTheWhereResultAtOnce() throws Exception {
        assertSetFailureAfterCommitFailsResultAtOnce(QueryTable::where, newTable(intCol(KEY, 1)));
    }

    /** The same guarantee for {@code wouldMatch}. */
    @Test
    public void testSetFailureAfterCommitFailsTheWouldMatchResultAtOnce() throws Exception {
        assertSetFailureAfterCommitFailsResultAtOnce(
                (source, filter) -> source.wouldMatch(new WouldMatchPair("M", filter)),
                newTable(intCol(KEY, 1, 2, 3), booleanCol("M", true, false, false)));
    }

    /**
     * Run {@code operation} with its attempt parked between subscribing to its set and subscribing its result to the
     * source, tick the source so that second subscription is refused, and change the set while it is parked.
     * <p>
     * This is the one window in which a set change reaches a result whose attempt is still going to be rejected. This
     * test holds the recompute it queues until that attempt has been released, and it must then do nothing: refiltering
     * would read the source for a table nobody holds, and, for {@code wouldMatch}, a listener that read the operation's
     * fields rather than its own attempt's would act on the retried result instead. (A recompute that runs before the
     * release refilters a table that is about to be released, which wastes work but harms nothing; it is not what this
     * test pins.)
     */
    private Table assertRecomputeForARejectedAttemptDoesNothing(
            @NotNull final BiFunction<QueryTable, DynamicWhereFilter, Table> operation) throws Exception {
        final Gate sourceGate = new Gate();
        final Gate subscribeGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new GateAfterSubscribeFilter(setTable, subscribeGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Nothing is satisfied yet, so this attempt reads previous values.
            subscribeGate.arm();
            final Future<Table> future = pool.submit(() -> operation.apply(source, filter));
            assertTrue("the attempt subscribed to its set",
                    subscribeGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));
            assertEquals("the attempt is following its set", 1, filter.sharedSet().registeredFilterCount());

            // Tick the source, so this attempt's own subscription to it is refused and the attempt is rejected.
            addSourceRow(source, sourceKey, 3, 4);
            source.notifyListeners(i(3), i(), i());

            // Change the set, which the attempt is following, so a recompute is queued for its doomed result.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }

            // Let the attempt be rejected and released, and park the retry at its read. Reaching that read is what
            // proves the release happened, so the queued recompute below runs against a result that is already gone.
            sourceGate.arm();
            subscribeGate.release();
            assertTrue("the retry began its read",
                    sourceGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            // noinspection StatementWithEmptyBody
            while (updateGraph.flushOneNotificationForUnitTests()) {
            }
            assertEquals("the released result must not be refiltered", 1, sourceKey.chunkReads.get());

            sourceGate.release();
            pumpUntilDone(future);
            final Table result = future.get();

            assertEquals("only the rejected attempt and the retry may read the source", 2,
                    sourceKey.chunkReads.get());
            assertEquals("only the committed attempt is following the set", 1,
                    filter.sharedSet().registeredFilterCount());
            updateGraph.completeCycleForUnitTests();

            assertFalse("the retried result must not fail", result.isFailed());
            assertTrue("no error may be reported: " + base.getUpdateErrors(), base.getUpdateErrors().isEmpty());
            return result;
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A recompute queued for a {@code where} attempt that is rejected after it subscribed does nothing once that
     * attempt's result is released, and the retry's result is correct and unfailed.
     */
    @Test
    public void testRecomputeForARejectedWhereAttemptDoesNothing() throws Exception {
        final Table result = assertRecomputeForARejectedAttemptDoesNothing(QueryTable::where);
        assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
    }

    /**
     * The same guarantee for {@code wouldMatch}, whose listener must act on its own attempt's result and columns rather
     * than on the operation's fields, which the retry has since reassigned.
     */
    @Test
    public void testRecomputeForARejectedWouldMatchAttemptDoesNothing() throws Exception {
        final Table result = assertRecomputeForARejectedAttemptDoesNothing(
                (source, filter) -> source.wouldMatch(new WouldMatchPair("M", filter)));
        assertTableEquals(newTable(intCol(KEY, 1, 2, 3, 4), booleanCol("M", true, true, false, false)), result);
    }

    /**
     * A where listener whose filters run through the update graph's job scheduler finishes its work on notifications of
     * its own, after its own notification has returned. A recompute that reaches a doomed result while it is still
     * alive can therefore be in flight when that result's attempt is rejected and released. The listener must hold the
     * result until that work completes, so the work runs against a live table, and release it afterwards, so the table
     * does not leak.
     */
    @Test
    public void testRefilterInFlightForARejectedWhereAttemptOutlivesItsRelease() throws Exception {
        final Gate sourceGate = new Gate();
        final Gate subscribeGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final GateAfterSubscribeFilter filter = new GateAfterSubscribeFilter(setTable, subscribeGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try (final SafeCloseable ignored = new ForcedParallelWhere()) {
            // Nothing is satisfied yet, so this attempt reads previous values.
            subscribeGate.arm();
            final Future<Table> future = pool.submit(() -> source.where(filter));
            assertTrue("the attempt subscribed to its set",
                    subscribeGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));
            final QueryTable rejectedResult = filter.firstResult;
            assertNotNull(rejectedResult);

            // Tick the source, so this attempt's own subscription to it is refused and the attempt is rejected.
            addSourceRow(source, sourceKey, 3, 4);
            source.notifyListeners(i(3), i(), i());

            // Change the set, which the attempt is following, so a recompute is queued for its doomed result.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }

            // Run the where listener now, while its result is alive. It schedules its filter work on the update graph
            // and returns; that work has not run yet.
            assertTrue("the where listener's notification was queued", updateGraph.flushOneNotificationForUnitTests());
            assertEquals("the filter work has not run yet", 1, sourceKey.chunkReads.get());

            // Let the attempt be rejected and released, and park the retry at its read, which proves the release.
            sourceGate.arm();
            subscribeGate.release();
            assertTrue("the retry began its read",
                    sourceGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            assertTrue("the listener must hold its result while its filter work is in flight",
                    rejectedResult.tryRetainReference());
            rejectedResult.dropReference();

            // Run the filter work. It reads the source for a table that is alive, and the listener then lets go of it.
            // noinspection StatementWithEmptyBody
            while (updateGraph.flushOneNotificationForUnitTests()) {
            }
            assertEquals("the in-flight refilter read the source once", 2, sourceKey.chunkReads.get());
            assertFalse("the listener must release its result once its filter work is done",
                    rejectedResult.tryRetainReference());

            sourceGate.release();
            pumpUntilDone(future);
            final Table result = future.get();
            assertEquals("the retry read the source once", 3, sourceKey.chunkReads.get());
            updateGraph.completeCycleForUnitTests();

            assertFalse("the retried result must not fail", result.isFailed());
            assertTrue("no error may be reported: " + base.getUpdateErrors(), base.getUpdateErrors().isEmpty());
            assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * Run {@code operation} with its attempt parked between subscribing to its set and subscribing its result to the
     * source, tick the source so that second subscription is refused, and fail the set while it is parked.
     * <p>
     * The failure reaches the rejected attempt's result through its listener. This test holds that listener until the
     * attempt has been released, and it must then leave the released result alone: nobody was handed that table, so
     * failing it would only report the set's error against it. The retry finds the set failed, and the operation
     * throws.
     */
    private void assertSetFailureForARejectedAttemptFailsNothing(
            @NotNull final BiFunction<QueryTable, DynamicWhereFilter, Table> operation) throws Exception {
        final Gate sourceGate = new Gate();
        final Gate subscribeGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final QueryTable indexTable = registerSetIndex(setTable, new Gate());
        final GateAfterSubscribeFilter filter = new GateAfterSubscribeFilter(setTable, subscribeGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Nothing is satisfied yet, so this attempt reads previous values.
            subscribeGate.arm();
            final Future<Table> future = pool.submit(() -> operation.apply(source, filter));
            assertTrue("the attempt subscribed to its set",
                    subscribeGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));
            final QueryTable rejectedResult = filter.firstResult;
            assertNotNull(rejectedResult);

            // Tick the source, so this attempt's own subscription to it is refused and the attempt is rejected.
            addSourceRow(source, sourceKey, 3, 4);
            source.notifyListeners(i(3), i(), i());

            final RuntimeException setError = new RuntimeException("set table failure");
            try (final SafeCloseable ignored = base.new ErrorExpectation()) {
                // Fail the set, which the attempt is following, so a failure is queued for its doomed result.
                indexTable.notifyListenersOnError(setError, null);
                updateGraph.markSourcesRefreshedForUnitTests();
                while (!filter.satisfied(step)) {
                    assertTrue(updateGraph.flushOneNotificationForUnitTests());
                }

                // Let the attempt be rejected and released, and park the retry at its read. Reaching that read is what
                // proves the release happened, so the queued failure below runs against a result that is already gone.
                sourceGate.arm();
                subscribeGate.release();
                assertTrue("the retry began its read",
                        sourceGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

                // noinspection StatementWithEmptyBody
                while (updateGraph.flushOneNotificationForUnitTests()) {
                }
                assertFalse("the rejected attempt's result must have been released",
                        rejectedResult.tryRetainReference());
                assertFalse("a result that was never handed out must not be failed", rejectedResult.isFailed());

                // The retry reaches its commit and finds the set failed.
                sourceGate.release();
                pumpUntilDone(future);
                final ExecutionException failure = assertThrows(ExecutionException.class, future::get);
                assertTrue("the operation must fail because its set failed: " + failure.getCause(),
                        hasCause(failure, TableAlreadyFailedException.class));
                assertEquals("an operation that could not be built follows nothing", 0,
                        filter.sharedSet().registeredFilterCount());
                updateGraph.completeCycleForUnitTests();
            }
            for (final Throwable reported : base.getUpdateErrors()) {
                assertSame("only the set's own failure may be reported", setError, reported);
            }
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A set failure queued for a {@code where} attempt that is rejected after it subscribed leaves that attempt's
     * released result alone, and the operation throws.
     */
    @Test
    public void testSetFailureForARejectedWhereAttemptFailsNothing() throws Exception {
        assertSetFailureForARejectedAttemptFailsNothing(QueryTable::where);
    }

    /** The same guarantee for {@code wouldMatch}. */
    @Test
    public void testSetFailureForARejectedWouldMatchAttemptFailsNothing() throws Exception {
        assertSetFailureForARejectedAttemptFailsNothing(
                (source, filter) -> source.wouldMatch(new WouldMatchPair("M", filter)));
    }

    /**
     * Run {@code operation} with its source satisfied and its set listener still pending, so that the attempt must wait
     * for that listener, and run the listener while the attempt is parked just before its wait.
     * <p>
     * The listener changes the set, and the attempt then reads current values, which include that change. Its commit
     * must accept what it read rather than refuse it for having changed since the attempt began: the source is read
     * once, and the result follows the set from then on.
     */
    private Table assertWaitingAttemptCommitsOnItsFirstRead(
            @NotNull final BiFunction<QueryTable, DynamicWhereFilter, Table> operation) throws Exception {
        final Gate unsatisfiedGate = new Gate();
        // Never armed; this source only counts reads, so that the single read can be asserted.
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(new Gate());
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new GateWhenUnsatisfiedFilter(setTable, unsatisfiedGate);

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Tick the set, so that its listener is pending, and refresh the sources, so that the source is satisfied.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();

            unsatisfiedGate.arm();
            final Future<Table> future = pool.submit(() -> operation.apply(source, filter));
            assertTrue("the attempt found the set unsatisfied",
                    unsatisfiedGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));
            assertEquals("the attempt has not read the source", 0, sourceKey.chunkReads.get());

            // Run the set listener while the attempt is parked, so that the wait it is about to begin is satisfied.
            while (!filter.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }
            assertEquals("the set changed on this step", step, filter.lastStateChangeStep());

            unsatisfiedGate.release();
            pumpUntilDone(future);
            final Table result = future.get();

            assertEquals("the attempt read the source once and committed", 1, sourceKey.chunkReads.get());
            assertEquals("the committed attempt is following the set", 1,
                    filter.sharedSet().registeredFilterCount());
            updateGraph.completeCycleForUnitTests();
            return result;
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A {@code where} that waits for the set listener commits on its first read, with the result reflecting the change
     * that listener made.
     */
    @Test
    public void testWhereWaitingForTheSetListenerCommitsOnItsFirstRead() throws Exception {
        final Table result = assertWaitingAttemptCommitsOnItsFirstRead(QueryTable::where);
        assertTableEquals(newTable(intCol(KEY, 1, 2)), result);
    }

    /** The same guarantee for {@code wouldMatch}, which commits through the same snapshot control. */
    @Test
    public void testWouldMatchWaitingForTheSetListenerCommitsOnItsFirstRead() throws Exception {
        final Table result = assertWaitingAttemptCommitsOnItsFirstRead(
                (source, filter) -> source.wouldMatch(new WouldMatchPair("M", filter)));
        assertTableEquals(newTable(intCol(KEY, 1, 2, 3), booleanCol("M", true, true, false)), result);
    }

    /**
     * Run {@code operation} with its attempt parked in its previous-values read of the source, and fail the set while
     * it is parked.
     * <p>
     * The attempt read a set that then died on its own step, so its completion check refuses it, the same way it
     * refuses a set change on that step. The retry reads current values and is refused at its commit, because no result
     * over a dead set can follow it, so the caller is told by the operation throwing. Nothing is left registered with
     * the set, and the caller never receives a table that would silently stop following its set.
     */
    private void assertSetFailureDuringTheReadFailsTheOperation(
            @NotNull final BiFunction<QueryTable, DynamicWhereFilter, Table> operation) throws Exception {
        final Gate sourceGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, true, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final QueryTable indexTable = registerSetIndex(setTable, new Gate());
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            // Nothing is satisfied yet, so this attempt reads previous values.
            sourceGate.arm();
            final Future<Table> future = pool.submit(() -> operation.apply(source, filter));
            assertTrue("the attempt began its previous-values read",
                    sourceGate.awaitReached(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)));

            final RuntimeException setError = new RuntimeException("set table failure");
            try (final SafeCloseable ignored = base.new ErrorExpectation()) {
                indexTable.notifyListenersOnError(setError, null);
                updateGraph.markSourcesRefreshedForUnitTests();
                while (!filter.satisfied(step)) {
                    assertTrue(updateGraph.flushOneNotificationForUnitTests());
                }
            }
            assertEquals("a failure is a state change on its step", step, filter.lastStateChangeStep());

            sourceGate.release();
            pumpUntilDone(future);

            final ExecutionException failure = assertThrows(ExecutionException.class, future::get);
            assertTrue("the operation must fail because its set failed: " + failure.getCause(),
                    hasCause(failure, TableAlreadyFailedException.class));
            assertEquals("the refused attempt was retried once, and that retry was refused at its commit", 2,
                    sourceKey.chunkReads.get());
            assertEquals("an operation that could not be built follows nothing", 0,
                    filter.sharedSet().registeredFilterCount());
            updateGraph.completeCycleForUnitTests();

            for (final Throwable reported : base.getUpdateErrors()) {
                assertSame("only the set's own failure may be reported", setError, reported);
            }
        } finally {
            endCycleIfOpen();
        }
    }

    /**
     * A {@code where} whose set fails while it is reading throws, rather than handing back a table built from a set
     * that can never again notify it.
     */
    @Test
    public void testSetFailureDuringTheReadFailsTheWhere() throws Exception {
        assertSetFailureDuringTheReadFailsTheOperation(QueryTable::where);
    }

    /** The same guarantee for {@code wouldMatch}. */
    @Test
    public void testSetFailureDuringTheReadFailsTheWouldMatch() throws Exception {
        assertSetFailureDuringTheReadFailsTheOperation(
                (source, filter) -> source.wouldMatch(new WouldMatchPair("M", filter)));
    }

    // endregion Attempts that cannot commit over the set they read (DH-23666)
}
