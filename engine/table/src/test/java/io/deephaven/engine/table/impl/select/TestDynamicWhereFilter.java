//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
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
            assertFalse(filter.stateChangedOnStep(step));
            final long generation = shared.beginRead();

            // The only queued notification belongs to the discarded attempt's listener. Let it run.
            updateGraph.markSourcesRefreshedForUnitTests();
            assertTrue(updateGraph.flushOneNotificationForUnitTests());

            assertFalse("a discarded attempt's listener must not report a state change for the committed kernel",
                    filter.stateChangedOnStep(step));
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
     * A filter registers with its shared set inside every snapshot attempt, so between a failed attempt and its retry
     * the filter still points at the failed attempt's released result. A set change in that window does not refilter
     * that result: the failed attempt's read and the retry's read are the only reads the source sees.
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
            assertTrue("the set changed on this step", filter.stateChangedOnStep(step));

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
        assertTrue("a key change is a kernel change", filter.stateChangedOnStep(modifyStep[0]));
        assertTableEquals(newTable(intCol(KEY, 3)), result);

        // An update that touches only the row set column leaves the keys alone.
        final long[] quietStep = new long[1];
        updateGraph.runWithinUnitTestCycle(() -> {
            quietStep[0] = updateGraph.clock().currentStep();
            indexTable.notifyListeners(new TableUpdateImpl(i(), i(), i(0), RowSetShiftData.EMPTY,
                    indexTable.newModifiedColumnSet(ROW_SET_COLUMN)));
        });
        assertFalse("a modify that leaves every key alone must not report a kernel change",
                filter.stateChangedOnStep(quietStep[0]));
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
}
