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
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndexOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.WouldMatchPair;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.dataindex.AbstractDataIndex;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Deterministic races for the lock-free {@link DynamicWhereFilter}.
 * <p>
 * Each test parks a table operation at a known point on a worker thread (a {@link Gate} inside a column source or a
 * table's listener registration), drives the update graph from the test thread while the operation is parked, and then
 * lets the operation continue. The update graph is a {@link ControlledUpdateGraph}, so the set table's listener runs
 * only when the test flushes it.
 */
@Category(OutOfBandTest.class)
public class TestDynamicWhereFilterSnapshotRaces {

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
     * {@code wouldMatch} on a static table initializes under snapshot control when a filter has refreshing
     * dependencies, as a {@code where} on the same table does. A {@link DynamicWhereFilter} over a refreshing set is
     * therefore read consistently with the set listener for the step: the operation waits for the listener, and the
     * result reflects the set as of that step.
     */
    @Test
    public void testStaticWouldMatchIsProtectedFromRefreshingSet() throws Exception {
        final Gate sourceGate = new Gate();
        final GatedIntegerArraySource sourceKey = new GatedIntegerArraySource(sourceGate);
        final QueryTable source = sourceTable(sourceKey, false, 1, 2, 3);
        final QueryTable setTable = TstUtils.testRefreshingTable(i(0).toTracking(), intCol(KEY, 1));
        final DynamicWhereFilter filter = new DynamicWhereFilter(setTable, true, pairs());

        updateGraph.startCycleForUnitTests(false);
        final long step = updateGraph.clock().currentStep();
        try {
            sourceGate.arm();
            final Future<Table> matchFuture = pool.submit(() -> source.wouldMatch(new WouldMatchPair("M", filter)));

            // Change the set on this step. The set listener has not run yet.
            TstUtils.addToTable(setTable, i(1), intCol(KEY, 2));
            setTable.notifyListeners(i(1), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();

            if (sourceGate.awaitReached(1_000)) {
                // The operation reached the kernel before the set listener ran. Run the listener under it.
                while (!filter.satisfied(step)) {
                    assertTrue(updateGraph.flushOneNotificationForUnitTests());
                }
            } else {
                // The operation is waiting for the set listener. Let the listener run, then wake it.
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
                result = matchFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new AssertionError("wouldMatch on a static table failed when its set ticked: " + e.getCause(),
                        e.getCause());
            }
            updateGraph.completeCycleForUnitTests();
            assertTableEquals(newTable(intCol(KEY, 1, 2, 3), booleanCol("M", true, true, false)), result);
        } finally {
            endCycleIfOpen();
        }
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
        ConstructSnapshot.callDataSnapshotFunction("TestDynamicWhereFilterSnapshotRaces",
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
}
