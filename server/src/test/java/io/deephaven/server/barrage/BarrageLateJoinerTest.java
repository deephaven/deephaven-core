//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

/**
 * Round-trip coverage for subscribers that join a {@link BarrageMessageProducer} while other subscribers already have
 * deltas queued.
 *
 * <p>
 * Subscribing is not only a request for data, it is also a flush: both {@link BarrageMessageProducer#addSubscription}
 * and {@link BarrageMessageProducer#updateSubscription} schedule the update propagation job immediately rather than
 * waiting out the subscriber update interval. That job then has to serve two populations from one queue of pending
 * deltas, and it does so by splitting the queue at the step the snapshot was taken on:
 * <ul>
 * <li>deltas at or before the snapshot step are coalesced and sent only to the <em>existing</em> subscribers, under the
 * viewports and columns they had before this job ran; the newcomer must not receive them, because its snapshot already
 * reflects them;</li>
 * <li>the snapshot goes to the newcomer alone;</li>
 * <li>deltas recorded after the snapshot step (i.e. while the snapshot was being built) are coalesced and sent to
 * everyone, the newcomer included.</li>
 * </ul>
 * The queue is emptied by that job, so a subsequent join starts from a clean queue -- pending deltas never span two
 * subscription generations except transiently, inside a single run of the job.
 *
 * <p>
 * These tests pin that behavior down before it acquires a second consumer. DH-21949 adds proactive coalescing of the
 * pending deltas, which must preserve the split exactly: a compacted delta may never merge data recorded before a
 * snapshot with data recorded after it.
 */
@Category(OutOfBandTest.class)
public class BarrageLateJoinerTest extends BarrageMessageRoundTripTestBase {

    /**
     * Small enough that a snapshot of the whole table fits in a single message (well under
     * {@code BarrageUtil.MIN_SNAPSHOT_CELL_COUNT} cells), so message-count assertions stay exact.
     */
    private static final int TABLE_SIZE = 100;

    private static final int NUM_COLUMNS = 3;

    /** Bumped on every tick so each update writes values no previous tick produced. */
    private int tickCounter;

    /** Row keys {@link #churnRows} adds live here, far from the table's initial rows and anything a shift reaches. */
    private static final long CHURN_KEY_BASE = 1L << 20;
    /** Every this many churn cycles, {@link #churnRows} also shifts the second half of the table. */
    private static final int SHIFT_EVERY = 8;
    private int churnCount;
    /** The row key the previous churn cycle added, or -1. */
    private long lastChurnKey;
    /** How far the second half of the table has been shifted up so far. */
    private int shiftOffset;

    private static BitSet allColumns() {
        final BitSet columns = new BitSet();
        columns.set(0, NUM_COLUMNS);
        return columns;
    }

    private static ControlledUpdateGraph updateGraph() {
        return ExecutionContext.getContext().getUpdateGraph().cast();
    }

    private QueryTable newSourceTable() {
        return newSourceTable(TABLE_SIZE);
    }

    private QueryTable newSourceTable(final int size) {
        tickCounter = 0;
        churnCount = 0;
        lastChurnKey = -1;
        shiftOffset = 0;
        final int[] intValues = new int[size];
        final double[] doubleValues = new double[size];
        final String[] stringValues = new String[size];
        for (int ii = 0; ii < size; ++ii) {
            intValues[ii] = ii;
            doubleValues[ii] = ii * 0.5;
            stringValues[ii] = "v" + ii;
        }
        return TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(),
                TableTools.intCol("intCol", intValues),
                TableTools.doubleCol("doubleCol", doubleValues),
                TableTools.stringCol("strCol", stringValues));
    }

    /**
     * Runs one update graph cycle that modifies {@code rowsToModify} in every column. The producer records one delta
     * per such cycle; no propagation happens until {@link #flushProducerTable()} is called.
     */
    private void modifyRows(final QueryTable sourceTable, final RowSet rowsToModify) {
        final int tick = ++tickCounter;
        updateGraph().runWithinUnitTestCycle(() -> {
            writeRows(sourceTable, rowsToModify, tick);
            sourceTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(), RowSetFactory.empty(), rowsToModify.copy(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
        });
    }

    /** Writes values derived from {@code tick} into every column of {@code rows}; must run inside a cycle. */
    private static void writeRows(final QueryTable sourceTable, final RowSet rows, final int tick) {
        writeColumns(sourceTable, rows, tick, "intCol", "doubleCol", "strCol");
    }

    /**
     * Writes values derived from {@code tick} into the named columns of {@code rows}, re-writing the other columns with
     * the values they already hold (the test table insists every column be supplied); must run inside a cycle. Rows
     * that do not exist yet must name every column.
     */
    private static void writeColumns(final QueryTable sourceTable, final RowSet rows, final int tick,
            final String... columnNames) {
        final Set<String> written = Set.of(columnNames);
        final ColumnSource<?> intSource = sourceTable.getColumnSource("intCol");
        final ColumnSource<?> doubleSource = sourceTable.getColumnSource("doubleCol");
        final ColumnSource<?> stringSource = sourceTable.getColumnSource("strCol");
        final int numRows = rows.intSize();
        final int[] intValues = new int[numRows];
        final double[] doubleValues = new double[numRows];
        final String[] stringValues = new String[numRows];
        int ii = 0;
        for (final RowSet.Iterator it = rows.iterator(); it.hasNext();) {
            final long rowKey = it.nextLong();
            intValues[ii] = written.contains("intCol") ? (int) (rowKey * 1000 + tick) : intSource.getInt(rowKey);
            doubleValues[ii] =
                    written.contains("doubleCol") ? rowKey + tick / 1000.0 : doubleSource.getDouble(rowKey);
            stringValues[ii] =
                    written.contains("strCol") ? "v" + rowKey + "-" + tick : (String) stringSource.get(rowKey);
            ++ii;
        }
        TstUtils.addToTable(sourceTable, rows,
                TableTools.intCol("intCol", intValues),
                TableTools.doubleCol("doubleCol", doubleValues),
                TableTools.stringCol("strCol", stringValues));
    }

    /** Runs one cycle that modifies only the named columns of {@code rows}, and says so in its modified column set. */
    private void modifyColumns(final QueryTable sourceTable, final RowSet rows, final String... columnNames) {
        final int tick = ++tickCounter;
        updateGraph().runWithinUnitTestCycle(() -> {
            writeColumns(sourceTable, rows, tick, columnNames);
            sourceTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(), RowSetFactory.empty(), rows.copy(),
                    RowSetShiftData.EMPTY, sourceTable.newModifiedColumnSet(columnNames)));
        });
    }

    /**
     * Runs one cycle modifying a subset of the columns on a row range that depends on which subset, so that over a run
     * of cycles each column accumulates its own recorded modifications: partly overlapping between columns, and every
     * fourth cycle identical for two of them, which is the case where columns share one recorded row set.
     */
    private void modifySomeColumns(final QueryTable sourceTable, final int cycle) {
        switch (cycle % 4) {
            case 0:
                try (final RowSet rows = RowSetFactory.fromRange(0, 29)) {
                    modifyColumns(sourceTable, rows, "intCol");
                }
                break;
            case 1:
                try (final RowSet rows = RowSetFactory.fromRange(20, 59)) {
                    modifyColumns(sourceTable, rows, "doubleCol");
                }
                break;
            case 2:
                try (final RowSet rows = RowSetFactory.fromRange(50, 89)) {
                    modifyColumns(sourceTable, rows, "strCol");
                }
                break;
            default:
                try (final RowSet rows = RowSetFactory.fromRange(10, 39)) {
                    modifyColumns(sourceTable, rows, "intCol", "doubleCol");
                }
                break;
        }
    }

    /**
     * Runs one update graph cycle carrying every kind of change, so that coalescing it with its neighbors has work to
     * do: the first half of the table is modified again, the row the previous churn cycle added is removed and a new
     * one added, and every {@link #SHIFT_EVERY} cycles the second half of the table is shifted up by one key. On the
     * other cycles a slice of the second half is modified too, so that a later shift moves rows that carry recorded
     * modifications and the mapping has to follow them.
     */
    private void churnRows(final QueryTable sourceTable) {
        final int tick = ++tickCounter;
        final int churn = ++churnCount;
        updateGraph().runWithinUnitTestCycle(() -> {
            final WritableRowSet removed = RowSetFactory.empty();
            if (lastChurnKey >= 0) {
                removed.insert(lastChurnKey);
                TstUtils.removeRows(sourceTable, removed);
            }
            final long addedKey = CHURN_KEY_BASE + churn;
            final RowSet added = RowSetFactory.fromKeys(addedKey);
            writeRows(sourceTable, added, tick);
            lastChurnKey = addedKey;

            final boolean shiftCycle = churn % SHIFT_EVERY == 0;
            final WritableRowSet modified = RowSetFactory.fromRange(0, TABLE_SIZE / 2 - 1);
            if (!shiftCycle) {
                // ten rows of the second half, where it currently sits; the next shift cycle moves them
                modified.insertRange(TABLE_SIZE / 2 + shiftOffset, TABLE_SIZE / 2 + shiftOffset + 9);
            }
            writeRows(sourceTable, modified, tick);

            final RowSetShiftData shifted = shiftCycle ? shiftSecondHalf(sourceTable) : RowSetShiftData.EMPTY;

            sourceTable.notifyListeners(new TableUpdateImpl(added, removed, modified, shifted, ModifiedColumnSet.ALL));
        });
    }

    /**
     * Moves the second half of the table up by one row key, keeping every value with its row, and returns the shift
     * that describes the move. Must run inside a cycle, before the update is published.
     */
    private RowSetShiftData shiftSecondHalf(final QueryTable sourceTable) {
        final long start = TABLE_SIZE / 2 + shiftOffset;
        final long end = TABLE_SIZE - 1 + shiftOffset;
        final int numRows = (int) (end - start + 1);

        final ColumnSource<?> intSource = sourceTable.getColumnSource("intCol");
        final ColumnSource<?> doubleSource = sourceTable.getColumnSource("doubleCol");
        final ColumnSource<?> stringSource = sourceTable.getColumnSource("strCol");
        final int[] intValues = new int[numRows];
        final double[] doubleValues = new double[numRows];
        final String[] stringValues = new String[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            final long rowKey = start + ii;
            intValues[ii] = intSource.getInt(rowKey);
            doubleValues[ii] = doubleSource.getDouble(rowKey);
            stringValues[ii] = (String) stringSource.get(rowKey);
        }

        try (final RowSet oldKeys = RowSetFactory.fromRange(start, end)) {
            TstUtils.removeRows(sourceTable, oldKeys);
        }
        try (final RowSet newKeys = RowSetFactory.fromRange(start + 1, end + 1)) {
            TstUtils.addToTable(sourceTable, newKeys,
                    TableTools.intCol("intCol", intValues),
                    TableTools.doubleCol("doubleCol", doubleValues),
                    TableTools.stringCol("strCol", stringValues));
        }
        ++shiftOffset;

        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        builder.shiftRange(start, end, 1);
        return builder.build();
    }

    /**
     * Runs the scheduler work that is due now, which is the compaction job the producer scheduled with
     * {@code runImmediately}, without running the propagation job, which is waiting out {@link #UPDATE_INTERVAL}.
     * Advances the simulated clock by one millisecond.
     */
    private void runDueJobs() {
        scheduler.runUntil(scheduler.timeAfterMs(1));
    }

    /**
     * The number of deltas the producer is holding. Read reflectively: the producer deliberately exposes no accessor
     * for its queue, and whether the queue was compacted is otherwise invisible, since a subscriber receives the same
     * message either way.
     */
    private static int pendingDeltaCount(final BarrageMessageProducer producer) {
        try {
            final Field field = BarrageMessageProducer.class.getDeclaredField("pendingDeltas");
            field.setAccessible(true);
            synchronized (producer) {
                return ((List<?>) field.get(producer)).size();
            }
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Queues enough churn to cross the producer's delta-count trigger, runs the compaction job on its own, and checks
     * that it collapsed the queue to one delta, which then keeps accepting appends. On return the queue holds the
     * compacted delta and two raw ones behind it, all of one subscription generation, with nothing propagated yet.
     */
    private void queueAndCompact(final RemoteNugget nugget, final QueryTable sourceTable) {
        assertTrue("these tests rely on the default compaction configuration",
                BarrageMessageProducer.COMPACTION_ENABLED && BarrageMessageProducer.COMPACTION_MAX_PENDING_DELTAS > 0);
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;

        final int numCycles = BarrageMessageProducer.COMPACTION_MAX_PENDING_DELTAS + 3;
        for (int ii = 0; ii < numCycles; ++ii) {
            churnRows(sourceTable);
        }
        assertEquals("one delta per cycle until the compaction job runs", numCycles, pendingDeltaCount(producer));

        runDueJobs();
        assertEquals("compaction replaced the run with one delta", 1, pendingDeltaCount(producer));

        churnRows(sourceTable);
        churnRows(sourceTable);
        assertEquals("deltas append behind the compacted head", 3, pendingDeltaCount(producer));
    }

    /** Modifies the first {@code numRows} rows, {@code numCycles} times, without flushing the producer. */
    private void queueDeltas(final QueryTable sourceTable, final int numCycles, final int numRows) {
        try (final RowSet rowsToModify = RowSetFactory.fromRange(0, numRows - 1)) {
            for (int ii = 0; ii < numCycles; ++ii) {
                modifyRows(sourceTable, rowsToModify);
            }
        }
    }

    /**
     * The {@link BarrageMessage#isSnapshot} flag of every message this client has received and not yet delivered to its
     * replicated table, in arrival order.
     */
    private static List<Boolean> queuedSnapshotFlags(final RemoteClient client) {
        final List<Boolean> flags = new ArrayList<>();
        for (final BarrageMessage message : client.commandQueue) {
            flags.add(message.isSnapshot);
        }
        return flags;
    }

    private static void assertNoSnapshots(final String label, final RemoteClient client) {
        final List<Boolean> flags = queuedSnapshotFlags(client);
        assertFalse(label + ": expected at least one message", flags.isEmpty());
        assertFalse(label + ": expected no snapshot, got " + flags, flags.contains(true));
    }

    /**
     * A newcomer's first message must be its snapshot. Anything before it would be delta data describing changes the
     * client has no baseline for. This holds only for subscriptions that are new: {@code propagateToSubscribers} skips
     * a subscription while its {@code pendingInitialSnapshot} flag is set, which is exactly the window between joining
     * and being served.
     */
    private static void assertSnapshotFirst(final String label, final RemoteClient client) {
        final List<Boolean> flags = queuedSnapshotFlags(client);
        assertFalse(label + ": expected at least one message", flags.isEmpty());
        assertTrue(label + ": expected a snapshot first, got " + flags, flags.get(0));
    }

    /**
     * An <em>established</em> subscription that changes its viewport or columns is served differently from a newcomer:
     * it already has a baseline, so the queued deltas still belong to it and are sent first, under the viewport and
     * columns it had before the change ({@code snapshotViewport}/{@code snapshotColumns} in the producer). Only then
     * does it receive the snapshot that re-bases it onto the new viewport. Coalescing must never merge across that
     * boundary, because the two halves are addressed to different viewports of the same client.
     */
    private static void assertDeltaThenSnapshot(final String label, final RemoteClient client) {
        final List<Boolean> flags = queuedSnapshotFlags(client);
        assertTrue(label + ": expected a pre-snapshot delta followed by a snapshot, got " + flags, flags.size() >= 2);
        assertFalse(label + ": expected a pre-snapshot delta first, got " + flags, flags.get(0));
        assertTrue(label + ": expected a snapshot after the pre-snapshot delta, got " + flags, flags.get(1));
    }

    /** Delivers every client's queued messages and runs one consumer update graph cycle. */
    private void flushClients(final RemoteNugget nugget) {
        nugget.flushClientEvents();
        updateGraph().runWithinUnitTestCycle(updateSourceCombiner::run);
    }

    /**
     * A subscriber joining while other subscribers hold pending deltas must receive a snapshot and nothing that
     * predates it, while the existing subscribers receive the queued deltas as an ordinary (non-snapshot) update.
     */
    public void testFullSubscriberJoinsWithPendingDeltas() {
        checkJoinWithPendingDeltas(null, "late-full");
    }

    /** As {@link #testFullSubscriberJoinsWithPendingDeltas}, but the newcomer requests a viewport. */
    public void testViewportSubscriberJoinsWithPendingDeltas() {
        try (final RowSet viewport = RowSetFactory.fromRange(10, 40)) {
            checkJoinWithPendingDeltas(viewport, "late-viewport");
        }
    }

    private void checkJoinWithPendingDeltas(final RowSet lateViewport, final String lateClientName) {
        final QueryTable sourceTable = newSourceTable();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);

        final RemoteClient fullClient = nugget.newClient(null, allColumns(), "existing-full");
        final RemoteClient viewportClient;
        try (final RowSet viewport = RowSetFactory.fromRange(0, TABLE_SIZE / 2)) {
            viewportClient = nugget.newClient(viewport.copy(), allColumns(), "existing-viewport");
        }

        // satisfy the existing subscriptions
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("existing subscriptions satisfied");

        // queue several cycles of deltas without letting the producer propagate them
        queueDeltas(sourceTable, 5, TABLE_SIZE / 2);
        assertEquals("no propagation without a flush", 0, fullClient.pendingMessageCount());
        assertEquals("no propagation without a flush", 0, viewportClient.pendingMessageCount());

        // the newcomer joins; subscribing schedules the propagation job immediately
        final RemoteClient lateClient = nugget.newClient(
                lateViewport == null ? null : lateViewport.copy(), allColumns(), lateClientName);
        flushProducerTable();

        // the existing subscribers get the queued deltas, coalesced, as a non-snapshot update
        assertNoSnapshots("existing-full", fullClient);
        assertNoSnapshots("existing-viewport", viewportClient);

        // the newcomer gets its snapshot first and never sees the pre-snapshot deltas
        assertSnapshotFirst(lateClientName, lateClient);

        flushClients(nugget);
        nugget.validate("after late join");

        // everyone tracks the source from here on
        queueDeltas(sourceTable, 2, TABLE_SIZE);
        flushProducerTable();
        assertNoSnapshots("existing-full post-join", fullClient);
        assertNoSnapshots(lateClientName + " post-join", lateClient);
        flushClients(nugget);
        nugget.validate("after post-join deltas");
    }

    /**
     * Deltas recorded while a newcomer's snapshot is being built land after the snapshot step, so they must reach the
     * newcomer too -- its snapshot predates them. The existing subscribers see both halves of the split.
     */
    public void testSubscriberJoinsWhileSnapshotIsTaken() {
        final QueryTable sourceTable = newSourceTable();

        // Tick the source from inside the snapshot so that deltas straddle the snapshot step.
        final boolean[] tickedDuringSnapshot = new boolean[1];
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable) {
            @Override
            public void onGetSnapshot() {
                if (tickedDuringSnapshot[0]) {
                    return;
                }
                tickedDuringSnapshot[0] = true;
                try (final RowSet rowsToModify = RowSetFactory.fromRange(0, TABLE_SIZE - 1)) {
                    modifyRows(sourceTable, rowsToModify);
                }
            }
        };

        final RemoteClient existingClient = nugget.newClient(null, allColumns(), "existing-full");
        flushProducerTable();
        flushClients(nugget);
        // the snapshot hook fires for the existing client's own snapshot; ignore that one
        tickedDuringSnapshot[0] = false;
        nugget.validate("existing subscription satisfied");

        queueDeltas(sourceTable, 3, TABLE_SIZE / 2);

        final RemoteClient lateClient = nugget.newClient(null, allColumns(), "late-full");
        flushProducerTable();

        assertTrue("expected the source to tick during the snapshot", tickedDuringSnapshot[0]);
        assertNoSnapshots("existing-full", existingClient);
        assertSnapshotFirst("late-full", lateClient);

        // the newcomer must have received the post-snapshot delta in addition to its snapshot
        final List<Boolean> lateFlags = queuedSnapshotFlags(lateClient);
        assertTrue("expected a post-snapshot delta, got " + lateFlags, lateFlags.size() > 1);
        assertFalse("expected the trailing message to be a delta, got " + lateFlags,
                lateFlags.get(lateFlags.size() - 1));

        flushClients(nugget);
        nugget.validate("after joining during a snapshot");
    }

    /**
     * Ten subscribers join at ten different points in the delta stream. Each join drains the pending queue, so no
     * subscriber ever receives data from a generation other than its own.
     */
    public void testManySubscribersJoinAtDifferentOffsets() {
        final QueryTable sourceTable = newSourceTable();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);

        final RemoteClient firstClient = nugget.newClient(null, allColumns(), "existing-full");
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("first subscription satisfied");

        final List<RemoteClient> clients = new ArrayList<>();
        clients.add(firstClient);

        for (int joinIndex = 0; joinIndex < 10; ++joinIndex) {
            // a different number of queued cycles before each join
            queueDeltas(sourceTable, joinIndex + 1, TABLE_SIZE / 2);

            final RemoteClient newcomer;
            if (joinIndex % 2 == 0) {
                newcomer = nugget.newClient(null, allColumns(), "joiner-" + joinIndex);
            } else {
                try (final RowSet viewport = RowSetFactory.fromRange(joinIndex, TABLE_SIZE / 2 + joinIndex)) {
                    newcomer = nugget.newClient(viewport.copy(), allColumns(), "joiner-" + joinIndex);
                }
            }
            flushProducerTable();

            for (final RemoteClient existing : clients) {
                assertNoSnapshots("existing client at join " + joinIndex, existing);
            }
            assertSnapshotFirst("joiner-" + joinIndex, newcomer);

            clients.add(newcomer);
            flushClients(nugget);
            nugget.validate("after join " + joinIndex);
        }
    }

    /**
     * A queue that was compacted before a newcomer joins must serve everyone exactly as an uncompacted one would: the
     * existing subscribers receive the compacted head and what followed it as one non-snapshot update, the newcomer
     * receives its snapshot and nothing that predates it, and the replicated tables match the source afterwards.
     */
    public void testFullSubscriberJoinsAfterCompaction() {
        checkJoinAfterCompaction(null, "late-full");
    }

    /** As {@link #testFullSubscriberJoinsAfterCompaction}, but the newcomer requests a viewport. */
    public void testViewportSubscriberJoinsAfterCompaction() {
        try (final RowSet viewport = RowSetFactory.fromRange(10, 40)) {
            checkJoinAfterCompaction(viewport, "late-viewport");
        }
    }

    private void checkJoinAfterCompaction(final RowSet lateViewport, final String lateClientName) {
        final QueryTable sourceTable = newSourceTable();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);

        final RemoteClient fullClient = nugget.newClient(null, allColumns(), "existing-full");
        final RemoteClient viewportClient;
        try (final RowSet viewport = RowSetFactory.fromRange(0, TABLE_SIZE / 2)) {
            viewportClient = nugget.newClient(viewport.copy(), allColumns(), "existing-viewport");
        }
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("existing subscriptions satisfied");

        queueAndCompact(nugget, sourceTable);
        assertEquals("no propagation without a flush", 0, fullClient.pendingMessageCount());
        assertEquals("no propagation without a flush", 0, viewportClient.pendingMessageCount());

        final RemoteClient lateClient = nugget.newClient(
                lateViewport == null ? null : lateViewport.copy(), allColumns(), lateClientName);
        flushProducerTable();

        assertNoSnapshots("existing-full", fullClient);
        assertNoSnapshots("existing-viewport", viewportClient);
        assertSnapshotFirst(lateClientName, lateClient);

        flushClients(nugget);
        nugget.validate("after late join onto a compacted queue");

        // a second round, now with the newcomer among the existing subscribers
        queueAndCompact(nugget, sourceTable);
        flushProducerTable();
        assertNoSnapshots("existing-full post-join", fullClient);
        assertNoSnapshots(lateClientName + " post-join", lateClient);
        flushClients(nugget);
        nugget.validate("after a second compaction with the newcomer subscribed");
    }

    /**
     * Compaction of deltas that each modify a subset of the columns on their own rows. The compacted delta then carries
     * a distinct recorded-modification row set per column, shared where two columns were modified on identical rows,
     * and is coalesced once more with a partial-column update recorded behind it when the queue propagates.
     */
    public void testPartialColumnModificationsAfterCompaction() {
        final QueryTable sourceTable = newSourceTable();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);

        final RemoteClient fullClient = nugget.newClient(null, allColumns(), "existing-full");
        final RemoteClient viewportClient;
        try (final RowSet viewport = RowSetFactory.fromRange(0, TABLE_SIZE / 2)) {
            viewportClient = nugget.newClient(viewport.copy(), allColumns(), "existing-viewport");
        }
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("existing subscriptions satisfied");
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;

        final int numCycles = BarrageMessageProducer.COMPACTION_MAX_PENDING_DELTAS + 3;
        for (int ii = 0; ii < numCycles; ++ii) {
            modifySomeColumns(sourceTable, ii);
        }
        assertEquals("one delta per cycle until the compaction job runs", numCycles, pendingDeltaCount(producer));
        runDueJobs();
        assertEquals("compaction replaced the run with one delta", 1, pendingDeltaCount(producer));

        // a partial-column update behind the compacted head, coalesced with it when the newcomer forces propagation
        try (final RowSet rows = RowSetFactory.fromRange(30, 69)) {
            modifyColumns(sourceTable, rows, "doubleCol");
        }
        assertEquals("a delta appended behind the compacted head", 2, pendingDeltaCount(producer));

        final RemoteClient lateClient = nugget.newClient(null, allColumns(), "late-full");
        flushProducerTable();
        assertNoSnapshots("existing-full", fullClient);
        assertNoSnapshots("existing-viewport", viewportClient);
        assertSnapshotFirst("late-full", lateClient);
        flushClients(nugget);
        nugget.validate("after per-column compaction and a late join");

        try (final RowSet rows = RowSetFactory.fromRange(0, 99)) {
            modifyColumns(sourceTable, rows, "intCol", "strCol");
        }
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("after a post-join partial-column update");
    }

    /**
     * Compaction whose surviving mapping spans several source and destination chunks, in runs that are contiguous in
     * neither. The table holds three chunks' worth of rows; two large deltas modify overlapping, gapped ranges, then
     * small ones carry the queue over the count trigger, so the compacted delta draws the first chunk and a half and
     * the last half chunk from the first delta and the two chunks between them from the second.
     */
    public void testMultiChunkMappingsAfterCompaction() {
        final int chunk = BarrageMessageProducer.DELTA_CHUNK_SIZE;
        final int tableSize = 3 * chunk;
        final QueryTable sourceTable = newSourceTable(tableSize);
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);

        final RemoteClient fullClient = nugget.newClient(null, allColumns(), "existing-full");
        final RemoteClient viewportClient;
        try (final RowSet viewport = RowSetFactory.fromRange(chunk - 100, chunk + 100)) {
            viewportClient = nugget.newClient(viewport.copy(), allColumns(), "existing-viewport");
        }
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("existing subscriptions satisfied");
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;

        try (final RowSet head = RowSetFactory.fromRange(0, chunk + chunk / 2 - 1);
                final RowSet tail = RowSetFactory.fromRange(2L * chunk, tableSize - 1);
                final RowSet gapped = head.union(tail);
                final RowSet middle = RowSetFactory.fromRange(chunk / 2, 2L * chunk + chunk / 2 - 1)) {
            modifyRows(sourceTable, gapped);
            modifyRows(sourceTable, middle);
        }
        final int numCycles = BarrageMessageProducer.COMPACTION_MAX_PENDING_DELTAS + 3;
        for (int ii = 2; ii < numCycles; ++ii) {
            try (final RowSet rows = RowSetFactory.fromKeys(ii, chunk + ii, 2L * chunk + ii)) {
                modifyRows(sourceTable, rows);
            }
        }
        assertEquals("one delta per cycle until the compaction job runs", numCycles, pendingDeltaCount(producer));
        runDueJobs();
        assertEquals("compaction replaced the run with one delta", 1, pendingDeltaCount(producer));

        final RemoteClient lateClient;
        try (final RowSet viewport = RowSetFactory.fromRange(chunk + chunk / 2 - 50, chunk + chunk / 2 + 50)) {
            lateClient = nugget.newClient(viewport.copy(), allColumns(), "late-viewport");
        }
        flushProducerTable();
        assertNoSnapshots("existing-full", fullClient);
        assertNoSnapshots("existing-viewport", viewportClient);
        assertSnapshotFirst("late-viewport", lateClient);
        flushClients(nugget);
        nugget.validate("after multi-chunk compaction and a late join");

        try (final RowSet rows = RowSetFactory.fromRange(chunk / 4, 2L * chunk + chunk / 4)) {
            modifyRows(sourceTable, rows);
        }
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("after a post-join multi-chunk update");
    }

    /**
     * The byte trigger on its own, well under the count cap. Every cycle modifies the whole of a three-chunk table, so
     * each delta's size is known exactly and the test can follow the producer's policy step by step: nothing compacts
     * until the raw bytes cross the floor, the compacted head is then one table's worth, and the next compaction waits
     * until the raw bytes recorded since reach {@code max(floor, growthFactor * head)} again.
     */
    public void testByteTriggerCompaction() {
        final int chunk = BarrageMessageProducer.DELTA_CHUNK_SIZE;
        final int tableSize = 3 * chunk;
        final QueryTable sourceTable = newSourceTable(tableSize);
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient fullClient = nugget.newClient(null, allColumns(), "existing-full");
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("existing subscription satisfied");
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;

        // A whole-table modification records one full chunk per column per DELTA_CHUNK_SIZE rows, with no rounding.
        long bytesPerDelta = 0;
        for (final String columnName : new String[] {"intCol", "doubleCol", "strCol"}) {
            bytesPerDelta += (long) tableSize * sourceTable.getColumnSource(columnName).getChunkType().elementBytes();
        }
        final long floor = BarrageMessageProducer.COMPACTION_FLOOR_BYTES;
        final double growth = BarrageMessageProducer.COMPACTION_GROWTH_FACTOR;
        final int numCycles = 8;
        assertTrue("the test needs the count cap out of the way",
                numCycles < BarrageMessageProducer.COMPACTION_MAX_PENDING_DELTAS);
        assertTrue("the test needs one delta under the floor and two over it: bytesPerDelta=" + bytesPerDelta
                + ", floor=" + floor, bytesPerDelta < floor && 2 * bytesPerDelta >= floor);

        long rawBytes = 0;
        long headBytes = 0;
        int expectedPending = 0;
        int compactions = 0;
        try (final RowSet allRows = RowSetFactory.flat(tableSize)) {
            for (int cycle = 1; cycle <= numCycles; ++cycle) {
                modifyRows(sourceTable, allRows);
                ++expectedPending;
                rawBytes += bytesPerDelta;
                runDueJobs();
                if (expectedPending >= 2 && rawBytes >= Math.max(floor, growth * headBytes)) {
                    // every row survives exactly once, so the compacted head is one table's worth
                    expectedPending = 1;
                    headBytes = bytesPerDelta;
                    rawBytes = 0;
                    ++compactions;
                }
                assertEquals("pending deltas after cycle " + cycle, expectedPending, pendingDeltaCount(producer));
            }
        }
        assertTrue("expected the byte trigger to fire more than once, fired " + compactions, compactions >= 2);

        flushProducerTable();
        assertNoSnapshots("existing-full", fullClient);
        flushClients(nugget);
        nugget.validate("after byte-triggered compactions");
    }

    /**
     * As {@link #testViewportChangeWithPendingDeltas}, but the queue is compacted before the viewport changes. The
     * changing client's pre-snapshot data, now drawn from a compacted delta, is still sent under its old viewport.
     */
    public void testViewportChangeAfterCompaction() {
        final QueryTable sourceTable = newSourceTable();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);

        final RemoteClient stableClient = nugget.newClient(null, allColumns(), "stable-full");
        final RemoteClient changingClient;
        try (final RowSet viewport = RowSetFactory.fromRange(0, 20)) {
            changingClient = nugget.newClient(viewport.copy(), allColumns(), "changing-viewport");
        }
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("subscriptions satisfied");

        queueAndCompact(nugget, sourceTable);

        try (final RowSet newViewport = RowSetFactory.fromRange(40, 70)) {
            changingClient.setViewport(newViewport.copy());
        }
        flushProducerTable();

        assertNoSnapshots("stable-full", stableClient);
        assertDeltaThenSnapshot("changing-viewport", changingClient);

        flushClients(nugget);
        nugget.validate("after viewport change onto a compacted queue");

        // the churn shifted the table's second half, so modify the rows that exist now rather than [0, TABLE_SIZE)
        try (final RowSet allRows = sourceTable.getRowSet().copy()) {
            modifyRows(sourceTable, allRows);
            modifyRows(sourceTable, allRows);
        }
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("after post-change deltas");
    }

    /**
     * Changing an existing subscription's viewport takes the same path as a join: it schedules the propagation job
     * immediately and splits the pending deltas around a fresh snapshot. The changing client's pre-snapshot data is
     * sent under its <em>old</em> viewport, which is what makes the split necessary in the first place.
     */
    public void testViewportChangeWithPendingDeltas() {
        final QueryTable sourceTable = newSourceTable();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);

        final RemoteClient stableClient = nugget.newClient(null, allColumns(), "stable-full");
        final RemoteClient changingClient;
        try (final RowSet viewport = RowSetFactory.fromRange(0, 20)) {
            changingClient = nugget.newClient(viewport.copy(), allColumns(), "changing-viewport");
        }

        flushProducerTable();
        flushClients(nugget);
        nugget.validate("subscriptions satisfied");

        queueDeltas(sourceTable, 4, TABLE_SIZE / 2);

        try (final RowSet newViewport = RowSetFactory.fromRange(40, 70)) {
            changingClient.setViewport(newViewport.copy());
        }
        flushProducerTable();

        assertNoSnapshots("stable-full", stableClient);
        // the changing client is an existing subscriber: queued deltas first (old viewport), then its new snapshot
        assertDeltaThenSnapshot("changing-viewport", changingClient);

        flushClients(nugget);
        nugget.validate("after viewport change with pending deltas");

        queueDeltas(sourceTable, 2, TABLE_SIZE);
        flushProducerTable();
        flushClients(nugget);
        nugget.validate("after post-change deltas");
    }
}
