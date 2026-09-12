//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

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

    private static BitSet allColumns() {
        final BitSet columns = new BitSet();
        columns.set(0, NUM_COLUMNS);
        return columns;
    }

    private static ControlledUpdateGraph updateGraph() {
        return ExecutionContext.getContext().getUpdateGraph().cast();
    }

    private QueryTable newSourceTable() {
        tickCounter = 0;
        final int[] intValues = new int[TABLE_SIZE];
        final double[] doubleValues = new double[TABLE_SIZE];
        final String[] stringValues = new String[TABLE_SIZE];
        for (int ii = 0; ii < TABLE_SIZE; ++ii) {
            intValues[ii] = ii;
            doubleValues[ii] = ii * 0.5;
            stringValues[ii] = "v" + ii;
        }
        return TstUtils.testRefreshingTable(
                RowSetFactory.flat(TABLE_SIZE).toTracking(),
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
            final int numRows = rowsToModify.intSize();
            final int[] intValues = new int[numRows];
            final double[] doubleValues = new double[numRows];
            final String[] stringValues = new String[numRows];
            int ii = 0;
            for (final RowSet.Iterator it = rowsToModify.iterator(); it.hasNext();) {
                final long rowKey = it.nextLong();
                intValues[ii] = (int) (rowKey * 1000 + tick);
                doubleValues[ii] = rowKey + tick / 1000.0;
                stringValues[ii] = "v" + rowKey + "-" + tick;
                ++ii;
            }
            TstUtils.addToTable(sourceTable, rowsToModify,
                    TableTools.intCol("intCol", intValues),
                    TableTools.doubleCol("doubleCol", doubleValues),
                    TableTools.stringCol("strCol", stringValues));
            sourceTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(), RowSetFactory.empty(), rowsToModify.copy(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
        });
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
