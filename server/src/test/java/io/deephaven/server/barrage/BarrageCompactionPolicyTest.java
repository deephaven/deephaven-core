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
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

import java.util.BitSet;
import java.util.List;

/**
 * The compaction policy and the scheduler job that runs it (DH-21949 Phase 4). {@link BarrageDeltaCompactionTest}
 * covers what a compaction does to the data; these tests cover when the producer decides to compact, that the job
 * running on the scheduler thread swaps its result into a queue the update graph thread keeps appending to, and that
 * the memory a producer holds is bounded by a small multiple of its compacted footprint rather than by the number of
 * cycles per update interval.
 *
 * <p>
 * The propagation job is scheduled an update interval after the last flush, so running the scheduler "through now"
 * after each cycle runs exactly the compaction job, if the policy fired, and leaves the flush for later -- which is the
 * interleaving the scheduler thread produces in production.
 */
@Category(OutOfBandTest.class)
public class BarrageCompactionPolicyTest extends BarrageMessageRoundTripTestBase {

    private static final int SIZE = 1000;
    private static final int ROWS_PER_CYCLE = 100;

    private static ControlledUpdateGraph updateGraph() {
        return ExecutionContext.getContext().getUpdateGraph().cast();
    }

    private void flushClients(final List<RemoteNugget> nuggets) {
        for (final RemoteNugget nugget : nuggets) {
            nugget.flushClientEvents();
        }
        updateGraph().runWithinUnitTestCycle(updateSourceCombiner::run);
    }

    private RemoteNugget makeNugget(final QueryTable sourceTable) {
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final BitSet columns = new BitSet();
        columns.set(0, sourceTable.numColumns());
        nugget.newClient(null, columns, "full");
        nugget.newClient(RowSetFactory.fromRange(0, SIZE / 10), columns, "head");
        nugget.newClient(RowSetFactory.fromRange(0, SIZE / 10), columns, true, "tail");
        flushProducerTable();
        flushClients(List.of(nugget));
        nugget.validate("initial");
        return nugget;
    }

    private static QueryTable makeSourceTable() {
        final int[] values = new int[SIZE];
        for (int ii = 0; ii < SIZE; ++ii) {
            values[ii] = ii;
        }
        return TstUtils.testRefreshingTable(RowSetFactory.flat(SIZE).toTracking(),
                TableTools.intCol("intCol", values));
    }

    /** Runs whatever the scheduler holds for right now: the compaction job if the policy fired, never the flush. */
    private void runScheduledCompaction() {
        scheduler.runThrough(scheduler.currentTimeMillis());
    }

    /**
     * The bounded-memory property. The same rows are modified for many cycles with the floor at zero, so the policy
     * compacts whenever the raw data since the last compaction reaches the compacted size. Since the compacted
     * footprint is one cycle's worth, the producer never holds more than about three cycles' worth -- the compacted
     * delta, the raw deltas that triggered the next compaction, and the transient -- however many cycles elapse, and
     * the queue never grows past a handful of deltas. Then a flush, and a second round after it to show the policy
     * state was reset.
     */
    public void testSameRowsTickingHoldBoundedMemory() {
        final QueryTable sourceTable = makeSourceTable();
        final RemoteNugget nugget = makeNugget(sourceTable);
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        producer.setCompactionPolicy(true, 0, 1.0, 0, false);
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");

        for (int round = 0; round < 2; ++round) {
            modifyRows(sourceTable, 0, ROWS_PER_CYCLE - 1, intOnly, 1_000 * round);
            final long oneCycleBytes = producer.getPendingDeltaBytes();
            assertTrue(oneCycleBytes > 0);

            long maxPendingBytes = oneCycleBytes;
            int maxPendingDeltas = 1;
            for (int cycle = 1; cycle < 40; ++cycle) {
                modifyRows(sourceTable, 0, ROWS_PER_CYCLE - 1, intOnly, 1_000 * round + cycle);
                runScheduledCompaction();
                maxPendingBytes = Math.max(maxPendingBytes, producer.getPendingDeltaBytes());
                maxPendingDeltas = Math.max(maxPendingDeltas, producer.getPendingDeltaCount());
            }
            assertTrue("held " + maxPendingBytes + " bytes for a footprint of " + oneCycleBytes,
                    maxPendingBytes <= 3 * oneCycleBytes);
            assertTrue("queued " + maxPendingDeltas + " deltas", maxPendingDeltas <= 3);

            flushProducerTable();
            flushClients(List.of(nugget));
            nugget.validate("bounded memory, round " + round);
            assertEquals(0, producer.getPendingDeltaCount());
        }
    }

    /**
     * Growing footprint: each cycle modifies a different block, so the compacted delta grows toward the whole table.
     * With the floor at zero the policy compacts each time the raw data doubles the compacted size, so the number of
     * compactions is logarithmic in the number of cycles and the queue is bounded by twice the footprint plus the
     * transient. The total the producer copied stays within a small constant of the data recorded.
     */
    public void testRotatingRowsCompactGeometrically() {
        final QueryTable sourceTable = makeSourceTable();
        final RemoteNugget nugget = makeNugget(sourceTable);
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        producer.setCompactionPolicy(true, 0, 1.0, 0, false);
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");

        final int cycles = 64;
        long recordedBytes = 0;
        long maxPendingBytes = 0;
        final long copiedBefore = producer.getCompactionCopiedBytes();
        for (int cycle = 0; cycle < cycles; ++cycle) {
            final int first = (cycle * ROWS_PER_CYCLE) % SIZE;
            final long before = producer.getPendingDeltaBytes();
            modifyRows(sourceTable, first, first + ROWS_PER_CYCLE - 1, intOnly, 7_000 + cycle);
            recordedBytes += producer.getPendingDeltaBytes() - before;
            runScheduledCompaction();
            maxPendingBytes = Math.max(maxPendingBytes, producer.getPendingDeltaBytes());
        }
        final long copied = producer.getCompactionCopiedBytes() - copiedBefore;
        final long footprint = producer.getPendingDeltaBytes();
        assertTrue("copied " + copied + " for " + recordedBytes + " recorded", copied <= 3 * recordedBytes);
        assertTrue("held " + maxPendingBytes + " for a footprint of " + footprint, maxPendingBytes <= 3 * footprint);
        assertTrue("expected a handful of compactions, not one per cycle", producer.getPendingDeltaCount() < cycles);

        flushProducerTable();
        flushClients(List.of(nugget));
        nugget.validate("geometric compaction");
    }

    /** With the byte trigger out of reach, the count cap alone bounds the queue to the cap plus the compacted delta. */
    public void testDeltaCountCap() {
        final QueryTable sourceTable = makeSourceTable();
        final RemoteNugget nugget = makeNugget(sourceTable);
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        producer.setCompactionPolicy(true, Long.MAX_VALUE, 1.0, 3, false);
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");

        final long copiedBefore = producer.getCompactionCopiedBytes();
        int maxPendingDeltas = 0;
        for (int cycle = 0; cycle < 20; ++cycle) {
            final int first = (cycle * ROWS_PER_CYCLE) % SIZE;
            modifyRows(sourceTable, first, first + ROWS_PER_CYCLE - 1, intOnly, 9_000 + cycle);
            runScheduledCompaction();
            maxPendingDeltas = Math.max(maxPendingDeltas, producer.getPendingDeltaCount());
        }
        assertTrue("queued " + maxPendingDeltas, maxPendingDeltas <= 4);
        assertTrue(producer.getCompactionCopiedBytes() > copiedBefore);

        flushProducerTable();
        flushClients(List.of(nugget));
        nugget.validate("count cap");
    }

    /** Under the floor with no count cap, the producer never compacts: the queue is one delta per cycle, as before. */
    public void testUnderTheFloorNothingCompacts() {
        final QueryTable sourceTable = makeSourceTable();
        final RemoteNugget nugget = makeNugget(sourceTable);
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        producer.setCompactionPolicy(true, Long.MAX_VALUE, 1.0, 0, false);
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");

        final long copiedBefore = producer.getCompactionCopiedBytes();
        for (int cycle = 0; cycle < 10; ++cycle) {
            modifyRows(sourceTable, 0, ROWS_PER_CYCLE - 1, intOnly, 11_000 + cycle);
            runScheduledCompaction();
        }
        assertEquals(10, producer.getPendingDeltaCount());
        assertEquals(copiedBefore, producer.getCompactionCopiedBytes());

        flushProducerTable();
        flushClients(List.of(nugget));
        nugget.validate("under the floor");
    }

    /**
     * An append-only table gives the policy nothing to compact. Every attempt declines and backs off, the queue holds
     * one delta per cycle, nothing is copied, and the flush is unaffected.
     */
    public void testAppendOnlyTableDeclinesAndBacksOff() {
        final QueryTable sourceTable = makeSourceTable();
        final RemoteNugget nugget = makeNugget(sourceTable);
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        producer.setCompactionPolicy(true, 0, 1.0, 0, false);

        final long copiedBefore = producer.getCompactionCopiedBytes();
        int nextKey = SIZE;
        for (int cycle = 0; cycle < 20; ++cycle) {
            appendRows(sourceTable, nextKey, ROWS_PER_CYCLE, 13_000 + cycle);
            nextKey += ROWS_PER_CYCLE;
            runScheduledCompaction();
        }
        assertEquals(20, producer.getPendingDeltaCount());
        assertEquals(copiedBefore, producer.getCompactionCopiedBytes());

        flushProducerTable();
        flushClients(List.of(nugget));
        nugget.validate("append-only");
    }

    /**
     * The job and the flush queued together. The compaction job is scheduled by the last cycle and the propagation job
     * is already due; draining the scheduler runs both in order and the subscribers see the right table.
     */
    public void testCompactionJobAndFlushQueuedTogether() {
        final QueryTable sourceTable = makeSourceTable();
        final RemoteNugget nugget = makeNugget(sourceTable);
        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        producer.setCompactionPolicy(true, 0, 1.0, 0, false);
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");

        for (int cycle = 0; cycle < 5; ++cycle) {
            modifyRows(sourceTable, 0, ROWS_PER_CYCLE - 1, intOnly, 17_000 + cycle);
        }
        assertEquals(5, producer.getPendingDeltaCount());
        flushProducerTable();
        flushClients(List.of(nugget));
        nugget.validate("compaction then flush");
        assertEquals(0, producer.getPendingDeltaCount());
    }

    // ---- update helpers ----

    private void appendRows(final QueryTable sourceTable, final int firstKey, final int count, final int base) {
        final int[] newValues = new int[count];
        for (int ii = 0; ii < count; ++ii) {
            newValues[ii] = base + ii;
        }
        updateGraph().runWithinUnitTestCycle(() -> {
            try (final RowSet added = RowSetFactory.fromRange(firstKey, firstKey + count - 1)) {
                TstUtils.addToTable(sourceTable, added, TableTools.intCol("intCol", newValues));
                sourceTable.notifyListeners(new TableUpdateImpl(added.copy(), RowSetFactory.empty(),
                        RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            }
        });
    }

    private void modifyRows(final QueryTable sourceTable, final int firstRow, final int lastRow,
            final ModifiedColumnSet columnSet, final int base) {
        final int numRows = lastRow - firstRow + 1;
        final int[] newValues = new int[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            newValues[ii] = base + firstRow + ii;
        }
        updateGraph().runWithinUnitTestCycle(() -> {
            try (final RowSet rows = RowSetFactory.fromRange(firstRow, lastRow)) {
                TstUtils.addToTable(sourceTable, rows, TableTools.intCol("intCol", newValues));
                sourceTable.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(),
                        rows.copy(), RowSetShiftData.EMPTY, columnSet));
            }
        });
    }
}
