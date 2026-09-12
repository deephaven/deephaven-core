//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.BooleanGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.SortedLongGenerator;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

/**
 * Correctness coverage for {@link BarrageMessageProducer#compactPendingDeltas}, which folds a run of queued per-cycle
 * updates into a single equivalent update so that a producer serving slow subscribers does not hold one delta per
 * update graph cycle (DH-21949).
 *
 * <p>
 * The property under test is that compaction is invisible: coalescing a run of deltas early and then coalescing the
 * result with later deltas has to produce what coalescing the whole run at propagation time would have produced. These
 * tests assert that end-to-end -- every subscriber's replicated table is compared against the source table after
 * compaction -- because that is the guarantee subscribers actually depend on, and it exercises the wire encoding as
 * well as the coalescing.
 *
 * <p>
 * The case that makes compaction more than a re-run of the existing aggregation is modified data. A delta recorded from
 * a single cycle records the same rows for every column it modified, but coalescing a run leaves each column with its
 * own surviving set: a row modified in one column and later superseded by an add drops out for that column and not for
 * others. {@link #testPerColumnModifications} covers that directly.
 */
@Category(OutOfBandTest.class)
public class BarrageDeltaCompactionTest extends BarrageMessageRoundTripTestBase {

    private static ControlledUpdateGraph updateGraph() {
        return ExecutionContext.getContext().getUpdateGraph().cast();
    }

    private void flushClients(final List<RemoteNugget> nuggets) {
        for (final RemoteNugget nugget : nuggets) {
            nugget.flushClientEvents();
        }
        updateGraph().runWithinUnitTestCycle(updateSourceCombiner::run);
    }

    private static BitSet allColumns(final Table table) {
        final BitSet columns = new BitSet();
        columns.set(0, table.numColumns());
        return columns;
    }

    /** Subscribes a full client plus forward and reverse viewport clients to {@code nugget}. */
    private void addClients(final RemoteNugget nugget, final int size) {
        final BitSet columns = allColumns(nugget.originalTable);
        nugget.newClient(null, columns, "full");
        nugget.newClient(RowSetFactory.fromRange(0, size / 10), columns, "header");
        nugget.newClient(RowSetFactory.fromRange(size / 2, size * 3L / 4), columns, "floating");
        nugget.newClient(RowSetFactory.fromRange(0, size / 10), columns, true, "footer");
    }

    /**
     * Random, shift-heavy updates over several table shapes, compacted at several different run lengths. The sorted and
     * flattened shapes are what produce shifts, which is where coalescing a run is hardest to get right.
     */
    public void testCompactionPreservesRandomUpdates() {
        for (final int compactEvery : new int[] {2, 3, 5}) {
            final int size = 100;
            final Random random = new Random(0);
            final ColumnInfo<?, ?>[] columnInfo;
            final QueryTable sourceTable = getTable(size / 4, random,
                    columnInfo = initColumnInfos(
                            new String[] {"Sym", "intCol", "doubleCol", "Indices", "boolCol"},
                            new SetGenerator<>("a", "b", "c", "d"),
                            new IntGenerator(10, 100),
                            new SetGenerator<>(10.1, 20.1, 30.1),
                            new SortedLongGenerator(0, Long.MAX_VALUE - 1),
                            new BooleanGenerator(0.2)));

            final List<RemoteNugget> nuggets = new ArrayList<>();
            for (final Supplier<Table> makeTable : List.<Supplier<Table>>of(
                    () -> sourceTable,
                    sourceTable::flatten,
                    () -> sourceTable.sort("doubleCol"))) {
                final RemoteNugget nugget = new RemoteNugget(makeTable);
                addClients(nugget, size);
                nuggets.add(nugget);
            }

            // satisfy the initial subscriptions before anything is queued
            flushProducerTable();
            flushClients(nuggets);
            for (final RemoteNugget nugget : nuggets) {
                nugget.validate("initial, compactEvery=" + compactEvery);
            }

            for (int step = 0; step < 4; ++step) {
                // queue a run of updates and fold it down
                for (int ii = 0; ii < compactEvery; ++ii) {
                    updateGraph().runWithinUnitTestCycle(
                            () -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
                }
                for (final RemoteNugget nugget : nuggets) {
                    final BarrageMessageProducer producer = nugget.barrageMessageProducer;
                    assertEquals(compactEvery, producer.getPendingDeltaCount());
                    assertTrue(producer.compactPendingDeltas(compactEvery));
                    assertEquals(1, producer.getPendingDeltaCount());
                }

                // queue more updates on top of the compacted delta, then let the producer propagate everything
                for (int ii = 0; ii < compactEvery; ++ii) {
                    updateGraph().runWithinUnitTestCycle(
                            () -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
                }
                for (final RemoteNugget nugget : nuggets) {
                    assertEquals(compactEvery + 1, nugget.barrageMessageProducer.getPendingDeltaCount());
                }

                flushProducerTable();
                flushClients(nuggets);
                for (final RemoteNugget nugget : nuggets) {
                    nugget.validate("compactEvery=" + compactEvery + " step=" + step);
                }
            }
        }
    }

    /**
     * Compacting twice in a row, so that a compacted delta is itself folded into another compaction. The second
     * compaction reads the per-column modified sets the first one produced, rather than the uniform sets a recorded
     * delta carries.
     */
    public void testRepeatedCompaction() {
        final int size = 100;
        final Random random = new Random(12345);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable sourceTable = getTable(size / 4, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "doubleCol", "Indices"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new SortedLongGenerator(0, Long.MAX_VALUE - 1)));

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        addClients(nugget, size);
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("initial");

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        for (int round = 0; round < 3; ++round) {
            for (int ii = 0; ii < 3; ++ii) {
                updateGraph().runWithinUnitTestCycle(
                        () -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                                GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            }
            // fold the new updates in with whatever compaction left behind on the previous round
            final int pending = producer.getPendingDeltaCount();
            assertTrue(producer.compactPendingDeltas(pending));
            assertEquals(1, producer.getPendingDeltaCount());
        }

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("after repeated compaction");
    }

    /**
     * Different columns modified on different rows across the compacted run. After compaction each column must retain
     * its own set of modified rows; a single shared set would either send data for rows a column never changed or drop
     * rows it did.
     */
    public void testPerColumnModifications() {
        final int size = 50;
        final PerColumnFixture fixture = new PerColumnFixture(size);

        final RemoteNugget nugget = new RemoteNugget(() -> fixture.sourceTable);
        addClients(nugget, size);
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("initial");

        // Four cycles touching different columns over partly overlapping rows, so that no single set of modified rows
        // describes all three columns.
        fixture.modifyInt(0, 9, 1000);
        fixture.modifyDouble(20, 29, 7.5);
        fixture.modifyDouble(5, 14, 9.5);
        fixture.modifyString(30, 34, "late");

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        assertEquals(4, producer.getPendingDeltaCount());
        assertTrue(producer.compactPendingDeltas(4));
        assertEquals(1, producer.getPendingDeltaCount());

        // more per-column modifications on top of the compacted delta
        fixture.modifyInt(40, 44, 2000);
        fixture.modifyDouble(0, 4, 11.5);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("per-column modifications");

        // and a second compaction that has to read the per-column sets the first one produced
        fixture.modifyInt(10, 19, 3000);
        fixture.modifyString(0, 9, "second");
        fixture.modifyDouble(15, 24, 13.5);
        assertTrue(producer.compactPendingDeltas(3));
        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("per-column modifications after a second compaction");
    }

    /**
     * A flat three-column table whose cells are tracked locally, so an update can rewrite one column while leaving the
     * others at their current values. {@code TstUtils.addToTable} replaces whole rows, so every column has to be
     * supplied on every update even though only one of them changes and only that one is named in the
     * {@link ModifiedColumnSet}.
     */
    private final class PerColumnFixture {
        private final QueryTable sourceTable;
        private final int[] intValues;
        private final double[] doubleValues;
        private final String[] stringValues;
        private final ModifiedColumnSet intOnly;
        private final ModifiedColumnSet doubleOnly;
        private final ModifiedColumnSet stringOnly;

        private PerColumnFixture(final int size) {
            intValues = new int[size];
            doubleValues = new double[size];
            stringValues = new String[size];
            for (int ii = 0; ii < size; ++ii) {
                intValues[ii] = ii;
                doubleValues[ii] = ii * 0.25;
                stringValues[ii] = "s" + ii;
            }
            sourceTable = TstUtils.testRefreshingTable(
                    RowSetFactory.flat(size).toTracking(),
                    TableTools.intCol("intCol", intValues.clone()),
                    TableTools.doubleCol("doubleCol", doubleValues.clone()),
                    TableTools.stringCol("strCol", stringValues.clone()));
            intOnly = sourceTable.newModifiedColumnSet("intCol");
            doubleOnly = sourceTable.newModifiedColumnSet("doubleCol");
            stringOnly = sourceTable.newModifiedColumnSet("strCol");
        }

        private void modifyInt(final int firstRow, final int lastRow, final int base) {
            for (int row = firstRow; row <= lastRow; ++row) {
                intValues[row] = base + row;
            }
            publish(firstRow, lastRow, intOnly);
        }

        private void modifyDouble(final int firstRow, final int lastRow, final double base) {
            for (int row = firstRow; row <= lastRow; ++row) {
                doubleValues[row] = base + row;
            }
            publish(firstRow, lastRow, doubleOnly);
        }

        private void modifyString(final int firstRow, final int lastRow, final String prefix) {
            for (int row = firstRow; row <= lastRow; ++row) {
                stringValues[row] = prefix + row;
            }
            publish(firstRow, lastRow, stringOnly);
        }

        private void publish(final int firstRow, final int lastRow, final ModifiedColumnSet columnSet) {
            final int numRows = lastRow - firstRow + 1;
            final int[] ints = new int[numRows];
            final double[] doubles = new double[numRows];
            final String[] strings = new String[numRows];
            for (int ii = 0; ii < numRows; ++ii) {
                ints[ii] = intValues[firstRow + ii];
                doubles[ii] = doubleValues[firstRow + ii];
                strings[ii] = stringValues[firstRow + ii];
            }
            updateGraph().runWithinUnitTestCycle(() -> {
                try (final RowSet rows = RowSetFactory.fromRange(firstRow, lastRow)) {
                    TstUtils.addToTable(sourceTable, rows,
                            TableTools.intCol("intCol", ints),
                            TableTools.doubleCol("doubleCol", doubles),
                            TableTools.stringCol("strCol", strings));
                    sourceTable.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(),
                            rows.copy(), RowSetShiftData.EMPTY, columnSet));
                }
            });
        }
    }

    /**
     * A run of updates that repeatedly rewrites the same rows collapses to one cycle's worth of data, which is the
     * memory win the ticket is about. The chunk storage the producer holds must fall accordingly.
     */
    public void testCompactionReducesRetainedBytes() {
        final int size = 4096;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final BitSet columns = allColumns(sourceTable);
        nugget.newClient(null, columns, "full");
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("initial");

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");

        final int numCycles = 10;
        for (int ii = 0; ii < numCycles; ++ii) {
            modifyIntColumn(sourceTable, 0, size - 1, intOnly, 1000 + ii);
        }

        final long bytesBefore = producer.getPendingDeltaBytes();
        assertEquals(numCycles, producer.getPendingDeltaCount());
        assertTrue(producer.compactPendingDeltas(numCycles));
        final long bytesAfter = producer.getPendingDeltaBytes();

        assertEquals(1, producer.getPendingDeltaCount());
        // every cycle rewrote the same rows, so the survivor is one cycle's worth
        assertTrue("expected compaction to shrink retained bytes: before=" + bytesBefore + " after=" + bytesAfter,
                bytesAfter * 2 < bytesBefore);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("after compaction");
    }

    /** Compaction has nothing to do for a single delta, and declines runs longer than the queue. */
    public void testCompactionDeclinesTrivialRuns() {
        final int size = 20;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(null, allColumns(sourceTable), "full");
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");

        assertFalse("nothing queued", producer.compactPendingDeltas(2));

        modifyIntColumn(sourceTable, 0, 9, intOnly, 10);
        assertEquals(1, producer.getPendingDeltaCount());
        assertFalse("a single delta is already compact", producer.compactPendingDeltas(1));
        assertFalse("cannot compact more than is queued", producer.compactPendingDeltas(2));
        assertEquals(1, producer.getPendingDeltaCount());

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("after declined compaction");
    }

    /**
     * A subscriber joining after a compaction must still be served correctly: the compacted delta belongs to the
     * existing subscribers only, and the newcomer's snapshot supersedes it.
     */
    public void testLateJoinerAfterCompaction() {
        final int size = 60;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final BitSet columns = allColumns(sourceTable);
        final RemoteClient existingClient = nugget.newClient(null, columns, "existing-full");
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("initial");

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");
        for (int ii = 0; ii < 4; ++ii) {
            modifyIntColumn(sourceTable, 0, 19, intOnly, 500 + ii);
        }
        assertTrue(producer.compactPendingDeltas(4));
        assertEquals(1, producer.getPendingDeltaCount());

        // the newcomer's snapshot splits the queue; the compacted delta is pre-snapshot and is not sent to it
        final RemoteClient lateClient = nugget.newClient(null, columns, "late-full");
        flushProducerTable();

        assertFalse("the existing client must receive the compacted delta, not a snapshot",
                existingClient.commandQueue.stream().anyMatch(message -> message.isSnapshot));
        assertTrue("the newcomer's first message must be its snapshot",
                lateClient.commandQueue.peek() != null && lateClient.commandQueue.peek().isSnapshot);

        flushClients(nuggets);
        nugget.validate("late joiner after compaction");

        for (int ii = 0; ii < 3; ++ii) {
            modifyIntColumn(sourceTable, 10, 29, intOnly, 900 + ii);
        }
        assertTrue(producer.compactPendingDeltas(3));
        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("after compaction with both subscribers");
    }

    /**
     * A compacted delta whose data spans more than one chunk. The copy kernel locates a row by dividing its position by
     * {@link BarrageMessageProducer#DELTA_CHUNK_SIZE}, so a compacted delta has to be chunked exactly that way -- which
     * is not the chunking the aggregation uses for messages headed to the wire. A single-chunk delta cannot tell the
     * two apart.
     */
    public void testCompactionAcrossChunkBoundaries() {
        final int numRows = 2 * BarrageMessageProducer.DELTA_CHUNK_SIZE + 100;
        final int[] values = new int[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(numRows).toTracking(), TableTools.intCol("intCol", values));

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        nugget.newClient(null, allColumns(sourceTable), "full");
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("initial");

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");
        for (int ii = 0; ii < 3; ++ii) {
            modifyIntColumn(sourceTable, 0, numRows - 1, intOnly, 1_000_000 * (ii + 1));
        }
        assertTrue(producer.compactPendingDeltas(3));
        assertEquals(1, producer.getPendingDeltaCount());

        // a further cycle so the compacted multi-chunk delta is itself read back by the copy kernel
        modifyIntColumn(sourceTable, 0, numRows - 1, intOnly, 9_000_000);
        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("compaction across chunk boundaries");
    }

    /**
     * Compacting only a prefix, leaving later deltas queued behind the compacted one. The propagation that follows then
     * coalesces a compacted delta together with recorded ones, which is the mixture a real compaction policy produces
     * and which the all-or-nothing cases above never exercise.
     */
    public void testCompactionOfStrictPrefix() {
        final int size = 100;
        final Random random = new Random(999);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable sourceTable = getTable(size / 4, random,
                columnInfo = initColumnInfos(
                        new String[] {"Sym", "intCol", "doubleCol", "Indices"},
                        new SetGenerator<>("a", "b", "c", "d"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new SortedLongGenerator(0, Long.MAX_VALUE - 1)));

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        addClients(nugget, size);
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("initial");

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        for (int step = 0; step < 3; ++step) {
            long prefixBytes = 0;
            for (int ii = 0; ii < 6; ++ii) {
                updateGraph().runWithinUnitTestCycle(
                        () -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                                GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
                if (ii == 3) {
                    prefixBytes = producer.getPendingDeltaBytes();
                }
            }
            assertEquals(6, producer.getPendingDeltaCount());
            final long bytesBefore = producer.getPendingDeltaBytes();
            final long copiedBefore = producer.getCompactionCopiedBytes();

            // fold only the oldest four, leaving two recorded deltas behind the compacted one
            assertTrue(producer.compactPendingDeltas(4));
            assertEquals(3, producer.getPendingDeltaCount());

            // The tail's bytes are untouched, and the compacted delta was copied in full, so what was counted as
            // copied is exactly what the queue gained back minus what it gave up. (Miscounting the tail as moved
            // would fail this.)
            final long copied = producer.getCompactionCopiedBytes() - copiedBefore;
            final long compactedBytes = producer.getPendingDeltaBytes() - (bytesBefore - prefixBytes);
            assertTrue("random updates include modifications, so this run is copied", copied > 0);
            assertEquals(compactedBytes, copied);

            flushProducerTable();
            flushClients(nuggets);
            nugget.validate("strict prefix compaction, step " + step);
        }
    }

    /**
     * A run in which nothing is superseded -- pure adds, whether in key order or not -- has nothing for coalescing to
     * drop, so compaction declines it rather than copy the whole run for no gain: the queue, the bytes held and the
     * copied-bytes counter are all unchanged, and the flush serves the forward, reverse and full subscribers as usual.
     * The same run plus one modification is no longer pure adds, and is compacted by copying.
     */
    public void testPureAddRunsDecline() {
        final int size = 100;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final BitSet columns = allColumns(sourceTable);
        nugget.newClient(null, columns, "full");
        nugget.newClient(RowSetFactory.fromRange(0, 20), columns, "head");
        nugget.newClient(RowSetFactory.fromRange(0, 9), columns, true, "tail");
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("initial");

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;

        // in key order
        int nextKey = size;
        for (int ii = 0; ii < 4; ++ii) {
            appendRows(sourceTable, nextKey, 25, 1000 * (ii + 1));
            nextKey += 25;
        }
        long bytesBefore = producer.getPendingDeltaBytes();
        final long copiedBefore = producer.getCompactionCopiedBytes();
        assertEquals(4, producer.getPendingDeltaCount());
        assertFalse("append-only run has nothing to gain from compaction", producer.compactPendingDeltas(4));
        assertEquals(4, producer.getPendingDeltaCount());
        assertEquals(bytesBefore, producer.getPendingDeltaBytes());
        assertEquals(copiedBefore, producer.getCompactionCopiedBytes());

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("declined append-only compaction, flushed normally");

        // out of key order: three blocks added in the order 2, 0, 1
        appendRows(sourceTable, nextKey + 50, 25, 3000);
        appendRows(sourceTable, nextKey, 25, 1000);
        appendRows(sourceTable, nextKey + 25, 25, 2000);
        nextKey += 75;
        bytesBefore = producer.getPendingDeltaBytes();
        assertEquals(3, producer.getPendingDeltaCount());
        assertFalse("out-of-order adds have nothing to gain from compaction", producer.compactPendingDeltas(3));
        assertEquals(3, producer.getPendingDeltaCount());
        assertEquals(bytesBefore, producer.getPendingDeltaBytes());
        assertEquals(copiedBefore, producer.getCompactionCopiedBytes());

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("declined out-of-order compaction, flushed normally");

        // the same shape plus a modification is no longer pure adds, so it is compacted, by copying
        appendRows(sourceTable, nextKey + 50, 25, 6000);
        appendRows(sourceTable, nextKey, 25, 4000);
        modifyIntColumn(sourceTable, 0, 9, sourceTable.newModifiedColumnSet("intCol"), 77);
        assertEquals(3, producer.getPendingDeltaCount());
        assertTrue(producer.compactPendingDeltas(3));
        assertEquals(1, producer.getPendingDeltaCount());
        assertTrue(producer.getCompactionCopiedBytes() > copiedBefore);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("mixed run compacted");
    }

    /**
     * Both copy paths of a compaction, on one table. Long stretches of modified rows average far more than the
     * range-copy threshold and are copied as runs, split where they cross the chunk boundary; every-third-row
     * modifications average one row per run and fall back to the per-cell kernel. A run of appends then modifications
     * mixes adds sourced from an add chunk with adds whose latest value sits in a later mod chunk. Subscribers with a
     * full, a head and a tail view must all see the right table after the compacted delta is flushed.
     */
    public void testRunCopyAndCellCopyPaths() {
        final int size = 2 * BarrageMessageProducer.DELTA_CHUNK_SIZE + 500;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final BitSet columns = allColumns(sourceTable);
        nugget.newClient(null, columns, "full");
        nugget.newClient(RowSetFactory.fromRange(0, 1000), columns, "head");
        nugget.newClient(RowSetFactory.fromRange(0, 1000), columns, true, "tail");
        final List<RemoteNugget> nuggets = List.of(nugget);

        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("initial");

        final BarrageMessageProducer producer = nugget.barrageMessageProducer;
        final ModifiedColumnSet intOnly = sourceTable.newModifiedColumnSet("intCol");

        // long runs crossing the first chunk boundary, then the same again with a different value: run path
        modifyIntColumn(sourceTable, BarrageMessageProducer.DELTA_CHUNK_SIZE - 1000,
                BarrageMessageProducer.DELTA_CHUNK_SIZE + 1000, intOnly, 100_000);
        modifyIntColumn(sourceTable, 0, size - 1, intOnly, 200_000);
        modifyIntColumn(sourceTable, BarrageMessageProducer.DELTA_CHUNK_SIZE - 10,
                BarrageMessageProducer.DELTA_CHUNK_SIZE + 10, intOnly, 300_000);
        assertTrue(producer.compactPendingDeltas(3));
        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("run copies across a chunk boundary");

        // every third row, twice with different phases: single-row runs, cell path
        modifyEveryNth(sourceTable, 0, size - 1, 3, intOnly, 7);
        modifyEveryNth(sourceTable, 1, size - 1, 3, intOnly, 11);
        assertTrue(producer.compactPendingDeltas(2));
        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("cell copies for scattered rows");

        // appends, then modifications of some appended rows and some old ones: adds sourced from add and mod chunks
        appendRows(sourceTable, size, 3000, 500_000);
        modifyIntColumn(sourceTable, size + 1000, size + 1999, intOnly, 600_000);
        modifyEveryNth(sourceTable, 10, size + 2999, 5, intOnly, 13);
        assertTrue(producer.compactPendingDeltas(3));
        flushProducerTable();
        flushClients(nuggets);
        nugget.validate("adds from add and mod chunks");
    }

    // ---- update helpers ----

    /** Appends {@code count} rows at keys {@code [firstKey, firstKey + count)} in one update graph cycle. */
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

    /** Rewrites {@code intCol} on every {@code stride}-th row of {@code [firstRow, lastRow]} in one cycle. */
    private void modifyEveryNth(final QueryTable sourceTable, final int firstRow, final int lastRow, final int stride,
            final ModifiedColumnSet columnSet, final int base) {
        final io.deephaven.engine.rowset.RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (long row = firstRow; row <= lastRow; row += stride) {
            builder.appendKey(row);
        }
        final RowSet rows = builder.build();
        final int[] newValues = new int[rows.intSize()];
        int ii = 0;
        for (final RowSet.Iterator it = rows.iterator(); it.hasNext();) {
            newValues[ii++] = base * 1000 + (int) it.nextLong();
        }
        updateGraph().runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(sourceTable, rows, TableTools.intCol("intCol", newValues));
            sourceTable.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(),
                    rows.copy(), RowSetShiftData.EMPTY, columnSet));
        });
        rows.close();
    }

    /** Rewrites {@code intCol} over {@code [firstRow, lastRow]} in one update graph cycle. */
    private void modifyIntColumn(final QueryTable sourceTable, final int firstRow, final int lastRow,
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
