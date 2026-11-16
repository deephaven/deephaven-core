//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.vectors.IntVectorColumnWrapper;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.IntVector;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.function.Consumer;

import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.SNAPSHOT_CHUNK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;

/**
 * Barrage round-trip coverage for incremental append/prepend/random updates, snapshot previous-value semantics, and
 * update coalescing, which {@link BarrageMessageIncrementalRoundTripTest} runs with serial writes to subscribers and
 * {@link BarrageMessageIncrementalRoundTripParallelTest} with parallel ones.
 */
@Category(OutOfBandTest.class)
public abstract class BarrageMessageIncrementalRoundTripTestBase extends BarrageMessageRoundTripTestBase {

    @Test
    public void testAppendIncrementalSharedProducer() {
        final int MAX_STEPS = 100;
        final Consumer<TestHelper> runOne = helper -> {
            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16)
                        + (helper.sourceTable.getRowSet().isNonempty() ? helper.sourceTable.getRowSet().lastRowKey()
                                : -1);
                final TableUpdateImpl update = new TableUpdateImpl();
                update.added = RowSetFactory.fromRange(lastKey + 1,
                        lastKey + Math.max(1, helper.size / maxSteps));
                update.removed = i();
                update.modified = i();
                update.shifted = RowSetShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update,
                        helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 2, 10}) {
                for (final int numConsumerCoalesce : new int[] {1, 2, 10}) {
                    runOne.accept(new SharedProducerForAllClients(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    @Test
    public void testPrependIncrementalSharedProducer() {
        final int MAX_STEPS = 100;
        final Consumer<TestHelper> runOne = helper -> {
            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey =
                        helper.sourceTable.getRowSet().isNonempty() ? helper.sourceTable.getRowSet().lastRowKey() : -1;
                final TableUpdateImpl update = new TableUpdateImpl();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = RowSetFactory.fromRange(0, stepSize - 1);
                update.removed = i();
                update.modified = i();
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final RowSetShiftData.Builder shifted = new RowSetShiftData.Builder();
                if (lastKey >= 0) {
                    shifted.shiftRange(0, lastKey, stepSize + (Math.abs(helper.random.nextLong()) % 16));
                }
                update.shifted = shifted.build();

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update,
                        helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalsce : new int[] {1, 10}) {
                for (final int numConsumerCoalesce : new int[] {1, 10}) {
                    runOne.accept(new SharedProducerForAllClients(numProducerCoalsce, numConsumerCoalesce, size, 0,
                            new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    @Test
    public void testRoundTripIncrementalSharedProducer() {
        final Consumer<TestHelper> runOne = helper -> {
            helper.runTest(() -> {
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                        GenerateTableUpdates.DEFAULT_PROFILE,
                        helper.size, helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 10}) {
                for (final int numConsumerCoalesce : new int[] {1, 10}) {
                    runOne.accept(new SharedProducerForAllClients(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(100)));
                }
            }
        }
    }

    @Test
    public void testUsePrevOnSnapshot() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(10, 12).toTracking(), col("intCol", 10, 12));
        final RemoteNugget remoteNugget = new RemoteNugget(() -> queryTable);
        final MutableObject<RemoteClient> remoteClient = new MutableObject<>();

        // flush producer in the middle of the cycle -- but we need a different thread to usePrev
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(queryTable, i(10, 12));
            TstUtils.addToTable(queryTable, i(5, 7), col("intCol", 10, 12));

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(5, 12, -5);

            queryTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    shiftBuilder.build(), ModifiedColumnSet.EMPTY));

            final BitSet cols = new BitSet(1);
            cols.set(0);
            remoteClient.setValue(
                    remoteNugget.newClient(RowSetFactory.fromRange(0, 1), cols, "prevSnapshot"));

            // flush producer in the middle of the cycle -- but we need a different thread to usePrev
            final Thread thread = new Thread(this::flushProducerTable);
            thread.start();
            do {
                try {
                    thread.join();
                } catch (final InterruptedException ignored) {

                }
            } while (thread.isAlive());
        });

        // We also have to flush the delta which is now in the pending list.
        flushProducerTable();

        // We expect two pending messages for our client: snapshot in prev and the shift update
        Assert.equals(remoteClient.getValue().commandQueue.size(), "remoteClient.getValue().commandQueue.size()", 2);
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        // validate
        remoteNugget.validate("post flush");
    }

    @Test
    public void testRegressModificationsInPrevView() {
        final BitSet allColumns = new BitSet(1);
        allColumns.set(0);

        final QueryTable queryTable = TstUtils.testRefreshingTable(i(5, 10, 12).toTracking(),
                col("intCol", 5, 10, 12));
        final RemoteNugget remoteNugget = new RemoteNugget(() -> queryTable);

        // Set original viewport.
        final RemoteClient remoteClient =
                remoteNugget.newClient(RowSetFactory.fromRange(1, 2), allColumns, "prevSnapshot");

        // Obtain snapshot of original viewport.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("original viewport");

        // Change viewport without overlap.
        remoteClient.setViewport(RowSetFactory.fromRange(0, 1));

        // Modify row that is outside of new viewport but in original.
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(queryTable, i(12), col("intCol", 13));

            queryTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    RowSetFactory.fromKeys(12),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
        });

        // Do not allow the two updates to coalesce; we must force the consumer to apply the modification. (An allowed
        // race.)
        flushProducerTable();

        // Add rows to shift modified row into new viewport.
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(queryTable, i(5));

            queryTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(),
                    RowSetFactory.fromKeys(5),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        // Obtain snapshot of new viewport. (which will not include the modified row)
        flushProducerTable();
        Assert.equals(remoteClient.commandQueue.size(), "remoteClient.getValue().commandQueue.size()", 3); // mod, add,
                                                                                                           // snaphot
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("new viewport with modification");
    }

    @Test
    public void testCoalescingLargeUpdates() {
        final BitSet allColumns = new BitSet(1);
        allColumns.set(0);

        final QueryTable sourceTable = TstUtils.testRefreshingTable(i().toTracking());
        sourceTable.setFlat();
        final QueryTable queryTable = (QueryTable) sourceTable.updateView("data = (short) k");

        final RemoteNugget remoteNugget = new RemoteNugget(() -> queryTable);

        // Create a few interesting clients around the mapping boundary.
        final int mb = SNAPSHOT_CHUNK_SIZE;
        final long sz = 2L * mb;
        // noinspection unused
        final RemoteClient[] remoteClients = new RemoteClient[] {
                remoteNugget.newClient(null, allColumns, "full"),
                remoteNugget.newClient(RowSetFactory.fromRange(0, 100), allColumns, "start"),
                remoteNugget.newClient(RowSetFactory.fromRange(mb - 100, mb + 100), allColumns, "middle"),
                remoteNugget.newClient(RowSetFactory.fromRange(sz - 100, sz + 100), allColumns, "end"),
        };

        // Obtain snapshot of original viewport.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("original viewport");

        // Add all of our new rows spread over multiple deltas.
        final int numDeltas = 4;
        final long blockSize = sz / numDeltas;
        for (int ii = 0; ii < numDeltas; ++ii) {
            final RowSet newRows = RowSetFactory.fromRange(ii * blockSize, (ii + 1) * blockSize - 1);
            updateGraph.runWithinUnitTestCycle(() -> {
                TstUtils.addToTable(sourceTable, newRows);
                sourceTable.notifyListeners(new TableUpdateImpl(
                        newRows,
                        RowSetFactory.empty(),
                        RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
            });
        }

        // Coalesce these to ensure mappings larger than a single chunk are handled correctly.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("large add rows update");

        // Modify all of our rows spread over multiple deltas.
        for (int ii = 0; ii < numDeltas; ++ii) {
            final RowSetBuilderSequential modRowsBuilder = RowSetFactory.builderSequential();
            for (int jj = ii; jj < sz; jj += numDeltas) {
                modRowsBuilder.appendKey(jj);
            }
            updateGraph.runWithinUnitTestCycle(() -> {
                sourceTable.notifyListeners(new TableUpdateImpl(
                        RowSetFactory.empty(),
                        RowSetFactory.empty(),
                        modRowsBuilder.build(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
            });
        }

        // Coalesce these to ensure mappings larger than a single chunk are handled correctly.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("large mod rows update");
    }

    @Test
    public void testVectorConcurrentModification() {
        // this is a regression test for DH-19238; Barrage was not creating a static copy of ColumnWrapped vectors
        final BitSet allColumns = new BitSet(2);
        allColumns.set(0, 2);

        final QueryTable queryTable = TstUtils.testRefreshingTable(RowSetFactory.flat(4).toTracking(),
                col("Sym", "ZVZZT", "ZVZZT", "ZVZZT", "ZVZZT"),
                col("Val", 1, 1, 1, 1));
        final RemoteNugget remoteNugget = new RemoteNugget(() -> queryTable.groupBy("Sym"));

        final RemoteClient remoteClient = remoteNugget.newClient(null, allColumns, "client");

        // Obtain snapshot of original table.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("original viewport");

        // Let's assert that our column source is virtual (important that the test is capable of failing).
        Assert.eq(remoteNugget.originalTable.getColumnSource("Val").get(0).getClass(),
                "remoteNugget.originalTable.getColumnSource(\"Val\").get(0).getClass()",
                IntVectorColumnWrapper.class);

        // Queue up a change to make Val == 2
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(queryTable, queryTable.getRowSet(),
                    col("Sym", "ZVZZT", "ZVZZT", "ZVZZT", "ZVZZT"),
                    col("Val", 2, 2, 2, 2));

            queryTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    queryTable.getRowSet().copy(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
        });

        // Queue up another change, but flush BMP before the table ticks. Client should receive 2's not 3's.
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(queryTable, queryTable.getRowSet(),
                    col("Sym", "ZVZZT", "ZVZZT", "ZVZZT", "ZVZZT"),
                    col("Val", 3, 3, 3, 3));

            flushProducerTable();

            queryTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    queryTable.getRowSet().copy(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
        });

        // we're expecting a single update in the queue, flush and propagate to BarrageTable
        Assert.equals(remoteClient.commandQueue.size(), "remoteClient.getValue().commandQueue.size()", 1);
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        IntVector destVal = (IntVector) remoteClient.barrageTable.getColumnSource("Val").get(0);
        Assert.neqNull(destVal, "destVal");
        Assert.eq(destVal.size(), "destVal.size()", queryTable.size(), "queryTable.size()");
        for (int ii = 0; ii < destVal.size(); ++ii) {
            Assert.eq(destVal.get(ii), "destVal.get(ii)", 2);
        }

        // flush bmp and client one last time so we properly cleanup the test's chunks
        flushProducerTable();
        Assert.equals(remoteClient.commandQueue.size(), "remoteClient.getValue().commandQueue.size()", 1);
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("end-of-test tables should match");
    }
}
