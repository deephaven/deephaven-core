//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import com.google.protobuf.CodedInputStream;
import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.client.impl.BarrageSubscriptionImpl.BarrageDataMarshaller;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.vectors.IntVectorColumnWrapper;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.extensions.barrage.util.GrpcMarshallingException;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.IntVector;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.experimental.categories.Category;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.SNAPSHOT_CHUNK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;

/**
 * Barrage round-trip coverage for subscription and viewport changes: column subscriptions, forward and reverse
 * viewports, mid-cycle subscription, and simultaneous subscription changes.
 */
@Category(OutOfBandTest.class)
public class BarrageMessageSubscriptionRoundTripTest extends BarrageMessageRoundTripTestBase {

    public void testColumnSubChange() {
        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    for (int subProducerCoalesce =
                            0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce =
                                0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = subProducerCoalesce;
                            final int finalSubConsumerCoalesce = subConsumerCoalesce;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0,
                                    new MutableInt(4)) {
                                {
                                    for (final RemoteNugget nugget : nuggets) {
                                        final BitSet columns = new BitSet();
                                        columns.set(0, nugget.originalTable.numColumns() / 2);
                                        nugget.clients.add(new RemoteClient(
                                                RowSetFactory.fromRange(size / 5, 2L * size / 5),
                                                columns, nugget.barrageMessageProducer, nugget.originalTable,
                                                "sub-changer"));
                                    }
                                }

                                void maybeChangeSub(final int step, final int rt, final int pt) {
                                    if (step != 2 || rt != finalSubConsumerCoalesce || pt != finalSubProducerCoalesce) {
                                        return;
                                    }

                                    for (final RemoteNugget nugget : nuggets) {
                                        final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                        final BitSet columns = new BitSet();
                                        final int numColumns = nugget.originalTable.numColumns();
                                        columns.set(numColumns / 2, numColumns);
                                        client.setSubscribedColumns(columns);
                                    }
                                }
                            }.runTest();
                        }
                    }
                }
            }
        }
    }

    public void testViewportChange() {
        for (final int size : new int[] {10, 100}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    for (int subProducerCoalesce =
                            0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce =
                                0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = 0;
                            final int finalSubConsumerCoalesce = 1;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0,
                                    new MutableInt(25)) {
                                @Override
                                void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
                                    final RemoteNugget nugget = new RemoteNugget(makeTable);
                                    nuggets.add(nugget);

                                    final BitSet columns = new BitSet();
                                    columns.set(0, 4);
                                    nugget.clients.add(
                                            new RemoteClient(RowSetFactory.fromRange(0, size / 5),
                                                    columns, nugget.barrageMessageProducer, nugget.originalTable,
                                                    "sub-changer"));
                                }

                                void maybeChangeSub(final int step, final int rt, final int pt) {
                                    if (step % 2 != 0 || rt != finalSubConsumerCoalesce
                                            || pt != finalSubProducerCoalesce) {
                                        return;
                                    }

                                    for (final RemoteNugget nugget : nuggets) {
                                        final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                        final WritableRowSet viewport = client.viewport.copy();
                                        viewport.shiftInPlace(Math.max(size / 25, 1));

                                        // maintain viewport direction in this test
                                        client.setViewport(viewport, client.reverseViewport);
                                    }
                                }
                            }.runTest();
                        }
                    }
                }
            }
        }
    }

    public void testViewportDirectionChange() {
        for (final int size : new int[] {10, 100}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    for (int subProducerCoalesce =
                            0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce =
                                0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = 0;
                            final int finalSubConsumerCoalesce = 1;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0,
                                    new MutableInt(25)) {
                                @Override
                                void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
                                    final RemoteNugget nugget = new RemoteNugget(makeTable);
                                    nuggets.add(nugget);

                                    final BitSet columns = new BitSet();
                                    columns.set(0, 4);
                                    nugget.clients.add(
                                            new RemoteClient(RowSetFactory.fromRange(0, size / 5),
                                                    columns, nugget.barrageMessageProducer, nugget.originalTable,
                                                    "sub-changer"));
                                }

                                void maybeChangeSub(final int step, final int rt, final int pt) {
                                    if (step % 2 != 0 || rt != finalSubConsumerCoalesce
                                            || pt != finalSubProducerCoalesce) {
                                        return;
                                    }

                                    for (final RemoteNugget nugget : nuggets) {
                                        final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                        final WritableRowSet viewport = client.viewport.copy();
                                        viewport.shiftInPlace(Math.max(size / 25, 1));

                                        // alternate viewport direction with every call to this function
                                        client.setViewport(viewport, !client.reverseViewport);
                                    }
                                }
                            }.runTest();
                        }
                    }
                }
            }
        }
    }

    public void testOverlappedColumnSubsChange() {
        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    for (int subProducerCoalesce =
                            0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce =
                                0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = subProducerCoalesce;
                            final int finalSubConsumerCoalesce = subConsumerCoalesce;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0,
                                    new MutableInt(4)) {
                                {
                                    for (final RemoteNugget nugget : nuggets) {
                                        final BitSet columns = new BitSet();
                                        columns.set(0, 3);
                                        nugget.clients.add(new RemoteClient(
                                                RowSetFactory.fromRange(size / 5, 2L * size / 5),
                                                columns, nugget.barrageMessageProducer, nugget.originalTable,
                                                "sub-changer"));
                                    }
                                }

                                void maybeChangeSub(final int step, final int rt, final int pt) {
                                    if (step != 2 || rt != finalSubConsumerCoalesce || pt != finalSubProducerCoalesce) {
                                        return;
                                    }

                                    for (final RemoteNugget nugget : nuggets) {
                                        final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                        final BitSet columns = new BitSet();
                                        columns.set(1, 4);
                                        client.setSubscribedColumns(columns);
                                    }
                                }
                            }.runTest();
                        }
                    }
                }
            }
        }
    }

    public void testViewportSubscribeMidCycle() {
        // This is a regression test for IDS-6392. It catches a race between when a subscription becomes active and
        // when the viewport becomes active post-snapshot.
        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {2, 3, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(4)) {

                        void maybeChangeSub(final int step, final int rt, final int pt) {
                            if (step != 0 || rt != 0 || pt != 1) {
                                // Only subscribe after we have sent at least one update, but no need to subscribe
                                // again.
                                return;
                            }

                            nuggets.forEach((nugget) -> nugget.clients.forEach(RemoteClient::doSubscribe));
                        }

                        @Override
                        void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
                            if (!nuggets.isEmpty()) {
                                return; // we can only have a single nugget since they all share a single source table
                            }

                            final RemoteNugget nugget = new RemoteNugget(makeTable) {
                                @Override
                                public void onGetSnapshot() {
                                    final ControlledUpdateGraph updateGraph =
                                            ExecutionContext.getContext().getUpdateGraph().cast();
                                    updateGraph.runWithinUnitTestCycle(
                                            () -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                                                    GenerateTableUpdates.DEFAULT_PROFILE, size,
                                                    random, sourceTable, columnInfo));
                                }
                            };
                            nuggets.add(nugget);

                            // we can only have viewport subscriptions or else we won't tickle the original bug
                            final BitSet columns = new BitSet();
                            columns.set(0, 4);
                            final boolean deferSubscription = true;
                            nugget.clients.add(new RemoteClient(
                                    RowSetFactory.fromRange(size / 5, 2L * size / 5),
                                    columns, nugget.barrageMessageProducer, nugget.originalTable,
                                    "sub-changer", false, deferSubscription));

                        }
                    }.runTest();
                }
            }
        }
    }

    public void testOverlappingViewportChange() {
        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    for (int subProducerCoalesce =
                            0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce =
                                0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = subProducerCoalesce;
                            final int finalSubConsumerCoalesce = subConsumerCoalesce;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0,
                                    new MutableInt(4)) {
                                @Override
                                public void createNuggets() {
                                    super.createNuggets();

                                    for (final RemoteNugget nugget : nuggets) {
                                        final BitSet columns = new BitSet();
                                        columns.set(0, 4);
                                        nugget.clients.add(new RemoteClient(
                                                RowSetFactory.fromRange(size / 5, 3L * size / 5),
                                                columns, nugget.barrageMessageProducer, nugget.originalTable,
                                                "sub-changer"));
                                    }
                                }

                                void maybeChangeSub(final int step, final int rt, final int pt) {
                                    if (step != 2 || rt != finalSubConsumerCoalesce || pt != finalSubProducerCoalesce) {
                                        return;
                                    }

                                    for (final RemoteNugget nugget : nuggets) {
                                        final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                        final WritableRowSet viewport = client.viewport.copy();
                                        viewport.shiftInPlace(size / 5);

                                        // maintain viewport direction in this test
                                        client.setViewport(viewport, client.reverseViewport);
                                    }
                                }
                            }.runTest();
                        }
                    }
                }
            }
        }
    }

    public void testSimultaneousSubscriptionChanges() {
        for (final int size : new int[] {10, 100, 1000}) {
            final int numProducerCoalesce = 8;
            final int numConsumerCoalesce = 8;
            for (int subConsumerCoalesce = 0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                final int finalSubConsumerCoalesce = subConsumerCoalesce;
                new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(4)) {
                    {
                        for (final RemoteNugget nugget : nuggets) {
                            final BitSet columns = new BitSet();
                            columns.set(0, 4);
                            nugget.clients.add(new RemoteClient(
                                    RowSetFactory.fromRange(size / 5, 2L * size / 5),
                                    columns, nugget.barrageMessageProducer, nugget.originalTable, "sub-changer"));
                        }
                    }

                    void maybeChangeSub(final int step, final int rt, final int pt) {
                        if (step != 2 || rt != finalSubConsumerCoalesce) {
                            return;
                        }

                        for (final RemoteNugget nugget : nuggets) {
                            final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                            final int firstKey = random.nextInt(size);
                            client.setViewport(RowSetFactory.fromRange(firstKey,
                                    firstKey + random.nextInt(size - firstKey)), client.reverseViewport);
                        }
                    }
                }.runTest();
            }
        }
    }

    /**
     * Bug-fix verification: when a full subscription and a gapped-viewport subscription coexist, the producer stores
     * all modified rows in a single delta (not just the viewport intersection). The viewport client's modOffsets then
     * map contiguous viewport positions to non-contiguous data positions (with a gap). In appendModColumns, maxLength
     * is computed as a data-position-space distance but then used as a row-index count, so modOffsets.get(endRange) can
     * return a data position past the chunk boundary, triggering "Subset is out of bounds for context of size N".
     */
    public void testModColumnChunkBoundaryWithGappedViewport() {
        final BitSet allColumns = new BitSet(1);
        allColumns.set(0);

        // Use a flat table with enough rows to span 2+ delta chunks when fully modified.
        final long numRows = 2L * BarrageMessageProducer.DELTA_CHUNK_SIZE + 100;

        final QueryTable sourceTable = TstUtils.testRefreshingTable(i().toTracking());
        sourceTable.setFlat();
        final QueryTable queryTable = (QueryTable) sourceTable.updateView("data = (short) k");

        final RemoteNugget remoteNugget = new RemoteNugget(() -> queryTable);

        // A full subscription is required so that modsToRecord = allRows (not just the viewport
        // intersection). This makes rowsModified.original contiguous over all rows, which in turn
        // makes clientModdedRowOffsets for the gapped-viewport client non-contiguous (gapped) —
        // the precondition for the appendModColumns chunk-boundary bug.
        // noinspection unused
        final RemoteClient fullClient = remoteNugget.newClient(null, allColumns, "full");

        // Create a viewport with a gap in the first chunk's range. The gap ensures that modOffsets
        // is non-contiguous: viewport row indices 0..N map to data positions with a hole, so N row
        // indices can map to data positions past the chunk boundary.
        final long gapStart = BarrageMessageProducer.DELTA_CHUNK_SIZE / 2;
        final long gapSize = BarrageMessageProducer.DELTA_CHUNK_SIZE / 4;
        final RemoteClient remoteClient;
        try (final RowSet before = RowSetFactory.fromRange(0, gapStart - 1);
                final RowSet after = RowSetFactory.fromRange(gapStart + gapSize, numRows - 1)) {
            final RowSet viewport = before.union(after);

            // noinspection unused
            remoteClient = remoteNugget.newClient(viewport, allColumns, "gapped-viewport");
        }

        // Obtain snapshot.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        // Add all rows in one delta.
        final RowSet allRows = RowSetFactory.fromRange(0, numRows - 1);
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(sourceTable, allRows);
            // This results in queryTable downstream update and the BMP propagating the adds.
            sourceTable.notifyListeners(new TableUpdateImpl(
                    allRows.copy(),
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        flushProducerTable();
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("after initial add");

        // Modify ALL rows in a single delta. With the gapped viewport, the mod offsets will have
        // a hole that causes appendModColumns to compute endPos past the first chunk's boundary.
        updateGraph.runWithinUnitTestCycle(() -> {
            // Faking a mods update from queryTable (since mods on a column-less table are impossible,
            // doing this to trigger the BMP to reproduce the bug).
            queryTable.notifyListeners(new TableUpdateImpl(
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    allRows.copy(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.ALL));
        });

        // This flush serializes the mod message; without the fix it throws:
        // "Subset {..} is out of bounds for context of size N"
        flushProducerTable();
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("mod spanning chunks with gapped viewport");
    }

    /**
     * The server grows a large full subscription's initial snapshot over several rounds; every growth message
     * re-declares the entire consistent key space as {@code added} while only {@code rowsIncluded} is incremental. A
     * client that consumes those messages across more than one of its own update graph cycles must publish none of
     * them: after the first cycle the table already contains the whole key space, so a second update would claim rows
     * as added that are already present. That is an invalid update in general, and it trips the
     * {@link Table#APPEND_ONLY_TABLE_ATTRIBUTE} invariant that
     * {@link io.deephaven.engine.table.impl.BaseTable#notifyListeners} checks whether or not a listener is attached.
     * <p>
     * The source is then ticked so that the deltas following the growth are validated against the contents the
     * replicated table was left holding.
     */
    public void testGrowingFullSubscriptionAppendOnly() {
        // The server ships at most MIN_SNAPSHOT_CELL_COUNT cells per growth round, so a single-column table needs more
        // rows than that for the subscription to be grown across multiple messages.
        final int size = (int) (BarrageUtil.MIN_SNAPSHOT_CELL_COUNT * 3);
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));
        sourceTable.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient client = nugget.newClient(null, allCols, "growing-append-only");

        // let the server produce every growth message for the initial snapshot
        flushProducerTable();
        assertTrue("expected a growing snapshot, but the server sent " + client.pendingMessageCount() + " message(s)",
                client.pendingMessageCount() > 1);

        // deliver them one at a time, running an update graph cycle in between as a live client would
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        while (client.pendingMessageCount() > 0) {
            client.flushEventsToReplicatedTable(1);
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        }

        // a null server viewport is how the server acknowledges that the full subscription is satisfied
        assertNull(client.barrageTable.getServerViewport());
        nugget.validate("growing append-only full subscription");

        // Tick the source now that the subscription is satisfied. The replicated table's update validator was attached
        // by the validate() above, taking the grown contents as its initial state, so these deltas must line up with
        // what the growth left behind.
        for (int step = 0; step < 3; ++step) {
            final int firstRow = size + step * 10;
            updateGraph.runWithinUnitTestCycle(() -> {
                final int[] newValues = new int[10];
                for (int ii = 0; ii < newValues.length; ++ii) {
                    newValues[ii] = firstRow + ii;
                }
                final RowSet added = RowSetFactory.fromRange(firstRow, firstRow + newValues.length - 1);
                TstUtils.addToTable(sourceTable, added, TableTools.intCol("intCol", newValues));
                sourceTable.notifyListeners(new TableUpdateImpl(added, RowSetFactory.empty(), RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("post-growth step " + step);
        }
    }

    /**
     * An incomplete full subscription publishes no updates for the rows it is being given, so a listener attached
     * before the server acknowledges the subscription would silently never hear about them. Attaching one must fail
     * instead.
     */
    public void testListenToIncompleteFullSubscription() {
        final int size = (int) (BarrageUtil.MIN_SNAPSHOT_CELL_COUNT * 3);
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient client = nugget.newClient(null, allCols, "incomplete-listen");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        // Deliver only the first of the growth messages, leaving the subscription unsatisfied.
        flushProducerTable();
        assertTrue(client.pendingMessageCount() > 1);
        client.flushEventsToReplicatedTable(1);
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        assertNotNull(client.barrageTable.getServerViewport());

        try {
            client.barrageTable.addUpdateListener(new FailureListener("Listener on incomplete table"));
            TestCase.fail("expected an IllegalStateException listening to an incomplete table");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("incomplete table"));
        }

        // Once the subscription is satisfied the table may be listened to as usual.
        while (client.pendingMessageCount() > 0) {
            client.flushEventsToReplicatedTable(1);
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        }
        assertNull(client.barrageTable.getServerViewport());
        nugget.validate("satisfied full subscription");
    }
}
