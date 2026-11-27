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
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.ColumnHolder;
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
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
import java.util.stream.IntStream;

import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.SNAPSHOT_CHUNK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;
import static org.junit.Assert.*;

/**
 * Barrage round-trip coverage for subscription and viewport changes: column subscriptions, forward and reverse
 * viewports, mid-cycle subscription, and simultaneous subscription changes, which
 * {@link BarrageMessageSubscriptionRoundTripTest} runs with serial writes to subscribers and
 * {@link BarrageMessageSubscriptionRoundTripParallelTest} with parallel ones.
 */
@Category(OutOfBandTest.class)
public abstract class BarrageMessageSubscriptionRoundTripTestBase extends BarrageMessageRoundTripTestBase {

    /**
     * The coalesced updates, out of {@code numCoalesce}, before which a test makes its subscription change: each of
     * them by default, so that the change lands at every point of a coalesced propagation.
     */
    protected int[] subscriptionChangeSteps(final int numCoalesce) {
        return IntStream.range(0, numCoalesce).toArray();
    }

    @Test
    public void testColumnSubChange() {
        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    for (final int subProducerCoalesce : subscriptionChangeSteps(numProducerCoalesce)) {
                        for (final int subConsumerCoalesce : subscriptionChangeSteps(numConsumerCoalesce)) {
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
                                    if (step != 2 || rt != subConsumerCoalesce || pt != subProducerCoalesce) {
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

    @Test
    public void testOverlappedColumnSubsChange() {
        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    for (final int subProducerCoalesce : subscriptionChangeSteps(numProducerCoalesce)) {
                        for (final int subConsumerCoalesce : subscriptionChangeSteps(numConsumerCoalesce)) {
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
                                    if (step != 2 || rt != subConsumerCoalesce || pt != subProducerCoalesce) {
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

    /**
     * Subscriptions to some of a table's columns share a producer with subscriptions to all of them, so the producer's
     * messages carry every column. {@link RemoteClient} checks that each subscription is sent only its own columns'
     * data, and that its replicated table reads null in, and never reports modified, the others.
     */
    @Test
    public void testColumnSubsetsShareProducerWithAllColumnSubscriptions() {
        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    final TestHelper helper = new SharedProducerForAllClients(numProducerCoalesce,
                            numConsumerCoalesce, size, 0, new MutableInt(20)) {
                        @Override
                        void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
                            super.createNuggetsForTableMaker(makeTable);
                            final RemoteNugget nugget = nuggets.get(nuggets.size() - 1);
                            final int numColumns = nugget.originalTable.numColumns();
                            final BitSet firstHalf = new BitSet();
                            firstHalf.set(0, Math.max(1, numColumns / 2));
                            final BitSet everyOther = new BitSet();
                            for (int ci = 0; ci < numColumns; ci += 2) {
                                everyOther.set(ci);
                            }
                            nugget.newClient(null, firstHalf, "full first half");
                            nugget.newClient(null, everyOther, "full every other");
                            nugget.newClient(RowSetFactory.fromRange(0, size / 10), everyOther, "header every other");
                            nugget.newClient(RowSetFactory.fromRange(size / 2, size * 3L / 4), firstHalf, true,
                                    "reverse floating first half");
                        }
                    };
                    helper.runTest(() -> {
                        final ControlledUpdateGraph updateGraph =
                                ExecutionContext.getContext().getUpdateGraph().cast();
                        updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                                GenerateTableUpdates.DEFAULT_PROFILE, helper.size, helper.random, helper.sourceTable,
                                helper.columnInfo));
                    });
                }
            }
        }
    }

    @Test
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

    @Test
    public void testOverlappingViewportChange() {
        for (final int size : new int[] {10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[] {1, 4}) {
                for (final int numConsumerCoalesce : new int[] {1, 4}) {
                    for (final int subProducerCoalesce : subscriptionChangeSteps(numProducerCoalesce)) {
                        for (final int subConsumerCoalesce : subscriptionChangeSteps(numConsumerCoalesce)) {
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
                                    if (step != 2 || rt != subConsumerCoalesce || pt != subProducerCoalesce) {
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

    /**
     * A subscription removed while its table is quiet is still completed. The producer run that removes it has nothing
     * else to send, and used to return before ending the subscriber's stream, leaving a client that half-closed its
     * subscription waiting for an end that never came.
     */
    @Test
    public void testRemovedSubscriptionIsCompletedWhileTheTableIsQuiet() {
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(10).toTracking(), TableTools.intCol("intCol", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient client = nugget.newClient(null, allCols, "removed-while-quiet");
        // send the whole initial snapshot and apply it, so that nothing is left to propagate
        flushProducerTable();
        nugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        assertFalse(client.dummyObserver.completed);

        nugget.barrageMessageProducer.removeSubscription(client.dummyObserver);
        flushProducerTable();

        assertTrue("the removed subscription's stream was not completed", client.dummyObserver.completed);
    }

    /**
     * A subscription removed before the producer first runs for it is still completed. It was never activated, so the
     * producer used to drop it with its pending changes without ending the subscriber's stream.
     */
    @Test
    public void testSubscriptionRemovedBeforeItsFirstRunIsCompleted() {
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(10).toTracking(), TableTools.intCol("intCol", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient client = nugget.newClient(null, allCols, "removed-before-first-run");
        nugget.barrageMessageProducer.removeSubscription(client.dummyObserver);
        assertFalse(client.dummyObserver.completed);

        flushProducerTable();

        assertTrue("the removed subscription's stream was not completed", client.dummyObserver.completed);
    }

    /**
     * A viewport subscription removed before the producer first runs for it releases the viewport it was given, which
     * it owned and which nothing else will close.
     */
    @Test
    public void testViewportRemovedBeforeItsFirstRunIsReleased() {
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(10).toTracking(), TableTools.intCol("intCol", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        // subscribed directly, rather than through a RemoteClient, which hands the producer a copy of its viewport
        final AtomicBoolean completed = new AtomicBoolean();
        final StreamObserver<BarrageMessageWriter.MessageView> observer = new StreamObserver<>() {
            @Override
            public void onNext(final BarrageMessageWriter.MessageView messageView) {}

            @Override
            public void onError(final Throwable t) {}

            @Override
            public void onCompleted() {
                completed.set(true);
            }
        };
        final WritableRowSet viewport = RowSetFactory.fromRange(0, 4);
        nugget.barrageMessageProducer.addSubscription(
                observer, BarrageSubscriptionOptions.builder().build(), allCols, viewport, false);
        nugget.barrageMessageProducer.removeSubscription(observer);

        flushProducerTable();

        assertTrue("the removed subscription's stream was not completed", completed.get());
        // a closed row set fails on use
        org.junit.Assert.assertThrows(NullPointerException.class, viewport::size);
    }

    @Test
    public void testSimultaneousSubscriptionChanges() {
        for (final int size : new int[] {10, 100, 1000}) {
            final int numProducerCoalesce = 8;
            final int numConsumerCoalesce = 8;
            for (final int subConsumerCoalesce : subscriptionChangeSteps(numConsumerCoalesce)) {
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
                        if (step != 2 || rt != subConsumerCoalesce) {
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
    @Test
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
     * A message holds added rows only for the columns that some subscription needs, so a message holds no added rows
     * for the table's first column when no subscription has that column. The added rows of the other columns must still
     * be split into record batches at the chunk boundaries of those other columns. Without that split, a delta that
     * adds more rows than one chunk holds fails with "Subset {..} is out of bounds for context of size N".
     * <p>
     * A delta exercises the failure deterministically. A growing snapshot reaches the same writer code, but the size of
     * each growth round depends on timing, so a snapshot message of more than one chunk cannot be forced here.
     */
    @Test
    public void testAddedRowsSpanningChunksWithoutTheFirstColumn() {
        final QueryTable sourceTable = TstUtils.testRefreshingTable(RowSetFactory.empty().toTracking(),
                TableTools.intCol("I"), TableTools.stringCol("S"));
        sourceTable.setFlat();

        // every column except the first
        final BitSet allButFirst = new BitSet();
        allButFirst.set(1, sourceTable.numColumns());

        final RemoteNugget remoteNugget = new RemoteNugget(() -> sourceTable);
        // noinspection unused
        final RemoteClient fullClient = remoteNugget.newClient(null, allButFirst, "full-without-first-column");
        // noinspection unused
        final RemoteClient viewportClient = remoteNugget.newClient(RowSetFactory.fromRange(0, Long.MAX_VALUE - 1),
                allButFirst, "viewport-without-first-column");

        // Obtain the (empty) initial snapshot.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("initial snapshot");

        // Add enough rows in one delta to fill more than two delta chunks.
        final int numRows = 2 * BarrageMessageProducer.DELTA_CHUNK_SIZE + 100;
        final int[] ints = new int[numRows];
        final String[] strings = new String[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            ints[ii] = ii;
            strings[ii] = ii % 7 == 0 ? null : Integer.toString(ii);
        }
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.flat(numRows);
            TstUtils.addToTable(sourceTable, added, TableTools.intCol("I", ints), TableTools.stringCol("S", strings));
            sourceTable.notifyListeners(new TableUpdateImpl(added, RowSetFactory.empty(), RowSetFactory.empty(),
                    RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        // This flush serializes the delta; without the fix it throws:
        // "Subset {..} is out of bounds for context of size N"
        flushProducerTable();
        remoteNugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        remoteNugget.validate("added rows spanning chunks without the first column");
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
    @Test
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
            client.flushMessages(1);
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
    @Test
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
        client.flushMessages(1);
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        assertNotNull(client.barrageTable.getServerViewport());

        try {
            client.barrageTable.addUpdateListener(new FailureListener("Listener on incomplete table"));
            fail("expected an IllegalStateException listening to an incomplete table");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("incomplete table"));
        }

        // Once the subscription is satisfied the table may be listened to as usual.
        while (client.pendingMessageCount() > 0) {
            client.flushMessages(1);
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        }
        assertNull(client.barrageTable.getServerViewport());
        nugget.validate("satisfied full subscription");
    }

    /**
     * A full subscription that joins while another full subscription is still growing must get a share of the next
     * growth round's row budget. A round spends the budget only on positions that hold rows. A round that spent the
     * budget on positions past the end of the table for the subscription processed first left the late subscription
     * only the rows the round snapshots for the early subscription, with the late subscription's own first rows a round
     * later and out of key order.
     *
     * <p>
     * The table has one column, so a round's row budget equals the round's cell budget. The table holds half again as
     * many rows as the first round's budget of {@link BarrageUtil#MIN_SNAPSHOT_CELL_COUNT}. The first round grows only
     * the early subscription, and the late subscription joins during that round's snapshot. No later round's budget is
     * smaller than the first round's, and finishing the early subscription costs half of the first round's budget, so
     * the second round has budget left for the late subscription's first half-budget of rows, whatever the producer
     * measured the first snapshot to cost.
     */
    @Test
    public void testLateFullSubscriptionSharesGrowthBudget() {
        final int firstRoundRows = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;
        final int size = firstRoundRows + firstRoundRows / 2;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final MutableObject<RemoteClient> lateClientHolder = new MutableObject<>();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable) {
            @Override
            public void onGetSnapshot() {
                // the first snapshot is the early subscription's first growth round
                if (lateClientHolder.getValue() == null) {
                    lateClientHolder.setValue(newClient(null, allCols, "late-full"));
                }
            }
        };
        final RemoteClient earlyClient = nugget.newClient(null, allCols, "early-full");

        flushProducerTable();
        final RemoteClient lateClient = lateClientHolder.getValue();
        assertNotNull("expected the late subscription to join during the first snapshot", lateClient);

        // the first round covers only part of the table, so the early subscription is still growing when the late
        // subscription joins
        final BarrageMessage earlyFirst = earlyClient.commandQueue.peek();
        assertNotNull("expected a message for the early subscription", earlyFirst);
        assertTrue("expected the early subscription's first message to be a snapshot", earlyFirst.isSnapshot);
        try (final RowSet firstRound = RowSetFactory.flat(firstRoundRows)) {
            assertEquals("the early subscription's first round", firstRound, earlyFirst.rowsIncluded);
        }

        // The second round charges the early subscription first, for the rows past the first round. The late
        // subscription shares those rows and spends the rest of the budget, at least lateOwnRows rows, on the late
        // subscription's own rows from the start of the table.
        final int lateOwnRows = firstRoundRows - (size - firstRoundRows);
        final BarrageMessage lateFirst = lateClient.commandQueue.peek();
        assertNotNull("expected a message for the late subscription", lateFirst);
        assertTrue("expected the late subscription's first message to be a snapshot", lateFirst.isSnapshot);
        assertTrue("expected the late subscription's own rows from the start of the table, got "
                + lateFirst.rowsIncluded, lateFirst.rowsIncluded.containsRange(0, lateOwnRows - 1));
        assertTrue("expected the rows shared with the early subscription, got " + lateFirst.rowsIncluded,
                lateFirst.rowsIncluded.containsRange(firstRoundRows, size - 1));

        nugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(earlyClient.barrageTable.getServerViewport());
        assertNull(lateClient.barrageTable.getServerViewport());
        nugget.validate("late full subscription during growth");
    }

    /**
     * A growth round's snapshot must stay within the round's row budget even when the table grows between planning the
     * round and taking the snapshot. A round reserves only positions that held rows when the round was planned, so the
     * rows the table gains before the snapshot wait for later rounds.
     *
     * <p>
     * Two full subscriptions start together on a one-column table of half the first round's budget of
     * {@link BarrageUtil#MIN_SNAPSHOT_CELL_COUNT}, so the round reserves every row. The table then grows by many
     * budgets' worth of rows just before the snapshot.
     */
    @Test
    public void testGrowthRoundStaysWithinBudgetWhenTableGrows() {
        final int budgetRows = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;
        final int initialSize = budgetRows / 2;
        final int grownSize = budgetRows * 10;
        final int[] values = new int[grownSize];
        for (int ii = 0; ii < grownSize; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(initialSize).toTracking(),
                TableTools.intCol("intCol", Arrays.copyOf(values, initialSize)));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final AtomicBoolean grown = new AtomicBoolean();
        nugget.barrageMessageProducer.setOnGetSnapshot(() -> {
            // grow the table after the first round is planned and before the first round's snapshot
            if (!grown.compareAndSet(false, true)) {
                return;
            }
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet added = RowSetFactory.fromRange(initialSize, grownSize - 1);
                TstUtils.addToTable(sourceTable, added,
                        TableTools.intCol("intCol", Arrays.copyOfRange(values, initialSize, grownSize)));
                sourceTable.notifyListeners(new TableUpdateImpl(added, RowSetFactory.empty(), RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });
        }, true);
        final RemoteClient firstClient = nugget.newClient(null, allCols, "first-full");
        final RemoteClient secondClient = nugget.newClient(null, allCols, "second-full");

        flushProducerTable();
        assertTrue("expected the table to grow during the first round", grown.get());

        // every row of the first round's snapshot reaches at least one of the subscriptions
        final BarrageMessage firstRoundOfFirst = firstClient.commandQueue.peek();
        final BarrageMessage firstRoundOfSecond = secondClient.commandQueue.peek();
        assertNotNull("expected a message for the first subscription", firstRoundOfFirst);
        assertNotNull("expected a message for the second subscription", firstRoundOfSecond);
        assertTrue("expected the first subscription's first message to be a snapshot", firstRoundOfFirst.isSnapshot);
        assertTrue("expected the second subscription's first message to be a snapshot", firstRoundOfSecond.isSnapshot);
        try (final RowSet firstRoundRows = firstRoundOfFirst.rowsIncluded.union(firstRoundOfSecond.rowsIncluded)) {
            assertTrue("expected the first round to snapshot at most " + budgetRows + " rows, got " + firstRoundRows,
                    firstRoundRows.size() <= budgetRows);
        }

        nugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(firstClient.barrageTable.getServerViewport());
        assertNull(secondClient.barrageTable.getServerViewport());
        nugget.validate("full subscriptions after the table grew during growth");
    }

    /**
     * A subscription to more columns than {@link BarrageUtil#MIN_SNAPSHOT_CELL_COUNT} must still grow. A growth round's
     * row budget is the round's cell budget divided by the subscribed column count, and the producer tunes the cell
     * budget only after a snapshot that includes rows. A row budget rounded down to zero would snapshot no rows, never
     * tune the cell budget, and leave the subscription growing forever.
     *
     * <p>
     * Every round must snapshot at least one row, so a hook fails the test once the producer takes more snapshots than
     * the table has rows. Without the hook, a regression would hang the test instead of failing the test.
     */
    @Test
    public void testSubscriptionWiderThanCellBudgetGrows() {
        final int numColumns = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT + 1;
        final int size = 3;
        final ColumnHolder<?>[] columns = new ColumnHolder[numColumns];
        for (int ci = 0; ci < numColumns; ++ci) {
            columns[ci] = TableTools.intCol("c" + ci, ci, ci + 1, ci + 2);
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(RowSetFactory.flat(size).toTracking(), columns);

        final BitSet allCols = new BitSet();
        allCols.set(0, numColumns);

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final MutableInt snapshotsAllowed = new MutableInt(size);
        nugget.barrageMessageProducer.setOnGetSnapshot(() -> {
            if (snapshotsAllowed.addAndGet(-1) < 0) {
                // an Error, so that the propagation job's catch of Exception cannot turn it into a subscription failure
                throw new AssertionError("expected every growth round to snapshot at least one row");
            }
        }, true);
        final RemoteClient client = nugget.newClient(null, allCols, "wide-full");

        flushProducerTable();
        nugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        assertTrue(client.name + ": " + client.dummyObserver.errors, client.dummyObserver.errors.isEmpty());
        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(client.barrageTable.getServerViewport());

        // check every cell directly; a table diff allocates a chunk for each column, too much at this width
        assertEquals(size, client.barrageTable.size());
        for (int ci = 0; ci < numColumns; ++ci) {
            final ColumnSource<?> replicated = client.barrageTable.getColumnSource("c" + ci);
            for (int ii = 0; ii < size; ++ii) {
                assertEquals("c" + ci + " row " + ii, ci + ii, replicated.getInt(ii));
            }
        }
    }

    /**
     * A viewport feeds a UI that someone is watching, so a viewport subscription is satisfied in the snapshot round
     * that picks up the subscription, even when the viewport holds more rows than the round's budget. A forward and a
     * reverse viewport of twice the first round's budget of {@link BarrageUtil#MIN_SNAPSHOT_CELL_COUNT} rows, on a
     * one-column table, each arrive in one snapshot message.
     */
    @Test
    public void testViewportSubscriptionSatisfiedInOneRound() {
        final int budgetRows = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;
        final int size = budgetRows * 4;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final int firstPosition = budgetRows / 2;
        final int lastPosition = firstPosition + 2 * budgetRows - 1;
        final RemoteClient forwardClient = nugget.newClient(
                RowSetFactory.fromRange(firstPosition, lastPosition), allCols, "forward-viewport");
        final RemoteClient reverseClient = nugget.newClient(
                RowSetFactory.fromRange(firstPosition, lastPosition), allCols, true, "reverse-viewport");

        flushProducerTable();
        for (final RemoteClient client : List.of(forwardClient, reverseClient)) {
            assertEquals(client.name + ": expected one snapshot message", 1, client.pendingMessageCount());
            final BarrageMessage message = client.commandQueue.peek();
            assertTrue(client.name + ": expected a snapshot", message.isSnapshot);
            assertEquals(client.name + ": expected every viewport row", 2L * budgetRows, message.rowsIncluded.size());
        }

        nugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        nugget.validate("viewports satisfied in one round");
    }

    /**
     * Only viewport subscriptions may exceed a snapshot round's budget. When a viewport takes more rows than the
     * round's budget, a full subscription growing in the same round receives only the rows the viewport shares with the
     * full subscription, and the full subscription's other rows wait for later rounds.
     */
    @Test
    public void testViewportLargerThanBudgetLeavesFullSubscriptionsNothing() {
        final int budgetRows = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;
        final int size = budgetRows * 4;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient fullClient = nugget.newClient(null, allCols, "full");
        final RemoteClient viewportClient = nugget.newClient(
                RowSetFactory.fromRange(budgetRows, 3 * budgetRows - 1), allCols, "viewport");

        flushProducerTable();
        assertEquals("expected the viewport in one snapshot message", 1, viewportClient.pendingMessageCount());
        final BarrageMessage fullFirst = fullClient.commandQueue.peek();
        assertNotNull("expected a message for the full subscription", fullFirst);
        assertTrue("expected the full subscription's first message to be a snapshot", fullFirst.isSnapshot);
        try (final RowSet viewportRows = RowSetFactory.fromRange(budgetRows, 3 * budgetRows - 1)) {
            assertEquals("expected only the rows the viewport shares", viewportRows, fullFirst.rowsIncluded);
        }

        nugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(fullClient.barrageTable.getServerViewport());
        nugget.validate("full subscription after a viewport larger than the budget");
    }

    /**
     * A full subscription growing in the same round as a viewport smaller than the round's budget receives the rows the
     * viewport shares plus the budget the viewport leaves. With a viewport of half the first round's budget at the
     * start of the table, the full subscription's first snapshot holds exactly the first round's budget of rows.
     */
    @Test
    public void testFullSubscriptionsGetTheBudgetViewportsLeave() {
        final int budgetRows = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;
        final int size = budgetRows * 4;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient fullClient = nugget.newClient(null, allCols, "full");
        nugget.newClient(RowSetFactory.fromRange(0, budgetRows / 2 - 1), allCols, "viewport");

        flushProducerTable();
        final BarrageMessage fullFirst = fullClient.commandQueue.peek();
        assertNotNull("expected a message for the full subscription", fullFirst);
        assertTrue("expected the full subscription's first message to be a snapshot", fullFirst.isSnapshot);
        try (final RowSet firstRound = RowSetFactory.flat(budgetRows)) {
            assertEquals("expected the viewport's rows plus the budget the viewport leaves", firstRound,
                    fullFirst.rowsIncluded);
        }

        nugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(fullClient.barrageTable.getServerViewport());
        nugget.validate("full subscription sharing a round with a small viewport");
    }

    /**
     * A full subscription completes however fast the table grows. After each round is planned and before the round's
     * snapshot, the one-column table grows by twice the smallest budget any round can have,
     * {@link BarrageUtil#MIN_SNAPSHOT_CELL_COUNT} rows. At the first snapshot, the subscription's positions past the
     * end of the table join the subscription's viewport, so the rows added later arrive as updates, and only the rows
     * the table held at the first snapshot remain to be snapshotted, at least one budget per round. A hook fails the
     * test on a snapshot past that bound rather than letting a regression grow the subscription forever.
     */
    @Test
    public void testFullSubscriptionCompletesWhenGrowthExceedsBudget() {
        final int budgetRows = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;
        final int growthRows = budgetRows * 2;
        final int initialSize = budgetRows * 2;
        final int maxRounds = (initialSize + growthRows) / budgetRows;
        final int capacity = initialSize + growthRows * maxRounds;
        final int[] values = new int[capacity];
        for (int ii = 0; ii < capacity; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(initialSize).toTracking(),
                TableTools.intCol("intCol", Arrays.copyOf(values, initialSize)));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final MutableInt rounds = new MutableInt(0);
        final MutableInt tableSize = new MutableInt(initialSize);
        nugget.barrageMessageProducer.setOnGetSnapshot(() -> {
            if (rounds.incrementAndGet() > maxRounds) {
                // an Error, so that the propagation job's catch of Exception cannot turn it into a subscription failure
                throw new AssertionError("expected the full subscription to complete within " + maxRounds + " rounds");
            }
            // grow the table after the round is planned and before the round's snapshot
            final int firstKey = tableSize.get();
            tableSize.add(growthRows);
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet added = RowSetFactory.fromRange(firstKey, firstKey + growthRows - 1);
                TstUtils.addToTable(sourceTable, added,
                        TableTools.intCol("intCol", Arrays.copyOfRange(values, firstKey, firstKey + growthRows)));
                sourceTable.notifyListeners(new TableUpdateImpl(added, RowSetFactory.empty(), RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });
        }, true);
        final RemoteClient client = nugget.newClient(null, allCols, "full");

        flushProducerTable();
        nugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(client.barrageTable.getServerViewport());
        nugget.validate("full subscription on a table growing faster than the budget");
    }

    /**
     * A reverse full subscription completes however the table changes, and holds the table's values when complete. A
     * reverse subscription counts positions from the end of the table, so every appended row shifts every row the
     * subscription holds to a higher position, and rows the subscription holds cross into the remaining positions while
     * rows the subscription lacks cross into the subscription's viewport. Rows inserted and removed in the middle of
     * the table move rows across the same boundary.
     *
     * <p>
     * The one-column table starts with twice the first round's budget of {@link BarrageUtil#MIN_SNAPSHOT_CELL_COUNT}
     * rows, keyed ten apart so that rows can be inserted between them. After each round is planned and before the
     * round's snapshot, the table gains twice that budget of rows at the end, gains a sixteenth of the budget in the
     * middle, and loses a sixteenth of the budget in the middle. The first round snapshots at most half the table, so
     * the table changes between the subscription's first and last snapshots. Only the positions the table held at the
     * first snapshot remain to be snapshotted, at least one budget per round, and a hook fails the test on a snapshot
     * past that bound.
     */
    @Test
    public void testReverseFullSubscriptionCompletesAsRowsCrossTheBoundary() {
        final int budgetRows = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;
        final int appendRows = budgetRows * 2;
        final int middleRows = budgetRows / 16;
        final int initialSize = budgetRows * 2;
        final int maxRounds = 1 + (initialSize + appendRows + middleRows + budgetRows - 1) / budgetRows;
        final long keySpacing = 10;

        final RowSetBuilderSequential initialKeys = RowSetFactory.builderSequential();
        final int[] initialValues = new int[initialSize];
        for (int ii = 0; ii < initialSize; ++ii) {
            initialKeys.appendKey(ii * keySpacing);
            initialValues[ii] = (int) (ii * keySpacing);
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                initialKeys.build().toTracking(), TableTools.intCol("intCol", initialValues));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final MutableInt rounds = new MutableInt(0);
        final MutableInt nextAppendIndex = new MutableInt(initialSize);
        nugget.barrageMessageProducer.setOnGetSnapshot(() -> {
            if (rounds.incrementAndGet() > maxRounds) {
                // an Error, so that the propagation job's catch of Exception cannot turn it into a subscription failure
                throw new AssertionError(
                        "expected the reverse full subscription to complete within " + maxRounds + " rounds");
            }
            // change the table after the round is planned and before the round's snapshot
            final int round = rounds.get() - 1;
            // remove rows from the first half and insert rows between rows of the second half
            final int firstRemoved = initialSize / 4 + round * middleRows;
            final int firstInserted = initialSize / 2 + round * middleRows;
            final int firstAppended = nextAppendIndex.get();
            nextAppendIndex.add(appendRows);

            final RowSetBuilderSequential removedKeys = RowSetFactory.builderSequential();
            for (int ii = 0; ii < middleRows; ++ii) {
                removedKeys.appendKey((firstRemoved + ii) * keySpacing);
            }
            // the inserted keys all precede the appended keys, so the keys and values are in key order
            final RowSetBuilderSequential addedKeys = RowSetFactory.builderSequential();
            final int[] addedValues = new int[middleRows + appendRows];
            for (int ii = 0; ii < middleRows; ++ii) {
                final long key = (firstInserted + ii) * keySpacing + keySpacing / 2;
                addedKeys.appendKey(key);
                addedValues[ii] = (int) key;
            }
            for (int ii = 0; ii < appendRows; ++ii) {
                final long key = (firstAppended + ii) * keySpacing;
                addedKeys.appendKey(key);
                addedValues[middleRows + ii] = (int) key;
            }
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet removed = removedKeys.build();
                final RowSet added = addedKeys.build();
                TstUtils.removeRows(sourceTable, removed);
                TstUtils.addToTable(sourceTable, added, TableTools.intCol("intCol", addedValues));
                sourceTable.notifyListeners(new TableUpdateImpl(added, removed, RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });
        }, true);
        final RemoteClient client = nugget.newClient(null, allCols, true, "reverse-full");

        flushProducerTable();
        nugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        // The first round's budget covers half the table, so the subscription takes a second round, and the table
        // changes between the two snapshots. Later rounds may have larger budgets, so no more rounds are certain.
        assertTrue("expected the subscription to take more than one round, took " + rounds.get(), rounds.get() >= 2);
        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(client.barrageTable.getServerViewport());
        nugget.validate("reverse full subscription as rows cross the boundary");
    }

    /**
     * The full subscription nearest completion grows first, whatever the order the producer holds the subscriptions in.
     * A full subscription that started later needs every row an earlier full subscription still needs, so growing the
     * earlier subscription first serves both. Growing the later subscription first spends the budget on rows the
     * earlier subscription already holds.
     *
     * <p>
     * Removing a subscription moves the newest subscription into the removed subscription's place, ahead of older
     * subscriptions. A viewport subscription, an early full subscription and a late full subscription start in that
     * order, on a one-column table of three times the first round's budget of
     * {@link BarrageUtil#MIN_SNAPSHOT_CELL_COUNT} rows. The late subscription joins during the first round, and the
     * viewport subscription is removed during the second round, which puts the late subscription ahead of the early
     * subscription for the third round. The early subscription must gain rows in every round until the early
     * subscription holds every row.
     */
    @Test
    public void testNearestCompleteFullSubscriptionGrowsFirst() {
        final int size = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT * 3;
        final int[] values = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(size).toTracking(), TableTools.intCol("intCol", values));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient viewportClient = nugget.newClient(RowSetFactory.flat(1), allCols, "viewport");
        final RemoteClient earlyClient = nugget.newClient(null, allCols, "early-full");

        // the number of rows the early subscription holds before each round
        final List<Long> earlyRowsBeforeRound = new ArrayList<>();
        final MutableObject<RemoteClient> lateClientHolder = new MutableObject<>();
        final Runnable recordEarlyRows = () -> {
            try (final WritableRowSet earlyRows = RowSetFactory.empty()) {
                earlyClient.commandQueue.forEach(message -> earlyRows.insert(message.rowsIncluded));
                earlyRowsBeforeRound.add(earlyRows.size());
            }
        };
        nugget.barrageMessageProducer.setOnGetSnapshot(() -> {
            recordEarlyRows.run();
            if (earlyRowsBeforeRound.size() == 1) {
                lateClientHolder.setValue(nugget.newClient(null, allCols, "late-full"));
            } else if (earlyRowsBeforeRound.size() == 2) {
                nugget.barrageMessageProducer.removeSubscription(viewportClient.dummyObserver);
            }
        }, true);

        flushProducerTable();
        assertNotNull("expected the late subscription to join during the first round", lateClientHolder.getValue());
        assertTrue("expected the viewport subscription to be removed", viewportClient.dummyObserver.completed);
        // and after the last round
        recordEarlyRows.run();

        for (int round = 1; round < earlyRowsBeforeRound.size(); ++round) {
            final long before = earlyRowsBeforeRound.get(round - 1);
            if (before < size) {
                assertTrue("expected the early subscription to gain rows in round " + round + ", rows before each "
                        + "round: " + earlyRowsBeforeRound, earlyRowsBeforeRound.get(round) > before);
            }
        }

        nugget.flushClientEvents();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(earlyClient.barrageTable.getServerViewport());
        assertNull(lateClientHolder.getValue().barrageTable.getServerViewport());
        nugget.validate("full subscriptions grown nearest completion first");
    }

    /**
     * On a table that grows during every round, a full subscription completes on the subscription's own rows, without
     * waiting for a full subscription that joined later. Completion once required the rows a round reserves to reach
     * past the table's end, which a round allowed only after every full subscription had every row, so every full
     * subscription waited for the one furthest behind.
     *
     * <p>
     * An early full subscription starts on a one-column table of twice the first round's budget of
     * {@link BarrageUtil#MIN_SNAPSHOT_CELL_COUNT} rows, and a late full subscription joins during the first round.
     * After each round is planned and before the round's snapshot, the table grows by a tenth of that budget. The early
     * subscription grows first and gains at least one budget of rows each round, so the early subscription completes as
     * soon as the early subscription holds the rows the table held at the first snapshot, however far behind the late
     * subscription is.
     */
    @Test
    public void testFullSubscriptionCompletesWithoutWaitingForLaterOnes() {
        final int budgetRows = (int) BarrageUtil.MIN_SNAPSHOT_CELL_COUNT;
        final int growthRows = budgetRows / 10;
        final int initialSize = budgetRows * 2;
        final int maxEarlyRounds = 1 + (initialSize + growthRows - 1) / budgetRows;
        final int maxRounds = maxEarlyRounds * 2;
        final int capacity = initialSize + growthRows * maxRounds;
        final int[] values = new int[capacity];
        for (int ii = 0; ii < capacity; ++ii) {
            values[ii] = ii;
        }
        final QueryTable sourceTable = TstUtils.testRefreshingTable(
                RowSetFactory.flat(initialSize).toTracking(),
                TableTools.intCol("intCol", Arrays.copyOf(values, initialSize)));

        final BitSet allCols = new BitSet();
        allCols.set(0, sourceTable.numColumns());

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        final RemoteClient earlyClient = nugget.newClient(null, allCols, "early-full");

        // a full subscription's snapshot without a viewport is the server's acknowledgement that the subscription is
        // complete
        final MutableInt rounds = new MutableInt(0);
        final MutableInt earlyCompletedRound = new MutableInt(0);
        final Runnable recordEarlyCompletion = () -> {
            if (earlyCompletedRound.get() == 0 && earlyClient.commandQueue.stream()
                    .anyMatch(message -> message.isSnapshot && message.snapshotRowSet == null)) {
                earlyCompletedRound.set(rounds.get());
            }
        };
        final MutableObject<RemoteClient> lateClientHolder = new MutableObject<>();
        final MutableInt tableSize = new MutableInt(initialSize);
        nugget.barrageMessageProducer.setOnGetSnapshot(() -> {
            recordEarlyCompletion.run();
            if (rounds.incrementAndGet() > maxRounds) {
                // an Error, so that the propagation job's catch of Exception cannot turn it into a subscription failure
                throw new AssertionError(
                        "expected both full subscriptions to complete within " + maxRounds + " rounds");
            }
            if (rounds.get() == 1) {
                lateClientHolder.setValue(nugget.newClient(null, allCols, "late-full"));
            }
            // grow the table after the round is planned and before the round's snapshot
            final int firstKey = tableSize.get();
            tableSize.add(growthRows);
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet added = RowSetFactory.fromRange(firstKey, firstKey + growthRows - 1);
                TstUtils.addToTable(sourceTable, added,
                        TableTools.intCol("intCol", Arrays.copyOfRange(values, firstKey, firstKey + growthRows)));
                sourceTable.notifyListeners(new TableUpdateImpl(added, RowSetFactory.empty(), RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });
        }, true);

        flushProducerTable();
        // and after the last round
        recordEarlyCompletion.run();

        assertNotNull("expected the late subscription to join during the first round", lateClientHolder.getValue());
        assertTrue("expected the early subscription to complete within " + maxEarlyRounds + " rounds, completed in "
                + "round " + earlyCompletedRound.get(),
                earlyCompletedRound.get() > 0 && earlyCompletedRound.get() <= maxEarlyRounds);

        nugget.flushClientEvents();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        // a null server viewport is how the server acknowledges that a full subscription is satisfied
        assertNull(earlyClient.barrageTable.getServerViewport());
        assertNull(lateClientHolder.getValue().barrageTable.getServerViewport());
        nugget.validate("full subscriptions on a table growing every round");
    }
}
