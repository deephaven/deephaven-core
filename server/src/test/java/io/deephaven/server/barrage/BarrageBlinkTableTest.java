//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import com.google.protobuf.ByteString;
import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.client.impl.BarrageSubscriptionImpl.BarrageDataMarshaller;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageStreamGenerator;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.BarrageStreamReader;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.annotations.ReferentialIntegrity;
import junit.framework.TestCase;
import org.apache.arrow.flatbuf.Schema;
import org.junit.experimental.categories.Category;

import javax.inject.Singleton;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

@Category(OutOfBandTest.class)
public class BarrageBlinkTableTest extends RefreshingTableTestCase {
    private static final long UPDATE_INTERVAL = 1000; // arbitrary; we enforce coalescing on both sides

    private TestControlledScheduler scheduler;
    private Deque<Throwable> exceptions;
    private SourceCombiner updateSourceCombiner;
    private boolean useDeephavenNulls;

    private static final long NUM_ROWS = 1024;
    private static final long BATCH_SIZE = 16;
    private QueryTable sourceTable;
    private TrackingWritableRowSet blinkRowSet;
    private QueryTable blinkTable;
    private BarrageMessageProducer barrageMessageProducer;
    private TableUpdateValidator originalTUV;
    private FailureListener originalTUVListener;

    @Singleton
    @Component(modules = {
            ArrowModule.class
    })
    public interface TestComponent {
        BarrageStreamGenerator.Factory getStreamGeneratorFactory();

        @Component.Builder
        interface Builder {
            @BindsInstance
            Builder withScheduler(final Scheduler scheduler);

            TestComponent build();
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        updateSourceCombiner = new SourceCombiner();
        scheduler = new TestControlledScheduler();
        exceptions = new ArrayDeque<>();
        useDeephavenNulls = true;

        final TestComponent daggerRoot = DaggerBarrageBlinkTableTest_TestComponent.builder()
                .withScheduler(scheduler)
                .build();

        sourceTable = (QueryTable) TableTools.emptyTable(NUM_ROWS).view("I = ii");
        blinkRowSet = RowSetFactory.empty().toTracking();
        blinkTable = sourceTable.getSubTable(blinkRowSet);
        blinkTable.setRefreshing(true);
        blinkTable.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);

        barrageMessageProducer = blinkTable.getResult(new BarrageMessageProducer.Operation(
                scheduler, new SessionService.ObfuscatingErrorTransformer(), daggerRoot.getStreamGeneratorFactory(),
                blinkTable, UPDATE_INTERVAL, () -> {
                }));

        originalTUV = TableUpdateValidator.make(blinkTable);
        originalTUVListener = new FailureListener("Original Table Update Validator");
        originalTUV.getResultTable().addUpdateListener(originalTUVListener);
    }

    @Override
    public void tearDown() throws Exception {
        updateSourceCombiner = null;
        scheduler = null;
        exceptions = null;
        sourceTable = null;
        blinkRowSet = null;
        blinkTable = null;
        barrageMessageProducer = null;
        originalTUV = null;
        originalTUVListener = null;
        super.tearDown();
    }

    private void flushProducerTable() {
        scheduler.runUntilQueueEmpty();
    }

    // We should listen for failures on the table, and if we get any, the test case is no good.
    class FailureListener extends InstrumentedTableUpdateListener {
        final String tableName;

        FailureListener(String tableName) {
            super("Failure Listener");
            this.tableName = tableName;
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Incremental Table Update: (" + tableName + ")");
                System.out.println(upstream);
            }
        }

        @Override
        public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
            exceptions.add(originalException);
            final StringWriter errors = new StringWriter();
            originalException.printStackTrace(new PrintWriter(errors));
            TestCase.fail(errors.toString());
        }
    }

    private class RemoteClient {
        private final RowSet viewport;
        private final boolean reverseViewport;

        private final BitSet subscribedColumns;

        private final BarrageTable barrageTable;

        @ReferentialIntegrity
        private final TableUpdateValidator replicatedTUV;
        @ReferentialIntegrity
        private final FailureListener replicatedTUVListener;

        private final Queue<BarrageMessage> commandQueue = new ArrayDeque<>();

        RemoteClient() {
            this(null, null);
        }

        RemoteClient(final RowSet viewport, final BitSet subscribedColumns) {
            this.viewport = viewport;
            this.reverseViewport = false;
            this.subscribedColumns = subscribedColumns;

            final ByteString schemaBytes = BarrageUtil.schemaBytesFromTable(blinkTable);
            final Schema flatbufSchema = SchemaHelper.flatbufSchema(schemaBytes.asReadOnlyByteBuffer());
            final BarrageUtil.ConvertedArrowSchema schema = BarrageUtil.convertArrowSchema(flatbufSchema);
            this.barrageTable = BarrageTable.make(updateSourceCombiner, ExecutionContext.getContext().getUpdateGraph(),
                    null, schema.tableDef, schema.attributes, viewport == null, null);
            this.barrageTable.addSourceToRegistrar();

            final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                    .useDeephavenNulls(useDeephavenNulls)
                    .build();
            final BarrageDataMarshaller marshaller = new BarrageDataMarshaller(
                    options, schema.computeWireChunkTypes(), schema.computeWireTypes(),
                    schema.computeWireComponentTypes(),
                    new BarrageStreamReader(barrageTable.getDeserializationTmConsumer()));
            BarrageMessageRoundTripTest.DummyObserver dummyObserver =
                    new BarrageMessageRoundTripTest.DummyObserver(marshaller, commandQueue);

            if (viewport == null) {
                replicatedTUV = TableUpdateValidator.make(barrageTable);
                replicatedTUVListener = new FailureListener("Replicated Table Update Validator");
                replicatedTUV.getResultTable().addUpdateListener(replicatedTUVListener);
            } else {
                // the TUV is unaware of the viewport and gets confused about which data should be valid.
                // instead we rely on the validation of the content in the viewport between the consumer and expected
                // table.
                replicatedTUV = null;
                replicatedTUVListener = null;
            }

            barrageMessageProducer.addSubscription(dummyObserver, options, subscribedColumns,
                    viewport == null ? null : viewport.copy(), reverseViewport);
        }

        public void validateBatches(long startBatchIncl, long endBatchExcl) {
            WritableRowSet rowSet = endBatchExcl > startBatchIncl
                    ? RowSetFactory.fromRange(startBatchIncl * BATCH_SIZE, endBatchExcl * BATCH_SIZE - 1)
                    : RowSetFactory.empty();
            validate(sourceTable.getSubTable(rowSet.toTracking()));
        }

        private void validate(QueryTable expected) {
            // We expect all messages from original table to have been propagated to the replicated table at this point.

            QueryTable toCheck = barrageTable;
            if (viewport != null) {
                expected = expected
                        .getSubTable(expected.getRowSet().subSetForPositions(viewport, reverseViewport).toTracking());
                toCheck = toCheck
                        .getSubTable(toCheck.getRowSet().subSetForPositions(viewport, reverseViewport).toTracking());
            }
            if (subscribedColumns != null && subscribedColumns.cardinality() != expected.numColumns()) {
                final List<Selectable> columns = new ArrayList<>();
                for (int i = subscribedColumns.nextSetBit(0); i >= 0; i = subscribedColumns.nextSetBit(i + 1)) {
                    columns.add(ColumnName.of(expected.getDefinition().getColumns().get(i).getName()));
                }
                expected = (QueryTable) expected.view(columns);
                toCheck = (QueryTable) toCheck.view(columns);
            }

            // Data should be identical and in-order.
            TstUtils.assertTableEquals(expected, toCheck);
        }

        private void showResult(final Table table) {
            System.out.println("Source Table:");
            TableTools.showWithRowSet(table, 100);
        }

        public void show(QueryTable expected) {
            QueryTable toCheck = barrageTable;
            if (viewport != null) {
                expected = expected.getSubTable(expected.getRowSet().subSetForPositions(viewport).toTracking());
                toCheck = toCheck.getSubTable(toCheck.getRowSet().subSetForPositions(viewport).toTracking());
            }
            if (subscribedColumns.cardinality() != expected.numColumns()) {
                final List<Selectable> columns = new ArrayList<>();
                for (int i = subscribedColumns.nextSetBit(0); i >= 0; i = subscribedColumns.nextSetBit(i + 1)) {
                    columns.add(ColumnName.of(expected.getDefinition().getColumns().get(i).getName()));
                }
                expected = (QueryTable) expected.view(columns);
                toCheck = (QueryTable) toCheck.view(columns);
            }

            final int maxLines = 100;
            final Pair<String, Long> diffPair =
                    TableTools.diffPair(toCheck, expected, maxLines, EnumSet.of(TableDiff.DiffItems.DoublesExact));

            if (diffPair.getFirst().equals("")) {
                showResult(toCheck);
            } else {
                final long numTableRows = Math.min(maxLines, Math.max(toCheck.size(), expected.size()));
                final long firstRow = Math.max(0, diffPair.getSecond() - 5);
                final long lastRow =
                        Math.min(firstRow + numTableRows, Math.min(firstRow + maxLines, diffPair.getSecond() + 5));

                System.out.println("Recomputed Table Differs:\n" + diffPair.getFirst()
                        + "\nRecomputed Table Rows [" + firstRow + ", " + lastRow + "]:");
                TableTools.showWithRowSet(expected, firstRow, lastRow + 1);
                System.out.println("Replicated Table Rows [" + firstRow + ", " + lastRow + "]:");
                TableTools.showWithRowSet(toCheck, firstRow, lastRow + 1);
            }
        }

        public void flushEventsToReplicatedTable() {
            for (final BarrageMessage msg : commandQueue) {
                barrageTable.handleBarrageMessage(msg);
                msg.close();
            }
            commandQueue.clear();
        }
    }

    private void releaseBlinkRows(int numBatches) {
        for (int ii = 0; ii < numBatches; ++ii) {
            releaseBlinkRows();
        }
    }

    private void releaseBlinkRows() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            final long lastKey = blinkRowSet.lastRowKey();
            update.removed = blinkRowSet.copy();
            blinkRowSet.clear();
            blinkRowSet.insertRange(lastKey + 1, lastKey + BATCH_SIZE);
            update.added = blinkRowSet.copy();
            update.modified = RowSetFactory.empty();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.shifted = RowSetShiftData.EMPTY;
            blinkTable.notifyListeners(update);
        });
    }

    public void testBasicBlinkSingleUpdates() {
        final RemoteClient client = new RemoteClient();
        flushProducerTable(); // empty snapshot
        client.flushEventsToReplicatedTable();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client.validateBatches(0, 0);

        for (long nr = 0; nr < NUM_ROWS / BATCH_SIZE; ++nr) {
            releaseBlinkRows();
            flushProducerTable();
            client.flushEventsToReplicatedTable();
            updateSourceCombiner.assertRefreshRequested();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            client.validateBatches(nr, nr + 1);
        }
    }

    public void testSenderAggregates() {
        final RemoteClient client = new RemoteClient();
        flushProducerTable(); // empty snapshot
        client.flushEventsToReplicatedTable();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client.validateBatches(0, 0);

        for (long nr = 0; nr < NUM_ROWS / BATCH_SIZE / 4; ++nr) {
            releaseBlinkRows(4);
            flushProducerTable();
            client.flushEventsToReplicatedTable();
            updateSourceCombiner.assertRefreshRequested();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            client.validateBatches(4 * nr, 4 * (nr + 1));
        }
    }

    public void testReceiverAggregates() {
        final RemoteClient client = new RemoteClient();
        flushProducerTable(); // empty snapshot
        client.flushEventsToReplicatedTable();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client.validateBatches(0, 0);

        for (long nr = 0; nr < NUM_ROWS / BATCH_SIZE / 4; ++nr) {
            releaseBlinkRows(4);
            flushProducerTable();
            client.flushEventsToReplicatedTable();
            updateSourceCombiner.assertRefreshRequested();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            client.validateBatches(4 * nr, 4 * (nr + 1));
        }
    }

    public void testBMPFlushesOnSub() {
        barrageMessageProducer.setOnGetSnapshot(this::releaseBlinkRows, false);

        final RemoteClient client1 = new RemoteClient();
        flushProducerTable(); // empty snapshot + release of BATCH_SIZE
        client1.flushEventsToReplicatedTable();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client1.validateBatches(0, 1);

        releaseBlinkRows(); // get sent to client1
        final RemoteClient client2 = new RemoteClient();
        flushProducerTable(); // empty snapshot + release of BATCH_SIZE
        client1.flushEventsToReplicatedTable();
        client2.flushEventsToReplicatedTable();

        updateSourceCombiner.assertRefreshRequested();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

        client1.validateBatches(1, 3); // gets before and after snap
        client2.validateBatches(2, 3); // gets only after snap
    }

    public void testReceiverFlushesEmptyCycle() {
        final RemoteClient client = new RemoteClient();
        flushProducerTable(); // empty snapshot
        client.flushEventsToReplicatedTable();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        updateSourceCombiner.assertRefreshRequested();

        releaseBlinkRows();
        flushProducerTable();
        Assert.eqFalse(updateSourceCombiner.refreshRequested, "refreshRequested");
        client.flushEventsToReplicatedTable();
        updateSourceCombiner.assertRefreshRequested();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client.validateBatches(0, 1);

        updateSourceCombiner.assertRefreshRequested();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client.validateBatches(0, 0);
    }

    public void testViewport() {
        // viewports on blink tables are a little silly; but let's ensure we get the expected behavior
        final RemoteClient client = new RemoteClient(RowSetFactory.fromRange(2 * BATCH_SIZE, 3 * BATCH_SIZE - 1), null);
        flushProducerTable(); // empty snapshot
        client.flushEventsToReplicatedTable();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client.validateBatches(0, 0);

        releaseBlinkRows();
        flushProducerTable();
        client.flushEventsToReplicatedTable();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client.validateBatches(0, 0);

        releaseBlinkRows(3);
        flushProducerTable();
        client.flushEventsToReplicatedTable();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client.validateBatches(3, 4);

        for (long nr = 1; nr < NUM_ROWS / BATCH_SIZE / 4; ++nr) {
            releaseBlinkRows(4);
            flushProducerTable();
            client.flushEventsToReplicatedTable();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            client.validateBatches(4 * nr + 2, 4 * (nr + 1) - 1);
        }
    }

    public void testSimultaneousFullAndViewport() {
        final RemoteClient client1 = new RemoteClient();
        final RemoteClient client2 =
                new RemoteClient(RowSetFactory.fromRange(2 * BATCH_SIZE, 3 * BATCH_SIZE - 1), null);
        flushProducerTable(); // empty snapshot
        client1.flushEventsToReplicatedTable();
        client2.flushEventsToReplicatedTable();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        client1.validate(sourceTable.getSubTable(RowSetFactory.empty().toTracking()));
        client2.validate(sourceTable.getSubTable(RowSetFactory.empty().toTracking()));

        for (long nr = 0; nr < NUM_ROWS / BATCH_SIZE / 4; ++nr) {
            releaseBlinkRows(4);
            flushProducerTable();
            client1.flushEventsToReplicatedTable();
            client2.flushEventsToReplicatedTable();
            updateSourceCombiner.assertRefreshRequested();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            client1.validateBatches(4 * nr, 4 * (nr + 1));
            client2.validateBatches(4 * nr + 2, 4 * (nr + 1) - 1);
        }
    }

    private static class SourceCombiner extends UpdateSourceCombiner {
        private boolean refreshRequested = false;

        private SourceCombiner() {
            super(ExecutionContext.getContext().getUpdateGraph());
        }

        @Override
        public void requestRefresh() {
            refreshRequested = true;
        }

        public void assertRefreshRequested() {
            Assert.eqTrue(refreshRequested, "refreshRequested");
            refreshRequested = false;
        }
    }
}
