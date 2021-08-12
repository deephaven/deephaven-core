/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.table;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.LiveTableRefreshCombiner;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableDiff;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.EvalNuggetInterface;
import io.deephaven.db.v2.GenerateTableUpdates;
import io.deephaven.db.v2.InstrumentedShiftAwareListener;
import io.deephaven.db.v2.LiveTableTestCase;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.TableUpdateValidator;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.grpc_api.arrow.ArrowModule;
import io.deephaven.grpc_api.arrow.FlightServiceGrpcBinding;
import io.deephaven.grpc_api.barrage.BarrageMessageConsumer;
import io.deephaven.grpc_api.barrage.BarrageMessageProducer;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
import io.deephaven.grpc_api.barrage.BarrageStreamReader;
import io.deephaven.grpc_api.util.Scheduler;
import io.deephaven.grpc_api.util.TestControlledScheduler;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.table.BarrageTable;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.grpc.Drainable;
import io.grpc.stub.StreamObserver;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.experimental.categories.Category;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.deephaven.db.v2.TstUtils.c;
import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.i;
import static io.deephaven.db.v2.TstUtils.initColumnInfos;

@Category(OutOfBandTest.class)
public class BarrageMessageRoundTripTest extends LiveTableTestCase {
    private static final long UPDATE_INTERVAL = 1000; // arbitrary; we enforce coalescing on both sides

    private Logger log;
    private TestControlledScheduler scheduler;
    private Deque<Throwable> exceptions;
    private LiveTableRefreshCombiner liveTableRegistrar;
    private boolean useDeephavenNulls;

    private TestComponent daggerRoot;

    @Singleton
    @Component(modules = {
            ArrowModule.class
    })
    public interface TestComponent {
        BarrageMessageProducer.StreamGenerator.Factory<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> getStreamGeneratorFactory();

        @Component.Builder
        interface Builder {
            @BindsInstance
            Builder withScheduler(final Scheduler scheduler);

            TestComponent build();
        }
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        log = new StreamLoggerImpl();
        liveTableRegistrar = new LiveTableRefreshCombiner();
        scheduler = new TestControlledScheduler();
        exceptions = new ArrayDeque<>();
        ChunkPoolReleaseTracking.enableStrict();
        useDeephavenNulls = true;

        daggerRoot = DaggerBarrageMessageRoundTripTest_TestComponent
                .builder()
                .withScheduler(scheduler)
                .build();
    }

    @Override
    protected void tearDown() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
        log = null;
        liveTableRegistrar = null;
        scheduler = null;
        exceptions = null;
        super.tearDown();
    }

    private void flushProducerTable() {
        scheduler.runUntilQueueEmpty();
    }

    // We should listen for failures on the table, and if we get any, the test case is no good.
    class FailureListener extends InstrumentedShiftAwareListener {
        final String tableName;

        FailureListener(String tableName) {
            super("Failure Listener");
            this.tableName = tableName;
        }

        @Override
        public void onUpdate(final Update upstream) {
            if (LiveTableTestCase.printTableUpdates) {
                System.out.println("Incremental Table Update: (" + tableName + ")");
                System.out.println(upstream);
            }
        }

        @Override
        public void onFailureInternal(Throwable originalException, UpdatePerformanceTracker.Entry sourceEntry) {
            exceptions.add(originalException);
            final StringWriter errors = new StringWriter();
            originalException.printStackTrace(new PrintWriter(errors));
            TestCase.fail(errors.toString());
        }
    }

    private class RemoteClient {
        private Index viewport;
        private BitSet subscribedColumns;

        private final String name;

        private final BarrageTable barrageTable;
        @ReferentialIntegrity
        private final BarrageMessageProducer<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> barrageMessageProducer;

        @ReferentialIntegrity
        private final TableUpdateValidator replicatedTUV;
        @ReferentialIntegrity
        private final FailureListener replicatedTUVListener;

        private boolean subscribed = false;
        private final Queue<BarrageMessage> commandQueue = new ArrayDeque<>();
        private final DummyObserver dummyObserver;

        // The replicated table's TableUpdateValidator will be confused if the table is a viewport. Instead we rely on
        // comparing the producer table to the consumer table to validate contents are correct.
        RemoteClient(final Index viewport, final BitSet subscribedColumns,
                     final BarrageMessageProducer<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> barrageMessageProducer,
                     final String name) {
            this(viewport, subscribedColumns, barrageMessageProducer, name, false);
        }

        RemoteClient(final Index viewport, final BitSet subscribedColumns,
                     final BarrageMessageProducer<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> barrageMessageProducer,
                     final String name, final boolean deferSubscription) {
            this.viewport = viewport;
            this.subscribedColumns = subscribedColumns;
            this.name = name;
            this.barrageMessageProducer = barrageMessageProducer;

            this.barrageTable = BarrageTable.make(liveTableRegistrar, LiveTableMonitor.DEFAULT, barrageMessageProducer.getTableDefinition(), viewport != null);

            final ChunkInputStreamGenerator.Options options = new ChunkInputStreamGenerator.Options.Builder()
                    .setIsViewport(viewport != null)
                    .setUseDeephavenNulls(useDeephavenNulls)
                    .build();
            final BarrageMarshaller marshaller = new BarrageMarshaller(
                    options, barrageTable.getWireChunkTypes(), barrageTable.getWireTypes(),
                    barrageTable.getWireComponentTypes(), new BarrageStreamReader());
            this.dummyObserver = new DummyObserver(marshaller, commandQueue);

            if (viewport == null) {
                replicatedTUV = TableUpdateValidator.make(barrageTable);
                replicatedTUVListener = new FailureListener("Replicated Table Update Validator");
                replicatedTUV.getResultTable().listenForUpdates(replicatedTUVListener);
            } else {
                // the TUV is unaware of the viewport and gets confused about which data should be valid.
                // instead we rely on the validation of the content in the viewport between the consumer and expected table.
                replicatedTUV = null;
                replicatedTUVListener = null;
            }

            if (!deferSubscription) {
                doSubscribe();
            }
        }

        public void doSubscribe() {
            subscribed = true;
            final ChunkInputStreamGenerator.Options options = new ChunkInputStreamGenerator.Options.Builder()
                    .setIsViewport(viewport != null)
                    .setUseDeephavenNulls(useDeephavenNulls)
                    .build();
            barrageMessageProducer.addSubscription(dummyObserver, options, subscribedColumns, viewport);
        }

        public void validate(final String msg, QueryTable expected) {
            if (!subscribed) {
                return; // no subscription implies no refresh implies no data -- so we're valid
            }

            // We expect all messages from original table to have been propagated to the replicated table at this point.

            QueryTable toCheck = barrageTable;
            if (viewport != null) {
                try (final Index view = expected.getIndex().subindexByPos(viewport)) {
                    expected = expected.getSubTable(view);
                }
                try (final Index view = toCheck.getIndex().subindexByPos(viewport)) {
                    toCheck = toCheck.getSubTable(view);
                }
            }
            if (subscribedColumns.cardinality() != expected.getColumns().length) {
                final List<Selectable> columns = new ArrayList<>();
                for (int i = subscribedColumns.nextSetBit(0); i >= 0; i = subscribedColumns.nextSetBit(i + 1)) {
                    columns.add(ColumnName.of(expected.getColumns()[i].getName()));
                }
                expected = (QueryTable) expected.view(columns);
                toCheck = (QueryTable) toCheck.view(columns);
            }

            // Data should be identical and in-order.
            TstUtils.assertTableEquals(expected, toCheck);
            // Since key-space needs to be kept the same, the indexes should also be identical between producer and consumer
            // (not the indexes between expected and consumer; as the consumer maintains the entire index).
            Assert.equals(barrageMessageProducer.getIndex(), "barrageMessageProducer.getIndex()", barrageTable.getIndex(), ".getIndex()");
        }

        private void showResult(final String label, final Table table) {
            System.out.println(label);
            TableTools.showWithIndex(table, 100);
        }

        public void show(QueryTable expected) {
            QueryTable toCheck = barrageTable;
            if (viewport != null) {
                try (final Index view = expected.getIndex().subindexByPos(viewport)) {
                    expected = expected.getSubTable(view);
                }
                try (final Index view = toCheck.getIndex().subindexByPos(viewport)) {
                    toCheck = toCheck.getSubTable(view);
                }
            }
            if (subscribedColumns.cardinality() != expected.getColumns().length) {
                final List<Selectable> columns = new ArrayList<>();
                for (int i = subscribedColumns.nextSetBit(0); i >= 0; i = subscribedColumns.nextSetBit(i + 1)) {
                    columns.add(ColumnName.of(expected.getColumns()[i].getName()));
                }
                expected = (QueryTable) expected.view(columns);
                toCheck = (QueryTable) toCheck.view(columns);
            }

            final int maxLines = 100;
            final Pair<String, Long> diffPair = TableTools.diffPair(toCheck, expected, maxLines, EnumSet.of(TableDiff.DiffItems.DoublesExact));

            if (diffPair.getFirst().equals("")) {
                showResult("Ticking Table (" + name + "):", toCheck);
            } else {
                final long numTableRows = Math.min(maxLines, Math.max(toCheck.size(), expected.size()));
                final long firstRow = Math.max(0, diffPair.getSecond() - 5);
                final long lastRow = Math.min(firstRow + numTableRows, Math.min(firstRow + maxLines, diffPair.getSecond() + 5));

                System.out.println("Recomputed Table (" + name + ") Differs:\n" + diffPair.getFirst() + "\nRecomputed Table Rows [" + firstRow + ", " + lastRow + "]:");
                TableTools.showWithIndex(expected, firstRow, lastRow + 1);
                System.out.println("Replicated Table Rows [" + firstRow + ", " + lastRow + "]:");
                TableTools.showWithIndex(toCheck, firstRow, lastRow + 1);
            }
        }

        public void flushEventsToReplicatedTable() {
            for (final BarrageMessage msg : commandQueue) {
                barrageTable.handleBarrageMessage(msg);
                msg.close();
            }
            commandQueue.clear();
        }

        public void setViewport(final Index newViewport) {
            viewport = newViewport;
            barrageMessageProducer.updateViewport(dummyObserver, viewport);
        }

        public void setSubscribedColumns(final BitSet newColumns) {
            subscribedColumns = newColumns;
            barrageMessageProducer.updateSubscription(dummyObserver, newColumns);
        }

        public void setViewportAndColumns(final Index newViewport, final BitSet newColumns) {
            viewport = newViewport;
            subscribedColumns = newColumns;
            barrageMessageProducer.updateViewportAndColumns(dummyObserver, viewport, subscribedColumns);
        }
    }

    private class RemoteNugget implements EvalNuggetInterface {

        private final Supplier<Table> makeTable;

        private final QueryTable originalTable;
        @ReferentialIntegrity
        private final BarrageMessageProducer<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> barrageMessageProducer;

        @ReferentialIntegrity
        private final TableUpdateValidator originalTUV;
        @ReferentialIntegrity
        private final FailureListener originalTUVListener;

        private final List<RemoteClient> clients = new ArrayList<>();

        RemoteNugget(final Supplier<Table> makeTable) {
            this.makeTable = makeTable;
            this.originalTable = (QueryTable) makeTable.get();
            this.barrageMessageProducer = originalTable.getResult(new BarrageMessageProducer.Operation<>(scheduler, daggerRoot.getStreamGeneratorFactory(), originalTable, UPDATE_INTERVAL, this::onGetSnapshot));

            originalTUV = TableUpdateValidator.make(originalTable);
            originalTUVListener = new FailureListener("Original Table Update Validator");
            originalTUV.getResultTable().listenForUpdates(originalTUVListener);
        }

        @Override
        public void validate(final String msg) {
            final QueryTable expected = (QueryTable) makeTable.get();
            for (final RemoteClient client : clients) {
                client.validate(msg, expected);
            }
        }

        @Override
        public void show() {
            final QueryTable expected = (QueryTable) makeTable.get();
            for (final RemoteClient client : clients) {
                client.show(expected);
            }
        }

        public void flushClientEvents() {
            for (final RemoteClient client : clients) {
                client.flushEventsToReplicatedTable();
            }
        }

        public RemoteClient newClient(final Index viewport, final BitSet subscribedColumns, final String name) {
            clients.add(new RemoteClient(viewport, subscribedColumns, barrageMessageProducer, name));
            return clients.get(clients.size() - 1);
        }

        public void onGetSnapshot() {
        }
    }

    private abstract class TestHelper {
        final int numProducerCoalesce;
        final int numConsumerCoalesce;

        final int size;
        final Random random;
        final MutableInt numSteps;

        final List<RemoteNugget> nuggets = new ArrayList<>();

        QueryTable sourceTable;
        TstUtils.ColumnInfo<?, ?>[] columnInfo;

        TestHelper(final int numProducerCoalesce, final int numConsumerCoalesce, final int size, final int seed, final MutableInt numSteps) {
            this.numProducerCoalesce = numProducerCoalesce;
            this.numConsumerCoalesce = numConsumerCoalesce;
            this.size = size;
            this.random = new Random(seed);
            this.numSteps = numSteps;
        }

        public void createTable() {
            sourceTable = getTable(size / 4, random, columnInfo = initColumnInfos(new String[]{"Sym", "intCol", "doubleCol", "Keys", "boolCol", "TimeStamp"},
                    new TstUtils.SetGenerator<>("a", "b", "c", "d"),
                    new TstUtils.IntGenerator(10, 100),
                    new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                    new TstUtils.SortedLongGenerator(0, Long.MAX_VALUE - 1),
                    new TstUtils.BooleanGenerator(0.2),
                    new TstUtils.UnsortedDateTimeGenerator(DBTimeUtils.convertDateTime("2020-02-14T00:00:00 NY"), DBTimeUtils.convertDateTime("2020-02-25T00:00:00 NY"))));
        }

        public void createNuggets() {
            createNuggetsForTableMaker(() -> sourceTable); // test the explicit updates
            createNuggetsForTableMaker(sourceTable::flatten); // test shift aggressive version of these updates
            createNuggetsForTableMaker(() -> sourceTable.sort("doubleCol")); // test updates in the middle of the keyspace
            createNuggetsForTableMaker(() -> sourceTable.where("intCol % 12 < 5")); // test sparse(r) updates
        }

        void runTest(final Runnable simulateSourceStep) {
            createTable();
            createNuggets();
            final int maxSteps = numSteps.getValue();
            final RemoteNugget[] nuggetsToValidate = nuggets.toArray(new RemoteNugget[0]);
            for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
                for (int rt = 0; rt < numConsumerCoalesce; ++rt) {
                    // coalesce updates in producer
                    for (int pt = 0; pt < numProducerCoalesce; ++pt) {
                        simulateSourceStep.run();
                    }

                    flushProducerTable();
                }

                // flush consumer
                for (final RemoteNugget nugget : nuggets) {
                    nugget.flushClientEvents();
                }
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(liveTableRegistrar::refresh);

                TstUtils.validate("", nuggetsToValidate);

                if (sourceTable.size() >= size) {
                    numSteps.setValue(maxSteps); // pretend we finished
                    return;
                }
            }
        }

        abstract void createNuggetsForTableMaker(final Supplier<Table> makeTable);
    }

    private class OneProducerPerClient extends TestHelper {
        OneProducerPerClient(final int numProducerCoalesce, final int numConsumerCoalesce, final int size, final int seed, final MutableInt numSteps) {
            super(numProducerCoalesce, numConsumerCoalesce, size, seed, numSteps);
        }

        void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
            nuggets.add(new RemoteNugget(makeTable));
            final BitSet subscribedColumns = new BitSet();
            subscribedColumns.set(0, nuggets.get(nuggets.size() - 1).originalTable.getColumns().length);
            nuggets.get(nuggets.size() - 1).newClient(null, subscribedColumns, "full");

            nuggets.add(new RemoteNugget(makeTable));
            nuggets.get(nuggets.size() - 1).newClient(Index.FACTORY.getIndexByRange(0, size / 10), subscribedColumns, "header");
            nuggets.add(new RemoteNugget(makeTable));
            nuggets.get(nuggets.size() - 1).newClient(Index.FACTORY.getIndexByRange(size / 2, size * 3 / 4), subscribedColumns, "floating");

            final Index.SequentialBuilder swissIndexBuilder = Index.FACTORY.getSequentialBuilder();
            final long rangeSize = Math.max(1, size / 20);
            for (long nr = 1; nr < 20; nr += 2) {
                swissIndexBuilder.appendRange(nr * rangeSize, (nr + 1) * rangeSize - 1);
            }
            try (final Index swissIndex = swissIndexBuilder.getIndex()) {
                final RemoteNugget nugget = new RemoteNugget(makeTable);
                nugget.newClient(swissIndex, subscribedColumns, "swiss viewport");
                nuggets.add(nugget);
            }
        }
    }

    private class SharedProducerForAllClients extends TestHelper {
        SharedProducerForAllClients(final int numProducerCoalesce, final int numConsumerCoalesce, final int size, final int seed, final MutableInt numSteps) {
            super(numProducerCoalesce, numConsumerCoalesce, size, seed, numSteps);
        }

        void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
            final RemoteNugget nugget = new RemoteNugget(makeTable);
            nuggets.add(nugget);

            final BitSet subscribedColumns = new BitSet();
            subscribedColumns.set(0, nugget.originalTable.getColumns().length);
            nugget.newClient(null, subscribedColumns, "full");

            nugget.newClient(Index.FACTORY.getIndexByRange(0, size / 10), subscribedColumns, "header");
            nugget.newClient(Index.FACTORY.getIndexByRange(size / 2, size * 3L / 4), subscribedColumns, "floating");

            final Index.SequentialBuilder swissIndexBuilder = Index.FACTORY.getSequentialBuilder();
            final long rangeSize = Math.max(1, size / 20);
            for (long nr = 1; nr < 20; nr += 2) {
                swissIndexBuilder.appendRange(nr * rangeSize, (nr + 1) * rangeSize - 1);
            }
            try (final Index swissIndex = swissIndexBuilder.getIndex()) {
                nugget.newClient(swissIndex, subscribedColumns, "swiss viewport");
            }
        }
    }

    public void testAppendIncremental() {
        final int MAX_STEPS = 100;
        final Consumer<TestHelper> runOne = helper -> {
            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16) + (helper.sourceTable.getIndex().nonempty() ? helper.sourceTable.getIndex().lastKey() : -1);
                final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
                update.added = Index.CURRENT_FACTORY.getIndexByRange(lastKey + 1, lastKey + Math.max(1, helper.size / maxSteps));
                update.removed = i();
                update.modified = i();
                update.shifted = IndexShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update, helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{1, 10}) {
                for (final int numConsumerCoalesce : new int[]{1, 10}) {
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    public void testPrependIncremental() {
        final int MAX_STEPS = 100;
        final Consumer<TestHelper> runOne = helper -> {
            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = helper.sourceTable.getIndex().nonempty() ? helper.sourceTable.getIndex().lastKey() : -1;
                final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = Index.CURRENT_FACTORY.getIndexByRange(0, stepSize - 1);
                update.removed = i();
                update.modified = i();
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final IndexShiftData.Builder shifted = new IndexShiftData.Builder();
                if (lastKey >= 0) {
                    shifted.shiftRange(0, lastKey, stepSize + (Math.abs(helper.random.nextLong()) % 16));
                }
                update.shifted = shifted.build();

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update, helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{1, 10}) {
                for (final int numConsumerCoalesce : new int[]{1, 10}) {
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    public void testRoundTripIncremental() {
        final Consumer<TestHelper> runOne = helper -> {
            helper.runTest(() -> LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() ->
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, helper.size, helper.random, helper.sourceTable, helper.columnInfo)));
        };

        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{1, 10}) {
                for (final int numConsumerCoalesce : new int[]{1, 10}) {
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(100)));
                }
            }
        }
    }

    public void testAppendIncrementalSharedProducer() {
        final int MAX_STEPS = 100;
        final Consumer<TestHelper> runOne = helper -> {
            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16) + (helper.sourceTable.getIndex().nonempty() ? helper.sourceTable.getIndex().lastKey() : -1);
                final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
                update.added = Index.CURRENT_FACTORY.getIndexByRange(lastKey + 1, lastKey + Math.max(1, helper.size / maxSteps));
                update.removed = i();
                update.modified = i();
                update.shifted = IndexShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update, helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{1, 2, 10}) {
                for (final int numConsumerCoalesce : new int[]{1, 2, 10}) {
                    runOne.accept(new SharedProducerForAllClients(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    public void testPrependIncrementalSharedProducer() {
        final int MAX_STEPS = 100;
        final Consumer<TestHelper> runOne = helper -> {
            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = helper.sourceTable.getIndex().nonempty() ? helper.sourceTable.getIndex().lastKey() : -1;
                final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = Index.CURRENT_FACTORY.getIndexByRange(0, stepSize - 1);
                update.removed = i();
                update.modified = i();
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final IndexShiftData.Builder shifted = new IndexShiftData.Builder();
                if (lastKey >= 0) {
                    shifted.shiftRange(0, lastKey, stepSize + (Math.abs(helper.random.nextLong()) % 16));
                }
                update.shifted = shifted.build();

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update, helper.random, helper.sourceTable, helper.columnInfo));
            });
        };

        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalsce : new int[]{1, 10}) {
                for (final int numConsumerCoalesce : new int[]{1, 10}) {
                    runOne.accept(new SharedProducerForAllClients(numProducerCoalsce, numConsumerCoalesce, size, 0, new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    public void testRoundTripIncrementalSharedProducer() {
        final Consumer<TestHelper> runOne = helper -> {
            helper.runTest(() -> LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() ->
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, helper.size, helper.random, helper.sourceTable, helper.columnInfo)));
        };

        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{1, 10}) {
                for (final int numConsumerCoalesce : new int[]{1, 10}) {
                    runOne.accept(new SharedProducerForAllClients(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(100)));
                }
            }
        }
    }

    // These test mid-cycle subscription changes and snapshot content

    private abstract class SubscriptionChangingHelper extends SharedProducerForAllClients {
        SubscriptionChangingHelper(final int numProducerCoalesce, final int numConsumerCoalesce, final int size, final int seed, final MutableInt numSteps) {
            super(numProducerCoalesce, numConsumerCoalesce, size, seed, numSteps);
        }

        void runTest() {
            createTable();
            createNuggets();
            final int maxSteps = numSteps.getValue();
            final RemoteNugget[] nuggetsToValidate = nuggets.toArray(new RemoteNugget[0]);
            for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
                for (int rt = 0; rt < numConsumerCoalesce; ++rt) {
                    // coalesce updates in producer
                    for (int pt = 0; pt < numProducerCoalesce; ++pt) {
                        maybeChangeSub(numSteps.intValue(), rt, pt);

                        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() ->
                                GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
                    }

                    // flush producer
                    flushProducerTable();
                }

                // flush consumer
                for (final RemoteNugget nugget : nuggets) {
                    nugget.flushClientEvents();
                }
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(liveTableRegistrar::refresh);

                TstUtils.validate("", nuggetsToValidate);
            }
        }

        abstract void maybeChangeSub(int step, int rt, int pt);
    }

    public void testColumnSubChange() {
        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{1, 4}) {
                for (final int numConsumerCoalesce : new int[]{1, 4}) {
                    for (int subProducerCoalesce = 0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce = 0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = subProducerCoalesce;
                            final int finalSubConsumerCoalesce = subConsumerCoalesce;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(4)) {
                                {
                                    for (final RemoteNugget nugget : nuggets) {
                                        final BitSet columns = new BitSet();
                                        columns.set(0, nugget.originalTable.getColumns().length / 2);
                                        nugget.clients.add(new RemoteClient(Index.FACTORY.getIndexByRange(size / 5, 2 * size / 5), columns, nugget.barrageMessageProducer, "sub-changer"));
                                    }
                                }

                                void maybeChangeSub(final int step, final int rt, final int pt) {
                                    if (step != 2 || rt != finalSubConsumerCoalesce || pt != finalSubProducerCoalesce) {
                                        return;
                                    }

                                    for (final RemoteNugget nugget : nuggets) {
                                        final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                        final BitSet columns = new BitSet();
                                        final int numColumns = nugget.originalTable.getColumns().length;
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
        for (final int size : new int[]{10, 100}) {
            for (final int numProducerCoalesce : new int[]{1, 4}) {
                for (final int numConsumerCoalesce : new int[]{1, 4}) {
                    for (int subProducerCoalesce = 0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce = 0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = 0;
                            final int finalSubConsumerCoalesce = 1;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(25)) {
                                @Override
                                void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
                                    final RemoteNugget nugget = new RemoteNugget(makeTable);
                                    nuggets.add(nugget);

                                    final BitSet columns = new BitSet();
                                    columns.set(0, 4);
                                    nugget.clients.add(new RemoteClient(Index.FACTORY.getIndexByRange(0, size / 5), columns, nugget.barrageMessageProducer, "sub-changer"));
                                }

                                void maybeChangeSub(final int step, final int rt, final int pt) {
                                    if (step % 2 != 0 || rt != finalSubConsumerCoalesce || pt != finalSubProducerCoalesce) {
                                        return;
                                    }

                                    for (final RemoteNugget nugget : nuggets) {
                                        final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                        final Index viewport = client.viewport.clone();
                                        viewport.shiftInPlace(Math.max(size / 25, 1));
                                        client.setViewport(viewport);
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
        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{1, 4}) {
                for (final int numConsumerCoalesce : new int[]{1, 4}) {
                    for (int subProducerCoalesce = 0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce = 0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = subProducerCoalesce;
                            final int finalSubConsumerCoalesce = subConsumerCoalesce;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(4)) {
                                {
                                    for (final RemoteNugget nugget : nuggets) {
                                        final BitSet columns = new BitSet();
                                        columns.set(0, 3);
                                        nugget.clients.add(new RemoteClient(Index.FACTORY.getIndexByRange(size / 5, 2 * size / 5), columns, nugget.barrageMessageProducer, "sub-changer"));
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
        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{2, 3, 4}) {
                for (final int numConsumerCoalesce : new int[]{1, 4}) {
                    new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(4)) {

                        void maybeChangeSub(final int step, final int rt, final int pt) {
                            if (step != 0 || rt != 0 || pt != 1) {
                                // Only subscribe after we have sent at least one update, but no need to subscribe again.
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
                                    LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() ->
                                            GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
                                }
                            };
                            nuggets.add(nugget);

                            // we can only have viewport subscriptions or else we won't tickle the original bug
                            final BitSet columns = new BitSet();
                            columns.set(0, 4);
                            final boolean deferSubscription = true;
                            nugget.clients.add(new RemoteClient(Index.FACTORY.getIndexByRange(size / 5, 2 * size / 5), columns, nugget.barrageMessageProducer, "sub-changer", deferSubscription));

                        }
                    }.runTest();
                }
            }
        }
    }

    public void testOverlappingViewportChange() {
        for (final int size : new int[]{10, 100, 1000}) {
            for (final int numProducerCoalesce : new int[]{1, 4}) {
                for (final int numConsumerCoalesce : new int[]{1, 4}) {
                    for (int subProducerCoalesce = 0; subProducerCoalesce < numProducerCoalesce; ++subProducerCoalesce) {
                        for (int subConsumerCoalesce = 0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                            final int finalSubProducerCoalesce = subProducerCoalesce;
                            final int finalSubConsumerCoalesce = subConsumerCoalesce;
                            new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(4)) {
                                @Override
                                public void createNuggets() {
                                    super.createNuggets();

                                    for (final RemoteNugget nugget : nuggets) {
                                        final BitSet columns = new BitSet();
                                        columns.set(0, 4);
                                        nugget.clients.add(new RemoteClient(Index.FACTORY.getIndexByRange(size / 5, 3 * size / 5), columns, nugget.barrageMessageProducer, "sub-changer"));
                                    }
                                }

                                void maybeChangeSub(final int step, final int rt, final int pt) {
                                    if (step != 2 || rt != finalSubConsumerCoalesce || pt != finalSubProducerCoalesce) {
                                        return;
                                    }

                                    for (final RemoteNugget nugget : nuggets) {
                                        final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                                        final Index viewport = client.viewport.clone();
                                        viewport.shiftInPlace(size / 5);
                                        client.setViewport(viewport);
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
        for (final int size : new int[]{10, 100, 1000}) {
            final int numProducerCoalesce = 8;
            final int numConsumerCoalesce = 8;
            for (int subConsumerCoalesce = 0; subConsumerCoalesce < numConsumerCoalesce; ++subConsumerCoalesce) {
                final int finalSubConsumerCoalesce = subConsumerCoalesce;
                new SubscriptionChangingHelper(numProducerCoalesce, numConsumerCoalesce, size, 0, new MutableInt(4)) {
                    {
                        for (final RemoteNugget nugget : nuggets) {
                            final BitSet columns = new BitSet();
                            columns.set(0, 4);
                            nugget.clients.add(new RemoteClient(Index.FACTORY.getIndexByRange(size / 5, 2 * size / 5), columns, nugget.barrageMessageProducer, "sub-changer"));
                        }
                    }

                    void maybeChangeSub(final int step, final int rt, final int pt) {
                        if (step != 2 || rt != finalSubConsumerCoalesce) {
                            return;
                        }

                        for (final RemoteNugget nugget : nuggets) {
                            final RemoteClient client = nugget.clients.get(nugget.clients.size() - 1);
                            final int firstKey = random.nextInt(size);
                            client.setViewport(Index.FACTORY.getIndexByRange(firstKey, firstKey + random.nextInt(size - firstKey)));
                        }
                    }
                }.runTest();
            }
        }
    }

    public void testUsePrevOnSnapshot() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(10, 12), c("intCol", 10, 12));
        final RemoteNugget remoteNugget = new RemoteNugget(() -> queryTable);
        final MutableObject<RemoteClient> remoteClient = new MutableObject<>();

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(queryTable, i(10, 12));
            TstUtils.addToTable(queryTable, i(5, 7), c("intCol", 10, 12));

            final IndexShiftData.Builder shiftBuilder = new IndexShiftData.Builder();
            shiftBuilder.shiftRange(0, 12, -5);

            queryTable.notifyListeners(new ShiftAwareListener.Update(
                    Index.FACTORY.getEmptyIndex(),
                    Index.FACTORY.getEmptyIndex(),
                    Index.FACTORY.getEmptyIndex(),
                    shiftBuilder.build(), ModifiedColumnSet.EMPTY));

            final BitSet cols = new BitSet(1);
            cols.set(0);
            remoteClient.setValue(remoteNugget.newClient(Index.FACTORY.getIndexByRange(0, 1), cols, "prevSnapshot"));

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
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(liveTableRegistrar::refresh);

        // validate
        remoteNugget.validate("post flush");
    }

    public void testRegressModificationsInPrevView() {
        final BitSet allColumns = new BitSet(1);
        allColumns.set(0);

        final QueryTable queryTable = TstUtils.testRefreshingTable(i(5, 10, 12), c("intCol", 5, 10, 12));
        final RemoteNugget remoteNugget = new RemoteNugget(() -> queryTable);

        // Set original viewport.
        final RemoteClient remoteClient = remoteNugget.newClient(Index.FACTORY.getIndexByRange(1, 2), allColumns, "prevSnapshot");

        // Obtain snapshot of original viewport.
        flushProducerTable();
        remoteNugget.flushClientEvents();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(liveTableRegistrar::refresh);
        remoteNugget.validate("original viewport");

        // Change viewport without overlap.
        remoteClient.setViewport(Index.FACTORY.getIndexByRange(0, 1));

        // Modify row that is outside of new viewport but in original.
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(queryTable, i(12), c("intCol", 13));

            queryTable.notifyListeners(new ShiftAwareListener.Update(
                    Index.FACTORY.getEmptyIndex(),
                    Index.FACTORY.getEmptyIndex(),
                    Index.FACTORY.getIndexByValues(12),
                    IndexShiftData.EMPTY, ModifiedColumnSet.ALL));
        });

        // Do not allow the two updates to coalesce; we must force the consumer to apply the modification. (An allowed race.)
        flushProducerTable();

        // Add rows to shift modified row into new viewport.
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(queryTable, i(5));

            queryTable.notifyListeners(new ShiftAwareListener.Update(
                    Index.FACTORY.getEmptyIndex(),
                    Index.FACTORY.getIndexByValues(5),
                    Index.FACTORY.getEmptyIndex(),
                    IndexShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        });

        // Obtain snapshot of new viewport. (which will not include the modified row)
        flushProducerTable();
        Assert.equals(remoteClient.commandQueue.size(), "remoteClient.getValue().commandQueue.size()", 3); // mod, add, snaphot
        remoteNugget.flushClientEvents();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(liveTableRegistrar::refresh);
        remoteNugget.validate("new viewport with modification");
    }

    public void testAllUniqueChunkTypeColumnSourcesWithValidityBuffers() {
        testAllUniqueChunkTypeColumnSources(false);
    }

    public void testAllUniqueChunkTypeColumnSourcesWithDeephavenNulls() {
        testAllUniqueChunkTypeColumnSources(true);
    }

    private void testAllUniqueChunkTypeColumnSources(final boolean useDeephavenNulls) {
        this.useDeephavenNulls = useDeephavenNulls;

        final int MAX_STEPS = 100;
        for (int size : new int[]{10, 1000, 10000}) {
            SharedProducerForAllClients helper = new SharedProducerForAllClients(1, 1, size, 0, new MutableInt(MAX_STEPS)) {
                @Override
                public void createTable() {
                    columnInfo = initColumnInfos(new String[]{"longCol", "intCol", "objCol", "byteCol", "doubleCol", "floatCol", "shortCol", "charCol", "boolCol", "strArrCol", "datetimeCol"},
                            new TstUtils.SortedLongGenerator(0, Long.MAX_VALUE - 1),
                            new TstUtils.IntGenerator(10, 100, 0.1),
                            new TstUtils.SetGenerator<>("a", "b", "c", "d"), // covers object
                            new TstUtils.ByteGenerator((byte) 0, (byte) 127, 0.1),
                            new TstUtils.DoubleGenerator(100.1, 200.1, 0.1),
                            new TstUtils.FloatGenerator(100.1f, 200.1f, 0.1),
                            new TstUtils.ShortGenerator((short) 0, (short) 20000, 0.1),
                            new TstUtils.CharGenerator('a', 'z', 0.1),
                            new TstUtils.BooleanGenerator(0.2),
                            new TstUtils.SetGenerator<>(new String[]{"a", "b"}, new String[]{"0", "1"}, new String[]{}, null),
                            new TstUtils.UnsortedDateTimeGenerator(DBTimeUtils.convertDateTime("2020-02-14T00:00:00 NY"), DBTimeUtils.convertDateTime("2020-02-25T00:00:00 NY")));
                    sourceTable = getTable(size / 4, random, columnInfo);
                }
            };

            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16) + (helper.sourceTable.isEmpty() ? -1 : helper.sourceTable.getIndex().lastKey());
                final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = Index.CURRENT_FACTORY.getIndexByRange(lastKey + 1, lastKey + stepSize);
                update.removed = i();
                if (helper.sourceTable.isEmpty()) {
                    update.modified = i();
                } else {
                    update.modified = Index.CURRENT_FACTORY.getIndexByRange(Math.max(0, lastKey - stepSize), lastKey);
                    update.modified.retain(helper.sourceTable.getIndex());
                }
                update.shifted = IndexShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update, helper.random, helper.sourceTable, helper.columnInfo));
            });
        }
    }

    public void testAllUniqueNonJavaSerRoundTripTypesWithValidityBuffers() {
        testAllUniqueNonJavaSerRoundTripTypes(false);
    }

    public void testAllUniqueNonJavaSerRoundTripTypesWithDeephavenNulls() {
        testAllUniqueNonJavaSerRoundTripTypes(true);
    }

    private void testAllUniqueNonJavaSerRoundTripTypes(final boolean useDeephavenNulls) {
        this.useDeephavenNulls = useDeephavenNulls;

        final int MAX_STEPS = 100;
        for (int size : new int[]{10, 1000, 10000}) {
            SharedProducerForAllClients helper = new SharedProducerForAllClients(1, 1, size, 0, new MutableInt(MAX_STEPS)) {
                @Override
                public void createTable() {
                    columnInfo = initColumnInfos(new String[]{"longCol", "intCol", "objCol", "byteCol", "doubleCol", "floatCol", "shortCol", "charCol", "boolCol", "strCol", "strArrCol", "datetimeCol"},
                            new TstUtils.SortedLongGenerator(0, Long.MAX_VALUE - 1),
                            new TstUtils.IntGenerator(10, 100, 0.1),
                            new TstUtils.SetGenerator<>("a", "b", "c", "d"), // covers strings
                            new TstUtils.ByteGenerator((byte) 0, (byte) 127, 0.1),
                            new TstUtils.DoubleGenerator(100.1, 200.1, 0.1),
                            new TstUtils.FloatGenerator(100.1f, 200.1f, 0.1),
                            new TstUtils.ShortGenerator((short) 0, (short) 20000, 0.1),
                            new TstUtils.CharGenerator('a', 'z', 0.1),
                            new TstUtils.BooleanGenerator(0.2),
                            new TstUtils.StringGenerator(),
                            new TstUtils.SetGenerator<>(new String[]{"a", "b"}, new String[]{"0", "1"}, new String[]{}, null),
                            new TstUtils.UnsortedDateTimeGenerator(DBTimeUtils.convertDateTime("2020-02-14T00:00:00 NY"), DBTimeUtils.convertDateTime("2020-02-25T00:00:00 NY")));
                    sourceTable = getTable(size / 4, random, columnInfo);
                }
            };

            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16) + (helper.sourceTable.isEmpty() ? -1 : helper.sourceTable.getIndex().lastKey());
                final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = Index.CURRENT_FACTORY.getIndexByRange(lastKey + 1, lastKey + stepSize);
                update.removed = i();
                if (helper.sourceTable.isEmpty()) {
                    update.modified = i();
                } else {
                    update.modified = Index.CURRENT_FACTORY.getIndexByRange(Math.max(0, lastKey - stepSize), lastKey);
                    update.modified.retain(helper.sourceTable.getIndex());
                }
                update.shifted = IndexShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update, helper.random, helper.sourceTable, helper.columnInfo));
            });
        }
    }

    public static class DummyObserver implements StreamObserver<BarrageStreamGenerator.View> {
        volatile boolean completed = false;

        private final BarrageMarshaller marshaller;
        private final Queue<BarrageMessage> receivedCommands;

        DummyObserver(final BarrageMarshaller marshaller, final Queue<BarrageMessage> receivedCommands) {
            this.marshaller = marshaller;
            this.receivedCommands = receivedCommands;
        }

        @Override
        public void onNext(final BarrageStreamGenerator.View messageView) {
            try {
                messageView.forEachStream(inputStream -> {
                    try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos = new BarrageProtoUtil.ExposedByteArrayOutputStream()) {
                        ((Drainable) inputStream).drainTo(baos);
                        inputStream.close();
                        final BarrageMessage message = marshaller.parse(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                        // we skip schema messages, but can't suppress without propagating something...
                        if (message != null) {
                            receivedCommands.add(message);
                        }
                    } catch (final IOException e) {
                        throw new IllegalStateException("Failed to parse barrage message: ", e);
                    }
                });
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to parse barrage message: ", e);
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            throw new IllegalStateException(throwable);
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private static class BarrageMarshaller extends FlightServiceGrpcBinding.BarrageDataMarshaller<ChunkInputStreamGenerator.Options> {
        public BarrageMarshaller(final ChunkInputStreamGenerator.Options options,
                                 final ChunkType[] columnChunkTypes,
                                 final Class<?>[] columnTypes,
                                 final Class<?>[] componentTypes,
                                 final BarrageMessageConsumer.StreamReader<ChunkInputStreamGenerator.Options> streamReader) {
            super(options, columnChunkTypes, columnTypes, componentTypes, streamReader);
        }
    }
}
