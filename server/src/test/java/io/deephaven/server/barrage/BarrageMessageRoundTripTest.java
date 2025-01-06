//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

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
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageStreamGenerator;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.BarrageStreamReader;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.Scheduler;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.mutable.MutableInt;
import io.grpc.stub.StreamObserver;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.experimental.categories.Category;

import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.deephaven.engine.table.impl.remote.ConstructSnapshot.SNAPSHOT_CHUNK_SIZE;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.col;

@Category(OutOfBandTest.class)
public class BarrageMessageRoundTripTest extends RefreshingTableTestCase {
    private static final long UPDATE_INTERVAL = 1000; // arbitrary; we enforce coalescing on both sides

    private TestControlledScheduler scheduler;
    private Deque<Throwable> exceptions;
    private UpdateSourceCombiner updateSourceCombiner;
    private boolean useDeephavenNulls;

    private TestComponent daggerRoot;

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
        updateSourceCombiner = new UpdateSourceCombiner(ExecutionContext.getContext().getUpdateGraph());
        scheduler = new TestControlledScheduler();
        exceptions = new ArrayDeque<>();
        useDeephavenNulls = true;

        daggerRoot = DaggerBarrageMessageRoundTripTest_TestComponent
                .builder()
                .withScheduler(scheduler)
                .build();
    }

    @Override
    public void tearDown() throws Exception {
        updateSourceCombiner = null;
        scheduler = null;
        exceptions = null;
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
        private RowSet viewport;
        private boolean reverseViewport;

        private BitSet subscribedColumns;

        private final String name;

        private final BarrageTable barrageTable;
        @ReferentialIntegrity
        private final BarrageMessageProducer barrageMessageProducer;

        @ReferentialIntegrity
        private final TableUpdateValidator replicatedTUV;
        @ReferentialIntegrity
        private final FailureListener replicatedTUVListener;

        private boolean subscribed = false;
        private final Queue<BarrageMessage> commandQueue = new ArrayDeque<>();
        private final DummyObserver dummyObserver;

        // The replicated table's TableUpdateValidator will be confused if the table is a viewport. Instead we rely on
        // comparing the producer table to the consumer table to validate contents are correct.
        RemoteClient(final RowSet viewport, final BitSet subscribedColumns,
                final BarrageMessageProducer barrageMessageProducer,
                final Table sourceTable, final String name) {
            // assume a forward viewport when not specified
            this(viewport, subscribedColumns, barrageMessageProducer, sourceTable, name, false, false);
        }

        RemoteClient(final RowSet viewport, final BitSet subscribedColumns,
                final BarrageMessageProducer barrageMessageProducer,
                final Table sourceTable,
                final String name, final boolean reverseViewport, final boolean deferSubscription) {
            this.viewport = viewport;
            this.reverseViewport = reverseViewport;
            this.subscribedColumns = subscribedColumns;
            this.name = name;
            this.barrageMessageProducer = barrageMessageProducer;

            final Map<String, Object> attributes = new HashMap<>(sourceTable.getAttributes());
            if (sourceTable.isFlat()) {
                attributes.put(BarrageUtil.TABLE_ATTRIBUTE_IS_FLAT, true);
            }
            this.barrageTable = BarrageTable.make(updateSourceCombiner,
                    ExecutionContext.getContext().getUpdateGraph(),
                    null, barrageMessageProducer.getTableDefinition(), attributes, viewport == null, null);
            this.barrageTable.addSourceToRegistrar();

            final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                    .useDeephavenNulls(useDeephavenNulls)
                    .build();
            final BarrageDataMarshaller marshaller = new BarrageDataMarshaller(
                    options, barrageTable.getWireChunkTypes(), barrageTable.getWireTypes(),
                    barrageTable.getWireComponentTypes(),
                    new BarrageStreamReader(barrageTable.getDeserializationTmConsumer()));
            this.dummyObserver = new DummyObserver(marshaller, commandQueue);

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

            if (!deferSubscription) {
                doSubscribe();
            }
        }

        public void doSubscribe() {
            subscribed = true;
            final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                    .useDeephavenNulls(useDeephavenNulls)
                    .build();
            barrageMessageProducer.addSubscription(dummyObserver, options, subscribedColumns,
                    viewport == null ? null : viewport.copy(), reverseViewport);
        }

        public void validate(final String msg, QueryTable expected) {
            if (!subscribed) {
                return; // no subscription implies no run implies no data -- so we're valid
            }

            // We expect all messages from original table to have been propagated to the replicated table at this point.

            QueryTable toCheck = barrageTable;
            if (viewport != null) {
                expected = expected
                        .getSubTable(expected.getRowSet().subSetForPositions(viewport, reverseViewport).toTracking());
            }
            if (subscribedColumns.cardinality() != expected.numColumns()) {
                final List<Selectable> columns = new ArrayList<>();
                for (int i = subscribedColumns.nextSetBit(0); i >= 0; i = subscribedColumns.nextSetBit(i + 1)) {
                    columns.add(ColumnName.of(expected.getDefinition().getColumns().get(i).getName()));
                }
                expected = (QueryTable) expected.view(columns);
                toCheck = (QueryTable) toCheck.view(columns);
            }

            // Data should be identical and in-order.
            TstUtils.assertTableEquals(expected, toCheck);
            if (viewport == null) {
                // Since key-space needs to be kept the same, the RowSets should also be identical between producer and
                // consumer (not RowSets between expected and consumer; as the consumer maintains the entire RowSet).
                Assert.equals(barrageMessageProducer.getRowSet(), "barrageMessageProducer.getRowSet()",
                        barrageTable.getRowSet(), "barrageTable.getRowSet()");
            } else {
                // otherwise, the RowSet should represent a flattened view of the viewport
                Assert.eqTrue(barrageTable.getRowSet().isFlat(), "barrageTable.getRowSet().isFlat()");
            }
        }

        private void showResult(final String label, final Table table) {
            System.out.println(label);
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
                showResult("Ticking Table (" + name + "):", toCheck);
            } else {
                final long numTableRows = Math.min(maxLines, Math.max(toCheck.size(), expected.size()));
                final long firstRow = Math.max(0, diffPair.getSecond() - 5);
                final long lastRow =
                        Math.min(firstRow + numTableRows, Math.min(firstRow + maxLines, diffPair.getSecond() + 5));

                System.out.println("Recomputed Table (" + name + ") Differs:\n" + diffPair.getFirst()
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

        public void setViewport(final RowSet newViewport) {
            // assume a forward viewport when not specified
            setViewport(newViewport, false);
        }

        public void setViewport(final RowSet newViewport, final boolean newReverseViewport) {
            viewport = newViewport;
            reverseViewport = newReverseViewport;

            // maintain the existing subscribedColumns set
            barrageMessageProducer.updateSubscription(dummyObserver, viewport, subscribedColumns, reverseViewport);
        }

        public void setSubscribedColumns(final BitSet newColumns) {
            subscribedColumns = newColumns;

            // maintain the existing viewport and viewport direction
            barrageMessageProducer.updateSubscription(dummyObserver, viewport, subscribedColumns, reverseViewport);
        }

        public void setViewportAndColumns(final RowSet newViewport, final BitSet newColumns) {
            // assume a forward viewport when not specified
            setViewportAndColumns(newViewport, newColumns, false);
        }

        public void setViewportAndColumns(final RowSet newViewport, final BitSet newColumns,
                final boolean newReverseViewport) {
            viewport = newViewport;
            reverseViewport = newReverseViewport;
            subscribedColumns = newColumns;
            barrageMessageProducer.updateSubscription(dummyObserver, viewport, subscribedColumns, newReverseViewport);
        }
    }

    private class RemoteNugget implements EvalNuggetInterface {

        private final Supplier<Table> makeTable;

        private final QueryTable originalTable;
        @ReferentialIntegrity
        private final BarrageMessageProducer barrageMessageProducer;

        @ReferentialIntegrity
        private final TableUpdateValidator originalTUV;
        @ReferentialIntegrity
        private final FailureListener originalTUVListener;

        private final List<RemoteClient> clients = new ArrayList<>();

        RemoteNugget(final Supplier<Table> makeTable) {
            this.makeTable = makeTable;
            this.originalTable = (QueryTable) makeTable.get();
            this.barrageMessageProducer = originalTable.getResult(new BarrageMessageProducer.Operation(scheduler,
                    new SessionService.ObfuscatingErrorTransformer(), daggerRoot.getStreamGeneratorFactory(),
                    originalTable, UPDATE_INTERVAL, this::onGetSnapshot));

            originalTUV = TableUpdateValidator.make(originalTable);
            originalTUVListener = new FailureListener("Original Table Update Validator");
            originalTUV.getResultTable().addUpdateListener(originalTUVListener);
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

        public RemoteClient newClient(final RowSet viewport, final BitSet subscribedColumns, final String name) {
            // assume a forward viewport when not specified
            return newClient(viewport, subscribedColumns, false, name);
        }

        public RemoteClient newClient(final RowSet viewport, final BitSet subscribedColumns,
                final boolean reverseViewport, final String name) {
            clients.add(new RemoteClient(viewport, subscribedColumns, barrageMessageProducer,
                    originalTable, name, reverseViewport, false));
            return clients.get(clients.size() - 1);
        }

        public void onGetSnapshot() {}
    }

    private abstract class TestHelper {
        final int numProducerCoalesce;
        final int numConsumerCoalesce;

        final int size;
        final Random random;
        final MutableInt numSteps;

        final List<RemoteNugget> nuggets = new ArrayList<>();

        QueryTable sourceTable;
        ColumnInfo<?, ?>[] columnInfo;

        TestHelper(final int numProducerCoalesce, final int numConsumerCoalesce, final int size, final int seed,
                final MutableInt numSteps) {
            this.numProducerCoalesce = numProducerCoalesce;
            this.numConsumerCoalesce = numConsumerCoalesce;
            this.size = size;
            this.random = new Random(seed);
            this.numSteps = numSteps;
        }

        public void createTable() {
            sourceTable = getTable(size / 4, random,
                    columnInfo =
                            initColumnInfos(
                                    new String[] {"Sym", "intCol", "doubleCol", "Indices", "boolCol", "TimeStamp"},
                                    new SetGenerator<>("a", "b", "c", "d"),
                                    new IntGenerator(10, 100),
                                    new SetGenerator<>(10.1, 20.1, 30.1),
                                    new SortedLongGenerator(0, Long.MAX_VALUE - 1),
                                    new BooleanGenerator(0.2),
                                    new UnsortedInstantGenerator(
                                            DateTimeUtils.parseInstant("2020-02-14T00:00:00 NY"),
                                            DateTimeUtils.parseInstant("2020-02-25T00:00:00 NY"))));
        }

        public void createNuggets() {
            // test the explicit updates
            createNuggetsForTableMaker(() -> sourceTable);
            // test shift aggressive version of these updates
            createNuggetsForTableMaker(sourceTable::flatten);
            // test updates in the middle of the keyspace
            createNuggetsForTableMaker(() -> sourceTable.sort("doubleCol"));
            // test sparse(r) updates
            createNuggetsForTableMaker(() -> sourceTable.where("intCol % 12 < 5"));
        }

        void runTest(final Runnable simulateSourceStep) {
            createTable();
            createNuggets();
            final int maxSteps = numSteps.get();
            final RemoteNugget[] nuggetsToValidate = nuggets.toArray(new RemoteNugget[0]);
            for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
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
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

                TstUtils.validate("", nuggetsToValidate);

                if (sourceTable.size() >= size) {
                    numSteps.set(maxSteps); // pretend we finished
                    return;
                }
            }
        }

        abstract void createNuggetsForTableMaker(final Supplier<Table> makeTable);
    }

    private class OneProducerPerClient extends TestHelper {
        OneProducerPerClient(final int numProducerCoalesce, final int numConsumerCoalesce, final int size,
                final int seed, final MutableInt numSteps) {
            super(numProducerCoalesce, numConsumerCoalesce, size, seed, numSteps);
        }

        void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
            final BitSet subscribedColumns = new BitSet();
            subscribedColumns.set(0, makeTable.get().numColumns());

            nuggets.add(new RemoteNugget(makeTable));
            nuggets.get(nuggets.size() - 1).newClient(null, subscribedColumns, "full");

            nuggets.add(new RemoteNugget(makeTable));
            nuggets.get(nuggets.size() - 1).newClient(RowSetFactory.fromRange(0, size / 10),
                    subscribedColumns, "header");
            nuggets.add(new RemoteNugget(makeTable));
            nuggets.get(nuggets.size() - 1).newClient(
                    RowSetFactory.fromRange(size / 2, size * 3L / 4),
                    subscribedColumns, "floating");

            nuggets.add(new RemoteNugget(makeTable));
            nuggets.get(nuggets.size() - 1).newClient(
                    RowSetFactory.fromRange(0, size / 10),
                    subscribedColumns, true, "footer");
            nuggets.add(new RemoteNugget(makeTable));
            nuggets.get(nuggets.size() - 1).newClient(
                    RowSetFactory.fromRange(size / 2, size * 3L / 4),
                    subscribedColumns, true, "reverse floating");

            final RowSetBuilderSequential swissIndexBuilder = RowSetFactory.builderSequential();
            final long rangeSize = Math.max(1, size / 20);
            for (long nr = 1; nr < 20; nr += 2) {
                swissIndexBuilder.appendRange(nr * rangeSize, (nr + 1) * rangeSize - 1);
            }
            final RowSet rs = swissIndexBuilder.build();

            nuggets.add(new RemoteNugget(makeTable));
            nuggets.get(nuggets.size() - 1).newClient(rs, subscribedColumns, "swiss");


            final RemoteNugget nugget = new RemoteNugget(makeTable);
            nugget.newClient(rs.copy(), subscribedColumns, true, "reverse swiss");
            nuggets.add(nugget);
        }
    }

    private class SharedProducerForAllClients extends TestHelper {
        SharedProducerForAllClients(final int numProducerCoalesce, final int numConsumerCoalesce, final int size,
                final int seed, final MutableInt numSteps) {
            super(numProducerCoalesce, numConsumerCoalesce, size, seed, numSteps);
        }

        void createNuggetsForTableMaker(final Supplier<Table> makeTable) {
            final BitSet subscribedColumns = new BitSet();
            subscribedColumns.set(0, makeTable.get().numColumns());

            final RemoteNugget nugget = new RemoteNugget(makeTable);
            nuggets.add(nugget);
            nugget.newClient(null, subscribedColumns, "full");

            nugget.newClient(RowSetFactory.fromRange(0, size / 10), subscribedColumns, "header");
            nugget.newClient(RowSetFactory.fromRange(size / 2, size * 3L / 4), subscribedColumns, "floating");

            nugget.newClient(RowSetFactory.fromRange(0, size / 10), subscribedColumns, true, "footer");
            nugget.newClient(RowSetFactory.fromRange(size / 2, size * 3L / 4), subscribedColumns, true,
                    "reverse floating");

            final RowSetBuilderSequential swissIndexBuilder = RowSetFactory.builderSequential();
            final long rangeSize = Math.max(1, size / 20);
            for (long nr = 1; nr < 20; nr += 2) {
                swissIndexBuilder.appendRange(nr * rangeSize, (nr + 1) * rangeSize - 1);
            }

            final RowSet rs = swissIndexBuilder.build();
            nugget.newClient(rs, subscribedColumns, "swiss");

            nugget.newClient(rs.copy(), subscribedColumns, true, "reverse swiss");
        }
    }

    public void testAppendIncremental() {
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
            for (final int numProducerCoalesce : new int[] {1, 10}) {
                for (final int numConsumerCoalesce : new int[] {1, 10}) {
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    public void testPrependIncremental() {
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
            for (final int numProducerCoalesce : new int[] {1, 10}) {
                for (final int numConsumerCoalesce : new int[] {1, 10}) {
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(MAX_STEPS)));
                }
            }
        }
    }

    public void testRoundTripIncremental() {
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
                    runOne.accept(new OneProducerPerClient(numProducerCoalesce, numConsumerCoalesce, size, 0,
                            new MutableInt(100)));
                }
            }
        }
    }

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

    // These test mid-cycle subscription changes and snapshot content
    private abstract class SubscriptionChangingHelper extends SharedProducerForAllClients {
        SubscriptionChangingHelper(final int numProducerCoalesce, final int numConsumerCoalesce, final int size,
                final int seed, final MutableInt numSteps) {
            super(numProducerCoalesce, numConsumerCoalesce, size, seed, numSteps);
        }

        void runTest() {
            createTable();
            createNuggets();
            final int maxSteps = numSteps.get();
            final RemoteNugget[] nuggetsToValidate = nuggets.toArray(new RemoteNugget[0]);
            for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
                for (int rt = 0; rt < numConsumerCoalesce; ++rt) {
                    // coalesce updates in producer
                    for (int pt = 0; pt < numProducerCoalesce; ++pt) {
                        maybeChangeSub(numSteps.get(), rt, pt);

                        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                        updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                                GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
                    }

                    // flush producer
                    flushProducerTable();
                }

                // flush consumer
                for (final RemoteNugget nugget : nuggets) {
                    nugget.flushClientEvents();
                }
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);

                TstUtils.validate("", nuggetsToValidate);
            }
        }

        abstract void maybeChangeSub(int step, int rt, int pt);
    }

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
            shiftBuilder.shiftRange(0, 12, -5);

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

    public void testAllUniqueChunkTypeColumnSourcesWithValidityBuffers() {
        testAllUniqueChunkTypeColumnSources(false);
    }

    public void testAllUniqueChunkTypeColumnSourcesWithDeephavenNulls() {
        testAllUniqueChunkTypeColumnSources(true);
    }

    private void testAllUniqueChunkTypeColumnSources(final boolean useDeephavenNulls) {
        this.useDeephavenNulls = useDeephavenNulls;

        final int MAX_STEPS = 100;
        for (int size : new int[] {10, 1000, 10000}) {
            SharedProducerForAllClients helper =
                    new SharedProducerForAllClients(1, 1, size, 0, new MutableInt(MAX_STEPS)) {
                        @Override
                        public void createTable() {
                            columnInfo = initColumnInfos(
                                    new String[] {"longCol", "intCol", "objCol", "byteCol", "doubleCol", "floatCol",
                                            "shortCol", "charCol", "boolCol", "strArrCol", "datetimeCol",
                                            "bytePrimArray", "intPrimArray"},
                                    new SortedLongGenerator(0, Long.MAX_VALUE - 1),
                                    new IntGenerator(10, 100, 0.1),
                                    new SetGenerator<>("a", "b", "c", "d"), // covers object
                                    new ByteGenerator((byte) 0, (byte) 127, 0.1),
                                    new DoubleGenerator(100.1, 200.1, 0.1),
                                    new FloatGenerator(100.1f, 200.1f, 0.1),
                                    new ShortGenerator((short) 0, (short) 20000, 0.1),
                                    new CharGenerator('a', 'z', 0.1),
                                    new BooleanGenerator(0.2),
                                    new SetGenerator<>(new String[] {"a", "b"}, new String[] {"0", "1"},
                                            new String[] {}, null),
                                    new UnsortedInstantGenerator(
                                            DateTimeUtils.parseInstant("2020-02-14T00:00:00 NY"),
                                            DateTimeUtils.parseInstant("2020-02-25T00:00:00 NY")),
                                    // uses var binary encoding
                                    new ByteArrayGenerator(Byte.MIN_VALUE, Byte.MAX_VALUE, 0, 32),
                                    // uses var list encoding
                                    new IntArrayGenerator(0, Integer.MAX_VALUE, 0, 32));
                            sourceTable = getTable(size / 4, random, columnInfo);
                        }
                    };

            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16)
                        + (helper.sourceTable.isEmpty() ? -1 : helper.sourceTable.getRowSet().lastRowKey());
                final TableUpdateImpl update = new TableUpdateImpl();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = RowSetFactory.fromRange(lastKey + 1, lastKey + stepSize);
                update.removed = i();
                if (helper.sourceTable.isEmpty()) {
                    update.modified = i();
                } else {
                    update.modified =
                            RowSetFactory.fromRange(Math.max(0, lastKey - stepSize), lastKey);
                    update.modified().writableCast().retain(helper.sourceTable.getRowSet());
                }
                update.shifted = RowSetShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update,
                        helper.random, helper.sourceTable, helper.columnInfo));
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
        for (int size : new int[] {10, 1000, 10000}) {
            SharedProducerForAllClients helper =
                    new SharedProducerForAllClients(1, 1, size, 0, new MutableInt(MAX_STEPS)) {
                        @Override
                        public void createTable() {
                            columnInfo = initColumnInfos(
                                    new String[] {"longCol", "intCol", "objCol", "byteCol", "doubleCol", "floatCol",
                                            "shortCol", "charCol", "boolCol", "strCol", "strArrCol", "datetimeCol"},
                                    new SortedLongGenerator(0, Long.MAX_VALUE - 1),
                                    new IntGenerator(10, 100, 0.1),
                                    new SetGenerator<>("a", "b", "c", "d"), // covers strings
                                    new ByteGenerator((byte) 0, (byte) 127, 0.1),
                                    new DoubleGenerator(100.1, 200.1, 0.1),
                                    new FloatGenerator(100.1f, 200.1f, 0.1),
                                    new ShortGenerator((short) 0, (short) 20000, 0.1),
                                    new CharGenerator('a', 'z', 0.1),
                                    new BooleanGenerator(0.2),
                                    new StringGenerator(),
                                    new SetGenerator<>(new String[] {"a", "b"}, new String[] {"0", "1"},
                                            new String[] {}, null),
                                    new UnsortedInstantGenerator(
                                            DateTimeUtils.parseInstant("2020-02-14T00:00:00 NY"),
                                            DateTimeUtils.parseInstant("2020-02-25T00:00:00 NY")));
                            sourceTable = getTable(size / 4, random, columnInfo);
                        }
                    };

            final int maxSteps = MAX_STEPS * helper.numConsumerCoalesce * helper.numProducerCoalesce;
            helper.runTest(() -> {
                final long lastKey = (Math.abs(helper.random.nextLong()) % 16)
                        + (helper.sourceTable.isEmpty() ? -1 : helper.sourceTable.getRowSet().lastRowKey());
                final TableUpdateImpl update = new TableUpdateImpl();
                final int stepSize = Math.max(1, helper.size / maxSteps);
                update.added = RowSetFactory.fromRange(lastKey + 1, lastKey + stepSize);
                update.removed = i();
                if (helper.sourceTable.isEmpty()) {
                    update.modified = i();
                } else {
                    update.modified =
                            RowSetFactory.fromRange(Math.max(0, lastKey - stepSize), lastKey);
                    update.modified().writableCast().retain(helper.sourceTable.getRowSet());
                }
                update.shifted = RowSetShiftData.EMPTY;
                update.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateTableUpdates(update,
                        helper.random, helper.sourceTable, helper.columnInfo));
            });
        }
    }

    public static class DummyObserver implements StreamObserver<BarrageStreamGenerator.MessageView> {
        volatile boolean completed = false;

        private final BarrageDataMarshaller marshaller;
        private final Queue<BarrageMessage> receivedCommands;

        DummyObserver(final BarrageDataMarshaller marshaller, final Queue<BarrageMessage> receivedCommands) {
            this.marshaller = marshaller;
            this.receivedCommands = receivedCommands;
        }

        @Override
        public void onNext(final BarrageStreamGenerator.MessageView messageView) {
            try {
                messageView.forEachStream(inputStream -> {
                    try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                        inputStream.drainTo(baos);
                        inputStream.close();
                        final BarrageMessage message =
                                marshaller.parse(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
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
}
