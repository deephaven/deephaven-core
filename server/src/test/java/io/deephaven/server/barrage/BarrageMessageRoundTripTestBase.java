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

public abstract class BarrageMessageRoundTripTestBase extends RefreshingTableTestCase {
    static final long UPDATE_INTERVAL = 1000; // arbitrary; we enforce coalescing on both sides

    TestControlledScheduler scheduler;
    Deque<Throwable> exceptions;
    UpdateSourceCombiner updateSourceCombiner;
    boolean useDeephavenNulls;
    /** Message readers retain dictionary value chunks; they must be closed before the leak check in tearDown. */
    List<BarrageMessageReaderImpl> openMessageReaders;

    TestComponent daggerRoot;

    @Singleton
    @Component(modules = {
            ArrowModule.class
    })
    public interface TestComponent {
        BarrageMessageWriter.Factory getStreamGeneratorFactory();

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
        openMessageReaders = new ArrayList<>();

        daggerRoot = DaggerBarrageMessageRoundTripTestBase_TestComponent
                .builder()
                .withScheduler(scheduler)
                .build();
    }

    @Override
    public void tearDown() throws Exception {
        openMessageReaders.forEach(BarrageMessageReaderImpl::close);
        openMessageReaders = null;
        updateSourceCombiner = null;
        scheduler = null;
        exceptions = null;
        super.tearDown();
    }

    void flushProducerTable() {
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

    class RemoteClient {
        RowSet viewport;
        boolean reverseViewport;

        BitSet subscribedColumns;

        final String name;

        final BarrageTable barrageTable;
        @ReferentialIntegrity
        final BarrageMessageProducer barrageMessageProducer;

        /** whether this is a full subscription, which {@link #viewport} does not record once it is changed */
        final boolean isFullSubscription;
        /** set once the server has acknowledged a full subscription, so the replicated table is complete */
        boolean fullSubscriptionSatisfied;

        @ReferentialIntegrity
        TableUpdateValidator replicatedTUV;
        @ReferentialIntegrity
        FailureListener replicatedTUVListener;

        boolean subscribed = false;
        final Queue<BarrageMessage> commandQueue = new ArrayDeque<>();
        final DummyObserver dummyObserver;

        UnaryOperator<QueryTable> transform = null;

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
            this.isFullSubscription = viewport == null;

            final Map<String, Object> attributes = new HashMap<>(sourceTable.getAttributes());
            if (sourceTable.isFlat()) {
                attributes.put(BarrageUtil.TABLE_ATTRIBUTE_IS_FLAT, true);
            }
            final BarrageUtil.ConvertedArrowSchema schema = BarrageUtil.convertArrowSchema(BarrageUtil.makeSchema(
                    BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, barrageMessageProducer.getTableDefinition(), attributes,
                    sourceTable.isFlat()));
            this.barrageTable =
                    BarrageTable.make(null, updateSourceCombiner, ExecutionContext.getContext().getUpdateGraph(),
                            null, schema, isFullSubscription, new BarrageTable.ViewportChangedCallback() {
                                @Override
                                public boolean viewportChanged(
                                        @Nullable final RowSet rowSet,
                                        @Nullable final BitSet columns,
                                        final boolean reverse) {
                                    if (!isFullSubscription || rowSet != null) {
                                        // the server is still growing this subscription toward the entire table
                                        return true;
                                    }
                                    fullSubscriptionSatisfied = true;
                                    return false;
                                }

                                @Override
                                public void onError(@NotNull final Throwable t) {
                                    exceptions.add(t);
                                }
                            });
            this.barrageTable.addSourceToRegistrar();

            final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                    .useDeephavenNulls(useDeephavenNulls)
                    .build();
            final BarrageMessageReaderImpl messageReader =
                    new BarrageMessageReaderImpl(barrageTable.getDeserializationTmConsumer());
            openMessageReaders.add(messageReader);
            final BarrageDataMarshaller marshaller = new BarrageDataMarshaller(
                    options, schema.computeWireChunkTypes(), schema.computeWireTypes(),
                    schema.computeWireComponentTypes(), messageReader);
            this.dummyObserver = new DummyObserver(marshaller, commandQueue);

            // Note that no TableUpdateValidator is created for a viewport subscription: the TUV is unaware of the
            // viewport and gets confused about which data should be valid. Instead we rely on the validation of the
            // content in the viewport between the consumer and expected table. A full subscription's validator is
            // attached by maybeAttachUpdateValidator() once the replicated table is complete.

            if (!deferSubscription) {
                doSubscribe();
            }
        }

        public void setTransform(UnaryOperator<QueryTable> transform) {
            this.transform = transform;
        }

        public void doSubscribe() {
            subscribed = true;
            final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                    .useDeephavenNulls(useDeephavenNulls)
                    .build();
            barrageMessageProducer.addSubscription(dummyObserver, options, subscribedColumns,
                    viewport == null ? null : viewport.copy(), reverseViewport);
        }

        /**
         * Attaches the replicated table's {@link TableUpdateValidator}, but not before the server has acknowledged the
         * full subscription. Until then the replicated table is incomplete: the server is still growing the client's
         * viewport toward the entire table and publishes no updates for the rows it delivers, so a validator attached
         * any earlier would be validating a table whose initial contents it was never given.
         */
        void maybeAttachUpdateValidator() {
            if (replicatedTUV != null || !fullSubscriptionSatisfied) {
                return;
            }
            replicatedTUV = TableUpdateValidator.make(barrageTable);
            replicatedTUVListener = new FailureListener("Replicated Table Update Validator");
            replicatedTUV.getResultTable().addUpdateListener(replicatedTUVListener);
        }

        public void validate(final String msg, QueryTable expected) {
            if (!subscribed) {
                return; // no subscription implies no run implies no data -- so we're valid
            }

            maybeAttachUpdateValidator();

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

            if (transform != null) {
                expected = transform.apply(expected);
                toCheck = transform.apply(toCheck);
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

        void showResult(final String label, final Table table) {
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

        public int pendingMessageCount() {
            return commandQueue.size();
        }

        /**
         * Delivers at most {@code maxMessages} of the queued messages, leaving the rest for a later flush. Callers use
         * this to spread a single server propagation across more than one consumer update graph cycle.
         */
        public void flushEventsToReplicatedTable(final int maxMessages) {
            for (int ii = 0; ii < maxMessages; ++ii) {
                final BarrageMessage msg = commandQueue.poll();
                if (msg == null) {
                    return;
                }
                barrageTable.handleBarrageMessage(msg);
                msg.close();
            }
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

    class RemoteNugget implements EvalNuggetInterface {

        final Supplier<Table> makeTable;

        final QueryTable originalTable;
        @ReferentialIntegrity
        final BarrageMessageProducer barrageMessageProducer;

        @ReferentialIntegrity
        final TableUpdateValidator originalTUV;
        @ReferentialIntegrity
        final FailureListener originalTUVListener;

        final List<RemoteClient> clients = new ArrayList<>();

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

    abstract class TestHelper {
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
            // test for the nested Vector encoding/decoding (though most types are tested
            createNuggetsForTableMaker(() -> sourceTable.groupBy("Sym").sort("Sym"));
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

    class OneProducerPerClient extends TestHelper {
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

    class SharedProducerForAllClients extends TestHelper {
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

    // These test mid-cycle subscription changes and snapshot content
    abstract class SubscriptionChangingHelper extends SharedProducerForAllClients {
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


    // ---- Growing full subscription tests ----

    public static class DummyObserver implements StreamObserver<BarrageMessageWriter.MessageView> {
        volatile boolean completed = false;
        volatile Throwable failure = null;

        final BarrageDataMarshaller marshaller;
        final Queue<BarrageMessage> receivedCommands;
        /**
         * The Arrow {@link MessageHeader} type of every wire message received, in arrival order (e.g.
         * {@link MessageHeader#DictionaryBatch}, {@link MessageHeader#RecordBatch}). Lets tests assert wire-level
         * message-ordering invariants that a lenient reader would otherwise paper over.
         */
        final List<Byte> observedHeaderTypes = new ArrayList<>();

        DummyObserver(final BarrageDataMarshaller marshaller, final Queue<BarrageMessage> receivedCommands) {
            this.marshaller = marshaller;
            this.receivedCommands = receivedCommands;
        }

        /**
         * Extracts the flatbuffer {@link MessageHeader} type from one FlightData-framed wire message, mirroring the
         * header parsing in {@link BarrageMessageReaderImpl}.
         */
        static byte peekHeaderType(final byte[] buf, final int len) throws IOException {
            final CodedInputStream decoder = CodedInputStream.newInstance(buf, 0, len);
            for (int tag = decoder.readTag(); tag != 0; tag = decoder.readTag()) {
                if (tag == BarrageProtoUtil.DATA_HEADER_TAG) {
                    final int size = decoder.readRawVarint32();
                    return Message.getRootAsMessage(ByteBuffer.wrap(decoder.readRawBytes(size))).headerType();
                }
                decoder.skipField(tag);
            }
            return MessageHeader.NONE;
        }

        @Override
        public void onNext(final BarrageMessageWriter.MessageView messageView) {
            try {
                messageView.forEachStream(inputStream -> {
                    try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                        inputStream.drainTo(baos);
                        inputStream.close();
                        observedHeaderTypes.add(peekHeaderType(baos.peekBuffer(), baos.size()));
                        final BarrageMessage message =
                                marshaller.parse(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                        // we skip schema messages, but can't suppress without propagating something...
                        if (message != null) {
                            receivedCommands.add(message);
                        }
                    } catch (final Exception e) {
                        this.failure = e;
                        throw new IllegalStateException("Failed to parse barrage message: ", e);
                    }
                });
            } catch (final Exception e) {
                this.failure = e;
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
