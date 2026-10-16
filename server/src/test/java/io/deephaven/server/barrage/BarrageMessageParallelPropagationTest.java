//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.StringGenerator;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.server.session.SessionService;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Round trips through producers that write each propagation phase to their subscribers on several threads at once: that
 * the writes really do run at the same time, that subscribers sharing a producer's dictionaries each still receive
 * every value they are sent, that a failing subscriber is removed without disturbing the others, and that many
 * subscriptions growing together all receive their snapshots.
 */
public class BarrageMessageParallelPropagationTest extends BarrageMessageRoundTripTestBase {

    private static final int PROPAGATION_THREADS = 4;

    @Override
    protected int propagationThreads() {
        return PROPAGATION_THREADS;
    }

    /**
     * Each subscriber's first message waits until every subscriber is receiving its first message, which can happen
     * only if the producer writes to all of them at once.
     */
    @Test
    public void testWritesToSubscribersRunAtTheSameTime() {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "intCol"},
                new SetGenerator<>("a", "b", "c"), new IntGenerator(0, 100));
        final QueryTable sourceTable = getTable(100, random, columnInfo);
        final BarrageMessageProducer producer = sourceTable.getResult(new BarrageMessageProducer.Operation(scheduler,
                new SessionService.ObfuscatingErrorTransformer(), daggerRoot.getStreamGeneratorFactory(),
                sourceTable, UPDATE_INTERVAL, null, propagationJobSchedulerFactory));

        final CyclicBarrier allReceiving = new CyclicBarrier(PROPAGATION_THREADS);
        final List<RendezvousObserver> observers = new ArrayList<>();
        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();
        for (int si = 0; si < PROPAGATION_THREADS; ++si) {
            final RendezvousObserver observer = new RendezvousObserver(allReceiving);
            observers.add(observer);
            producer.addSubscription(observer, options, null, null, false);
        }
        flushProducerTable();

        final List<Thread> writers = new ArrayList<>();
        for (final RendezvousObserver observer : observers) {
            assertNull("subscriber failed", observer.failure);
            // a schema, then the snapshot
            assertEquals(2, observer.messages);
            writers.add(observer.firstWriter);
        }
        assertEquals("every subscriber's first message was written by a different thread",
                PROPAGATION_THREADS, writers.stream().distinct().count());

        for (final RendezvousObserver observer : observers) {
            producer.removeSubscription(observer);
        }
        flushProducerTable();
    }

    /**
     * Full subscribers share the producer's dictionaries, and every full subscriber's write fills them, so with
     * parallel writes several subscribers add values to one dictionary at once. Each must still receive every value its
     * record batches refer to. The value space is large enough that the dictionaries keep growing, and outgrow the
     * table often enough to be reset, over the run.
     */
    @Test
    public void testManySubscribersWithDictionaryEncodedColumns() {
        final int size = 200;
        final Random random = new Random(1);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "Str", "intCol"},
                new SetGenerator<>("a", "b", "c", "d", "e", "f", "g", "h"),
                new StringGenerator(500),
                new IntGenerator(0, 100));
        final QueryTable sourceTable = getTable(size / 4, random, columnInfo);
        sourceTable.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE,
                dictionaryEncodedSchema(sourceTable.getDefinition(), "Sym", "Str"));

        final BitSet allColumns = new BitSet();
        allColumns.set(0, sourceTable.numColumns());
        final BitSet symAndInt = new BitSet();
        symAndInt.set(0);
        symAndInt.set(2);

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        for (int ci = 0; ci < 8; ++ci) {
            nugget.newClient(null, allColumns, "full-" + ci);
        }
        for (int ci = 0; ci < 4; ++ci) {
            nugget.newClient(null, symAndInt, "full-sym-int-" + ci);
        }
        for (int ci = 0; ci < 6; ++ci) {
            nugget.newClient(RowSetFactory.fromRange(ci * 10L, ci * 10L + 19), allColumns, "viewport-" + ci);
        }
        for (int ci = 0; ci < 4; ++ci) {
            nugget.newClient(RowSetFactory.fromRange(ci * 15L, ci * 15L + 24), allColumns, true, "reverse-" + ci);
        }

        runSteps(nugget, sourceTable, columnInfo, size, random, 40);
    }

    /**
     * A subscriber whose write fails is told so and removed; the others, written at the same time, carry on.
     */
    @Test
    public void testFailedSubscriberIsRemovedAlone() {
        final int size = 200;
        final Random random = new Random(2);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(0, 100),
                new SetGenerator<>(10.1, 20.1, 30.1));
        final QueryTable sourceTable = getTable(size / 4, random, columnInfo);

        final BitSet allColumns = new BitSet();
        allColumns.set(0, sourceTable.numColumns());
        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        for (int ci = 0; ci < 6; ++ci) {
            nugget.newClient(null, allColumns, "full-" + ci);
        }
        for (int ci = 0; ci < 2; ++ci) {
            nugget.newClient(RowSetFactory.fromRange(ci * 20L, ci * 20L + 29), allColumns, "viewport-" + ci);
        }
        runSteps(nugget, sourceTable, columnInfo, size, random, 5);

        // The failing subscriber's replicated table stops following the source once its write fails, so it is no
        // longer validated.
        final RemoteClient failing = nugget.clients.remove(2);
        failing.dummyObserver.failNextMessage = true;
        runSteps(nugget, sourceTable, columnInfo, size, random, 1);

        assertFalse("the failure was injected", failing.dummyObserver.failNextMessage);
        assertEquals("the failed subscriber was told", 1, failing.dummyObserver.errors.size());
        assertEquals(0, failing.pendingMessageCount());

        runSteps(nugget, sourceTable, columnInfo, size, random, 10);
        assertEquals("the failed subscriber was sent nothing more", 0, failing.pendingMessageCount());
        assertEquals(1, failing.dummyObserver.errors.size());
        for (final RemoteClient client : nugget.clients) {
            assertTrue(client.name, client.dummyObserver.errors.isEmpty());
        }
    }

    /**
     * Many subscriptions added in the same cycle grow toward their targets together, so their snapshots are written in
     * parallel; each must end up with exactly its rows.
     */
    @Test
    public void testManyGrowingSubscriptionsAddedTogether() {
        // enough rows that the first snapshots are split into several by the snapshot cell budget
        final int size = 4000;
        final Random random = new Random(3);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(0, 100),
                new SetGenerator<>(10.1, 20.1, 30.1));
        final QueryTable sourceTable = getTable(size, random, columnInfo);

        final BitSet allColumns = new BitSet();
        allColumns.set(0, sourceTable.numColumns());
        final BitSet someColumns = new BitSet();
        someColumns.set(1, sourceTable.numColumns());

        final RemoteNugget nugget = new RemoteNugget(() -> sourceTable);
        for (int ci = 0; ci < 12; ++ci) {
            nugget.newClient(null, ci % 3 == 0 ? someColumns : allColumns, "full-" + ci);
        }
        for (int ci = 0; ci < 6; ++ci) {
            final long first = ci * 500L;
            nugget.newClient(RowSetFactory.fromRange(first, first + 999), allColumns, ci % 2 == 1, "viewport-" + ci);
        }

        runSteps(nugget, sourceTable, columnInfo, size, random, 10);
        for (final RemoteClient client : nugget.clients) {
            if (client.isFullSubscription) {
                assertTrue(client.name + " reached the full table", client.fullSubscriptionSatisfied);
            }
        }
    }

    /** Runs {@code steps} random update cycles, propagating each and validating every client afterwards. */
    private void runSteps(final RemoteNugget nugget, final QueryTable sourceTable, final ColumnInfo<?, ?>[] columnInfo,
            final int size, final Random random, final int steps) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < steps; ++step) {
            updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                    GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
            flushProducerTable();
            nugget.flushClientEvents();
            updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
            nugget.validate("step " + step);
        }
    }

    /**
     * The natural schema of {@code definition}, with each named column dictionary encoded under its own id.
     */
    private static Schema dictionaryEncodedSchema(final TableDefinition definition, final String... columns) {
        final List<String> encoded = List.of(columns);
        final Schema natural =
                BarrageUtil.makeSchema(BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, definition, Map.of(), false);
        final List<Field> fields = natural.getFields().stream().map(field -> {
            final int id = encoded.indexOf(field.getName());
            if (id < 0) {
                return field;
            }
            return new Field(field.getName(),
                    new FieldType(field.isNullable(), field.getType(),
                            new DictionaryEncoding(id, false, new ArrowType.Int(32, true)), field.getMetadata()),
                    field.getChildren());
        }).collect(Collectors.toList());
        return new Schema(fields, natural.getCustomMetadata());
    }

    /** Drains what it is sent; its first message waits at {@code allReceiving} for the other subscribers' first. */
    private static final class RendezvousObserver implements StreamObserver<BarrageMessageWriter.MessageView> {
        private final CyclicBarrier allReceiving;
        private int messages;
        private Thread firstWriter;
        private volatile Throwable failure;

        private RendezvousObserver(final CyclicBarrier allReceiving) {
            this.allReceiving = allReceiving;
        }

        @Override
        public void onNext(final BarrageMessageWriter.MessageView view) {
            if (messages++ == 0) {
                firstWriter = Thread.currentThread();
                try {
                    allReceiving.await(30, TimeUnit.SECONDS);
                } catch (final Exception e) {
                    failure = e;
                    throw new IllegalStateException("the subscribers' writes did not run at the same time", e);
                }
            }
            try {
                view.forEachStream(stream -> {
                    try {
                        stream.drainTo(OutputStream.nullOutputStream());
                        stream.close();
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void onError(final Throwable t) {
            failure = t;
        }

        @Override
        public void onCompleted() {}
    }
}
