//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
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
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Round trips through producers that write each propagation phase to their subscribers on several threads at once: that
 * the writes really do run at the same time, that subscribers sharing a producer's dictionaries each still receive
 * every value they are sent, that a failing subscriber is removed without disturbing the others, that many
 * subscriptions growing together all receive their snapshots, and that viewport subscribers are written before full
 * subscribers.
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

    /**
     * Every viewport subscriber's write finishes before any full subscriber's write begins, in the snapshot phase and
     * in a delta phase, although the full subscriptions are added first and the pool is too busy to take any write,
     * which leaves every write to the propagation job's own thread.
     */
    @Test
    public void testViewportSubscribersAreWrittenFirst() {
        final Random random = new Random(4);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(new String[] {"Sym", "intCol"},
                new SetGenerator<>("a", "b", "c"), new IntGenerator(0, 100));
        // small enough that the snapshot fits in the first growth round, which sends each subscriber one message
        final int size = (int) (BarrageUtil.MIN_SNAPSHOT_CELL_COUNT / columnInfo.length);
        final QueryTable sourceTable = getTable(size, random, columnInfo);
        // queues every job and never starts one, as a pool whose threads are all busy does; the propagation job's
        // thread then runs every queued write itself
        final Executor busyPool = runnable -> {
        };
        final BarrageMessageProducer producer = sourceTable.getResult(new BarrageMessageProducer.Operation(scheduler,
                new SessionService.ObfuscatingErrorTransformer(), daggerRoot.getStreamGeneratorFactory(),
                sourceTable, UPDATE_INTERVAL, null, () -> new ExecutorJobScheduler(busyPool, PROPAGATION_THREADS)));

        final List<String> writes = Collections.synchronizedList(new ArrayList<>());
        final List<RecordingObserver> observers = new ArrayList<>();
        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();
        for (int si = 0; si < 3; ++si) {
            final RecordingObserver observer = new RecordingObserver("full-" + si, writes);
            observers.add(observer);
            producer.addSubscription(observer, options, null, null, false);
        }
        for (int si = 0; si < 3; ++si) {
            final RecordingObserver observer = new RecordingObserver("viewport-" + si, writes);
            observers.add(observer);
            producer.addSubscription(observer, options, null, RowSetFactory.fromRange(si * 10L, si * 10L + 9), false);
        }
        flushProducerTable();
        // a schema, then the snapshot, to each subscriber
        assertEquals("snapshot writes " + writes, 2 * observers.size(), writes.size());
        assertViewportsWrittenFirst("snapshot", writes);

        writes.clear();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                GenerateTableUpdates.DEFAULT_PROFILE, size, random, sourceTable, columnInfo));
        flushProducerTable();
        // the delta, to each subscriber; the identical full subscribers share one write, which sends each record batch
        // as its own message
        for (final RecordingObserver observer : observers) {
            assertTrue("delta writes " + writes, writes.contains(observer.name));
        }
        assertViewportsWrittenFirst("delta", writes);

        for (final RecordingObserver observer : observers) {
            assertNull(observer.name + " failed", observer.failure);
            producer.removeSubscription(observer);
        }
        flushProducerTable();
    }

    /** Asserts that every viewport subscriber's message in {@code writes} comes before every full subscriber's. */
    private static void assertViewportsWrittenFirst(final String phase, final List<String> writes) {
        int lastViewportWrite = -1;
        int firstFullWrite = writes.size();
        for (int wi = 0; wi < writes.size(); ++wi) {
            if (writes.get(wi).startsWith("viewport-")) {
                lastViewportWrite = wi;
            } else {
                firstFullWrite = Math.min(firstFullWrite, wi);
            }
        }
        assertTrue(phase + " writes " + writes, lastViewportWrite < firstFullWrite);
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
            drain(view);
        }

        @Override
        public void onError(final Throwable t) {
            failure = t;
        }

        @Override
        public void onCompleted() {}
    }

    /** Drains what it is sent, adding its name to {@code writes} for each message. */
    private static final class RecordingObserver implements StreamObserver<BarrageMessageWriter.MessageView> {
        private final String name;
        private final List<String> writes;
        private volatile Throwable failure;

        private RecordingObserver(final String name, final List<String> writes) {
            this.name = name;
            this.writes = writes;
        }

        @Override
        public void onNext(final BarrageMessageWriter.MessageView view) {
            writes.add(name);
            drain(view);
        }

        @Override
        public void onError(final Throwable t) {
            failure = t;
        }

        @Override
        public void onCompleted() {}
    }

    /** Reads every stream of {@code view} to its end and closes each one. */
    private static void drain(final BarrageMessageWriter.MessageView view) {
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
}
