//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Measures a {@link BarrageMessageProducer}'s propagation job end to end, from a real refreshing table to {@code
 * subscribers} subscribers: {@code initialSnapshot} sends a full snapshot to that many new subscriptions at once, and
 * {@code tick} sends them one update that modifies {@code rowsPerTick} rows in every column.
 *
 * <p>
 * The producer runs on a {@link TestControlledScheduler}, so a benchmark invocation is exactly one run of the queued
 * propagation work, on the benchmark thread. Each subscriber drains its streams into a sink that copies every buffer
 * into a small scratch array, standing in for the copy a gRPC transport makes into its write buffers.
 *
 * <p>
 * Subscription growth is disabled in the forked JVM so that an initial snapshot is one snapshot and one write per
 * subscriber, rather than a series sized to the update graph's cycle time.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
// Each fork runs measurably faster for its first ten seconds or so and then settles to a steady rate; the warmup is
// long
// enough to measure only the steady state.
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1, jvmArgsAppend = {"-Xms8g", "-Xmx8g", "-DBarrageMessageProducer.subscriptionGrowthEnabled=false"})
public class BarrageMessageProducerPropagateBenchmark {

    private static final long UPDATE_INTERVAL_MS = 1000;
    private static final int SINK_SCRATCH_BYTES = 1 << 20;

    /** Parameters and the table, producer and subscribers that both benchmarks share. */
    @State(Scope.Thread)
    public abstract static class ProducerState {
        /** Number of subscriptions to propagate to. */
        @Param({"1", "4", "8", "16"})
        int subscribers;

        /**
         * Writer threads per propagation phase, counting the job's own thread. Zero builds the producer with the
         * constructor that predates parallel writes, which writes to subscribers in turn.
         */
        @Param({"0", "4", "8"})
        int threads;

        /** {@code full} subscribes everyone to the whole table; {@code mixed} alternates full and viewport. */
        @Param({"full", "mixed"})
        String mix;

        @Param({"200000"})
        int numRows;

        @Param({"50"})
        int numColumns;

        SafeCloseable executionContext;
        SafeCloseable livenessScope;
        ThreadPoolExecutor fanOutPool;
        ControlledUpdateGraph updateGraph;
        TestControlledScheduler scheduler;
        QueryTable table;
        BarrageMessageProducer producer;
        BitSet allColumns;
        final List<Sink> sinks = new ArrayList<>();

        void setupProducer() {
            executionContext = TestExecutionContext.createForUnitTests().open();
            updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.enableUnitTestMode();
            updateGraph.resetForUnitTests(false);
            updateGraph.setSerialTableOperationsSafe(true);
            livenessScope = LivenessScopeStack.open(new LivenessScope(true), true);

            final Random random = new Random(0xB33FCAFEL);
            final ColumnHolder<?>[] columns = new ColumnHolder[numColumns];
            for (int ci = 0; ci < numColumns; ++ci) {
                final long[] values = new long[numRows];
                for (int ri = 0; ri < numRows; ++ri) {
                    values[ri] = random.nextLong();
                }
                columns[ci] = TableTools.longCol("C" + ci, values);
            }
            table = TstUtils.testRefreshingTable(RowSetFactory.flat(numRows).toTracking(), columns);
            allColumns = new BitSet(numColumns);
            allColumns.set(0, numColumns);

            scheduler = new TestControlledScheduler();
            producer = table.getResult(makeOperation());
        }

        private BarrageMessageProducer.Operation makeOperation() {
            final BarrageMessageWriter.Factory writerFactory = new BarrageMessageWriterImpl.Factory();
            final SessionService.ErrorTransformer errorTransformer =
                    new SessionService.ObfuscatingErrorTransformer();
            if (threads == 0) {
                // the constructor every producer used before writes could run in parallel
                return new BarrageMessageProducer.Operation(scheduler, errorTransformer, writerFactory, table,
                        UPDATE_INTERVAL_MS);
            }
            final Supplier<JobScheduler> propagationJobSchedulerFactory;
            if (threads == 1) {
                propagationJobSchedulerFactory = BarrageMessageProducer.SEQUENTIAL_PROPAGATION;
            } else {
                // the pool the server makes, but one this benchmark can shut down
                final AtomicInteger threadCount = new AtomicInteger();
                fanOutPool = ExecutorJobScheduler.newHelperPool(threads - 1, runnable -> {
                    final Thread thread = new Thread(() -> {
                        MultiChunkPool.enableDedicatedPoolForThisThread();
                        runnable.run();
                    }, "propagation-" + threadCount.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
                final ThreadPoolExecutor pool = fanOutPool;
                final int threadCountForScheduler = threads;
                propagationJobSchedulerFactory = () -> new ExecutorJobScheduler(pool, threadCountForScheduler);
            }
            return new BarrageMessageProducer.Operation(scheduler, errorTransformer, writerFactory, table,
                    UPDATE_INTERVAL_MS, null, propagationJobSchedulerFactory);
        }

        /** Subscribes {@code subscribers} new sinks; the next run of the scheduler sends their initial snapshots. */
        void subscribe() {
            final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();
            final long width = Math.max(1, numRows / 10);
            for (int si = 0; si < subscribers; ++si) {
                final Sink sink = new Sink();
                sinks.add(sink);
                final boolean viewport = mix.equals("mixed") && si % 2 == 1;
                RowSet vp = null;
                if (viewport) {
                    final long start = (si * (long) numRows / subscribers) % (numRows - width + 1);
                    vp = RowSetFactory.fromRange(start, start + width - 1);
                }
                producer.addSubscription(sink, options, allColumns, vp, false);
            }
        }

        /** Removes every subscription and lets the producer complete them. */
        void unsubscribe() {
            for (final Sink sink : sinks) {
                producer.removeSubscription(sink);
            }
            scheduler.runUntilQueueEmpty();
            for (final Sink sink : sinks) {
                if (sink.failure != null) {
                    throw new IllegalStateException("a subscriber failed", sink.failure);
                }
            }
            sinks.clear();
        }

        void tearDownProducer() {
            livenessScope.close();
            if (fanOutPool != null) {
                fanOutPool.shutdownNow();
            }
            executionContext.close();
        }
    }

    /** Each invocation sends initial snapshots to {@code subscribers} subscriptions added just before it. */
    public static class SnapshotState extends ProducerState {
        @Setup(Level.Trial)
        public void setupTrial() {
            setupProducer();
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            subscribe();
        }

        @TearDown(Level.Invocation)
        public void tearDownInvocation() {
            unsubscribe();
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() {
            tearDownProducer();
        }
    }

    /** Each invocation propagates one update that modifies {@code rowsPerTick} rows of every column. */
    public static class TickState extends ProducerState {
        @Param({"20000"})
        int rowsPerTick;

        private ColumnHolder<?>[] modifiedValues;
        private long nextModifiedRow;

        @Setup(Level.Trial)
        public void setupTrial() {
            setupProducer();
            subscribe();
            scheduler.runUntilQueueEmpty();
            // the initial snapshots are not part of what a tick measures
            takeBytes(this);

            final Random random = new Random(0xFEEDL);
            modifiedValues = new ColumnHolder[numColumns];
            for (int ci = 0; ci < numColumns; ++ci) {
                final long[] values = new long[rowsPerTick];
                for (int ri = 0; ri < rowsPerTick; ++ri) {
                    values[ri] = random.nextLong();
                }
                modifiedValues[ci] = TableTools.longCol("C" + ci, values);
            }
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            final long first = nextModifiedRow;
            nextModifiedRow = (nextModifiedRow + rowsPerTick) % (numRows - rowsPerTick + 1);
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet modified = RowSetFactory.fromRange(first, first + rowsPerTick - 1);
                TstUtils.addToTable(table, modified, modifiedValues);
                table.notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(), modified);
            });
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() {
            unsubscribe();
            tearDownProducer();
        }
    }

    @Benchmark
    public long initialSnapshot(final SnapshotState state) {
        state.scheduler.runUntilQueueEmpty();
        return takeBytes(state);
    }

    @Benchmark
    public long tick(final TickState state) {
        state.scheduler.runUntilQueueEmpty();
        return takeBytes(state);
    }

    private static long takeBytes(final ProducerState state) {
        long bytes = 0;
        for (final Sink sink : state.sinks) {
            if (sink.failure != null) {
                throw new IllegalStateException("a subscriber failed", sink.failure);
            }
            bytes += sink.out.takeBytes();
        }
        return bytes;
    }

    /** A subscriber that drains every message it is sent. */
    static final class Sink implements StreamObserver<BarrageMessageWriter.MessageView> {
        final CopyingOutputStream out = new CopyingOutputStream();
        volatile Throwable failure;

        @Override
        public void onNext(final BarrageMessageWriter.MessageView view) {
            try {
                view.forEachStream(stream -> {
                    try {
                        stream.drainTo(out);
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

    /** Copies every buffer it is given into a scratch array, wrapping around, and counts the bytes. */
    static final class CopyingOutputStream extends OutputStream {
        private final byte[] scratch = new byte[SINK_SCRATCH_BYTES];
        private int position;
        private long bytes;

        @Override
        public void write(final int b) {
            scratch[position] = (byte) b;
            position = (position + 1) % scratch.length;
            ++bytes;
        }

        @Override
        public void write(@NotNull final byte[] buffer, int offset, int length) {
            bytes += length;
            while (length > 0) {
                final int toCopy = Math.min(length, scratch.length - position);
                System.arraycopy(buffer, offset, scratch, position, toCopy);
                position = (position + toCopy) % scratch.length;
                offset += toCopy;
                length -= toCopy;
            }
        }

        long takeBytes() {
            final long result = bytes;
            bytes = 0;
            return result;
        }
    }
}
