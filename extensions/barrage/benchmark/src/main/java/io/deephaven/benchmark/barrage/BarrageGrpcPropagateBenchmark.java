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
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
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
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.PassthroughInputStreamMarshaller;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.openjdk.jmh.annotations.AuxCounters;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Measures what a {@link BarrageMessageProducer} spends to supply its subscribers, end to end and through a real gRPC
 * transport: from a 30-column refreshing table of mixed types, through the producer's propagation job, into the Netty
 * transport's buffers, and on over the loopback interface to {@code subscribers} gRPC clients that drain what they
 * receive.
 *
 * <p>
 * {@code snapshot} sends an initial snapshot of the whole table, of {@code rows} rows, to {@code subscribers} new
 * subscriptions at once; {@code tick} sends them one update that modifies {@code deltaRows} rows of every column. Each
 * invocation is one complete propagation job, taking the snapshot or materializing the delta, building the message
 * writer, and writing every subscriber's view of the message, where a write serializes the record batches into the gRPC
 * transport's buffers exactly as the server's Flight service does, on the writing thread; and then the wait until every
 * client has received the bytes. That whole span is the benchmark's time. Its parts are reported beside it, as totals
 * over the measurement with the number of {@code invocations}, so that the share of the work the parallel writes can
 * shorten is visible against the rest (divide each total by {@code invocations} for the mean per invocation):
 * </p>
 * <ul>
 * <li>{@code jobMillisTotal}: the propagation job alone, from its start to its return, which is when the producer's
 * thread is free again. The writes' serialization into transport buffers is inside this; the transport's sending is
 * not.</li>
 * <li>{@code prewriteMillisTotal}: from the job's start to the first subscriber write, which is the snapshot or the
 * delta's materialization into a message, and the writer's construction.</li>
 * <li>{@code writeMillisTotal}: from the first subscriber write's start to the last one's end, the fan-out that
 * {@code threads} spreads over threads.</li>
 * <li>{@code recordMillisTotal} ({@code tick} only): what the update cycle that produced the delta spent beyond
 * modifying the table, which is the producer's listener recording the delta. It runs before the job, outside the
 * measured span, and is reported so the full cost of supplying the update can be summed.</li>
 * <li>{@code megabytesTotal}: the message payload written to the transport, over all subscribers.</li>
 * </ul>
 *
 * <p>
 * The table's 30 columns are six {@code long}, five {@code int}, four {@code double}, three {@code short}, two
 * {@code byte}, two {@code char}, two {@code float}, one {@code Boolean}, two {@code Instant}, a {@code String} column
 * drawn from a pool of {@value #STR_POOL_SIZE} values, a {@code String} column of {@value #SYM_POOL_SIZE} symbols that
 * is dictionary encoded, and an {@code int} column whose value changes every {@value #RLE_RUN_LENGTH} rows that is
 * run-end encoded. The two encodings are requested through the table's {@link Table#BARRAGE_SCHEMA_ATTRIBUTE}, as a
 * user would request them. Subscriptions use the default options, under which a snapshot is a single record batch and
 * so a single gRPC message, as it is for the Java and Python clients.
 * </p>
 *
 * <p>
 * Memory: the transport holds an invocation's serialized messages until the clients have drained them, on both sides of
 * the loopback, in direct memory. Sixteen subscribers to a five million row snapshot hold about 12 GB per side at the
 * peak. The fork is sized for that; a smaller machine should run fewer subscribers or rows.
 * </p>
 *
 * <p>
 * The producer runs on a {@link TestControlledScheduler}, so an invocation is exactly one run of the queued propagation
 * work, on the benchmark thread. Subscription growth is disabled in the forked JVM so that an initial snapshot is one
 * snapshot and one write per subscriber.
 * </p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(value = 1, jvmArgsAppend = {"-Xms12g", "-Xmx12g", "-XX:MaxDirectMemorySize=32g",
        "-DBarrageMessageProducer.subscriptionGrowthEnabled=false"})
public class BarrageGrpcPropagateBenchmark {

    private static final long UPDATE_INTERVAL_MS = 1000;
    /** How long to wait for the clients to receive an invocation's messages. */
    private static final long DELIVERY_TIMEOUT_SECONDS = 600;
    /** How long to wait for a stream to open or close, which takes a round trip. */
    private static final long STREAM_TIMEOUT_SECONDS = 30;

    static final String STR_COLUMN = "Str";
    static final String SYM_COLUMN = "Sym";
    static final String RLE_COLUMN = "Rle";
    static final int STR_POOL_SIZE = 1024;
    static final int SYM_POOL_SIZE = 4096;
    static final int RLE_RUN_LENGTH = 1000;
    private static final int NUM_COLUMNS = 30;
    private static final long EPOCH_NANOS = 1_700_000_000_000_000_000L;

    private static final String SERVICE_NAME = "io.deephaven.benchmark.barrage.BarrageSink";
    private static final String FULL_METHOD_NAME = MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Subscribe");

    /** The sink service's one method, as the server sees it: the server writes Barrage messages to the stream. */
    private static final MethodDescriptor<InputStream, InputStream> SERVER_METHOD =
            MethodDescriptor.<InputStream, InputStream>newBuilder()
                    .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                    .setFullMethodName(FULL_METHOD_NAME)
                    .setRequestMarshaller(PassthroughInputStreamMarshaller.INSTANCE)
                    .setResponseMarshaller(PassthroughInputStreamMarshaller.INSTANCE)
                    .build();

    /** The same method as the client sees it: each message is drained and counted rather than parsed. */
    private static final MethodDescriptor<InputStream, Long> CLIENT_METHOD =
            MethodDescriptor.<InputStream, Long>newBuilder()
                    .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                    .setFullMethodName(FULL_METHOD_NAME)
                    .setRequestMarshaller(PassthroughInputStreamMarshaller.INSTANCE)
                    .setResponseMarshaller(DrainingMarshaller.INSTANCE)
                    .build();

    /** Drains a message and returns how many bytes it held: the cost of a client that discards what it is sent. */
    private enum DrainingMarshaller implements MethodDescriptor.Marshaller<Long> {
        INSTANCE;

        private static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[1 << 20]);

        @Override
        public InputStream stream(final Long value) {
            throw new UnsupportedOperationException("clients receive, they do not send");
        }

        @Override
        public Long parse(final InputStream stream) {
            final byte[] scratch = SCRATCH.get();
            long total = 0;
            try {
                int read;
                while ((read = stream.read(scratch)) >= 0) {
                    total += read;
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
            return total;
        }
    }

    /**
     * The parts of each invocation, reported beside its time as totals over the iteration together with the number of
     * invocations, because JMH sums an event counter over the measurement iterations: dividing a total by
     * {@code invocations} gives the mean per invocation over the whole measurement. The units are in the names, as JMH
     * reports an event counter as a bare number.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class Counters {
        public long invocations;
        public double jobMillisTotal;
        public double prewriteMillisTotal;
        public double writeMillisTotal;
        public double recordMillisTotal;
        public double megabytesTotal;

        @Setup(Level.Iteration)
        public void reset() {
            invocations = 0;
            jobMillisTotal = prewriteMillisTotal = writeMillisTotal = recordMillisTotal = megabytesTotal = 0;
        }

        void record(final double job, final double prewrite, final double write, final double record,
                final double megabytes) {
            ++invocations;
            jobMillisTotal += job;
            prewriteMillisTotal += prewrite;
            writeMillisTotal += write;
            recordMillisTotal += record;
            megabytesTotal += megabytes;
        }
    }

    /** Parameters and the table, producer, transport and subscribers that both benchmarks share. */
    @State(Scope.Thread)
    public abstract static class ProducerState {
        /** Number of subscriptions to propagate to. */
        @Param({"1", "2", "3", "4", "8", "16"})
        int subscribers;

        /** Writer threads per propagation phase, counting the job's own thread; one writes to subscribers in turn. */
        @Param({"1", "8"})
        int threads;

        /** {@code full} subscribes everyone to the whole table; {@code mixed} alternates full and viewport. */
        @Param({"full"})
        String mix;

        SafeCloseable executionContext;
        SafeCloseable livenessScope;
        ThreadPoolExecutor helperPool;
        ControlledUpdateGraph updateGraph;
        TestControlledScheduler scheduler;
        QueryTable table;
        BarrageMessageProducer producer;
        BitSet allColumns;
        String[] strPool;
        String[] symPool;

        Server server;
        final BlockingQueue<StreamObserver<InputStream>> openedStreams = new LinkedBlockingQueue<>();
        final List<Client> clients = new ArrayList<>();

        /** The first write's start and the last write's end, over all subscribers, of the current invocation. */
        final AtomicLong firstWriteStartNanos = new AtomicLong(Long.MAX_VALUE);
        final AtomicLong lastWriteEndNanos = new AtomicLong(Long.MIN_VALUE);
        /** What the update cycle before the current invocation spent recording the delta, for {@code tick}. */
        long recordNanos;

        /** The table's row count. */
        abstract int rows();

        void setupProducer() throws IOException {
            executionContext = TestExecutionContext.createForUnitTests().open();
            updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.enableUnitTestMode();
            updateGraph.resetForUnitTests(false);
            updateGraph.setSerialTableOperationsSafe(true);
            livenessScope = LivenessScopeStack.open(new LivenessScope(true), true);

            final Random random = new Random(0xB33FCAFEL);
            strPool = makePool(random, STR_POOL_SIZE, 8, 24);
            symPool = makePool(random, SYM_POOL_SIZE, 2, 5);
            final ColumnHolder<?>[] columns = makeColumns(rows(), random, strPool, symPool, 0);
            table = TstUtils.testRefreshingTable(RowSetFactory.flat(rows()).toTracking(), columns);
            table.setAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE, encodedSchema(table.getDefinition()));
            allColumns = new BitSet(NUM_COLUMNS);
            allColumns.set(0, NUM_COLUMNS);

            scheduler = new TestControlledScheduler();
            producer = table.getResult(makeOperation());

            // The sink: a real gRPC server on the loopback interface, built as the server's Netty transport is
            server = NettyServerBuilder.forPort(0)
                    .directExecutor()
                    .addService(ServerServiceDefinition.builder(SERVICE_NAME)
                            .addMethod(SERVER_METHOD, ServerCalls.asyncBidiStreamingCall(responseObserver -> {
                                openedStreams.add(responseObserver);
                                return new StreamObserver<>() {
                                    @Override
                                    public void onNext(final InputStream request) {}

                                    @Override
                                    public void onError(final Throwable t) {}

                                    @Override
                                    public void onCompleted() {}
                                };
                            }))
                            .build())
                    .build()
                    .start();
            for (int si = 0; si < subscribers; ++si) {
                clients.add(new Client(NettyChannelBuilder.forAddress("127.0.0.1", server.getPort())
                        .usePlaintext()
                        .maxInboundMessageSize(Integer.MAX_VALUE)
                        .build()));
            }
        }

        private BarrageMessageProducer.Operation makeOperation() {
            final BarrageMessageWriter.Factory writerFactory = new BarrageMessageWriterImpl.Factory();
            final SessionService.ErrorTransformer errorTransformer =
                    new SessionService.ObfuscatingErrorTransformer();
            final Supplier<JobScheduler> propagationJobSchedulerFactory;
            if (threads <= 1) {
                propagationJobSchedulerFactory = BarrageMessageProducer.SEQUENTIAL_PROPAGATION;
            } else {
                // the pool the server makes, but one this benchmark can shut down
                final AtomicInteger threadCount = new AtomicInteger();
                helperPool = ExecutorJobScheduler.newHelperPool(threads - 1, runnable -> {
                    final Thread thread = new Thread(() -> {
                        MultiChunkPool.enableDedicatedPoolForThisThread();
                        runnable.run();
                    }, "propagation-" + threadCount.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
                final ThreadPoolExecutor pool = helperPool;
                final int threadCountForScheduler = threads;
                propagationJobSchedulerFactory = () -> new ExecutorJobScheduler(pool, threadCountForScheduler);
            }
            return new BarrageMessageProducer.Operation(scheduler, errorTransformer, writerFactory, table,
                    UPDATE_INTERVAL_MS, null, propagationJobSchedulerFactory);
        }

        /** Opens a gRPC stream for each client and subscribes it; the next run of the scheduler sends snapshots. */
        void subscribe() throws InterruptedException {
            final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();
            final long width = Math.max(1, rows() / 10);
            for (int si = 0; si < subscribers; ++si) {
                final Client client = clients.get(si);
                client.open();
                final boolean viewport = mix.equals("mixed") && si % 2 == 1;
                RowSet vp = null;
                if (viewport) {
                    final long start = (si * (long) rows() / subscribers) % (rows() - width + 1);
                    vp = RowSetFactory.fromRange(start, start + width - 1);
                }
                producer.addSubscription(client.subscription, options, allColumns, vp, false);
            }
        }

        /** Removes every subscription, lets the producer complete the streams, and waits for the clients to see it. */
        void unsubscribe() throws InterruptedException {
            for (final Client client : clients) {
                producer.removeSubscription(client.subscription);
            }
            scheduler.runUntilQueueEmpty();
            for (final Client client : clients) {
                // A run of the producer that has nothing to propagate returns before completing the subscriptions it
                // removed, so the stream may still be open; close it here in that case so that the client sees its end.
                client.subscription.completeIfOpen();
                client.awaitClosed();
            }
        }

        void beginInvocation() {
            firstWriteStartNanos.set(Long.MAX_VALUE);
            lastWriteEndNanos.set(Long.MIN_VALUE);
            for (final Client client : clients) {
                client.resetBytes();
            }
        }

        /**
         * Runs the queued propagation job, waits until every client has received what it wrote, and records the parts.
         *
         * @return the bytes the clients received
         */
        long runJob(final Counters counters) {
            final long jobStart = System.nanoTime();
            scheduler.runUntilQueueEmpty();
            final long jobEnd = System.nanoTime();
            long written = 0;
            long received = 0;
            for (final Client client : clients) {
                written += client.subscription.written.get();
                received += client.awaitDelivery();
            }
            final boolean wrote = firstWriteStartNanos.get() != Long.MAX_VALUE;
            counters.record(
                    (jobEnd - jobStart) / 1e6,
                    wrote ? (firstWriteStartNanos.get() - jobStart) / 1e6 : 0,
                    wrote ? (lastWriteEndNanos.get() - firstWriteStartNanos.get()) / 1e6 : 0,
                    recordNanos / 1e6,
                    written / 1e6);
            recordNanos = 0;
            return received;
        }

        void tearDownProducer() throws InterruptedException {
            for (final Client client : clients) {
                client.channel.shutdownNow();
            }
            for (final Client client : clients) {
                client.channel.awaitTermination(STREAM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            clients.clear();
            if (server != null) {
                server.shutdownNow();
                server.awaitTermination(STREAM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            livenessScope.close();
            if (helperPool != null) {
                helperPool.shutdownNow();
                if (!helperPool.awaitTermination(STREAM_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the helper pool did not terminate");
                }
            }
            executionContext.close();
        }

        /**
         * One client: its channel, which lasts the trial, and its current stream, which lasts a subscription. The
         * producer writes to the stream's server end through {@link Subscription}, and this end drains and counts.
         */
        final class Client implements StreamObserver<Long> {
            final ManagedChannel channel;
            final AtomicLong received = new AtomicLong();
            Subscription subscription;
            volatile CountDownLatch closed;
            volatile Throwable failure;

            Client(final ManagedChannel channel) {
                this.channel = channel;
            }

            /** Starts a stream, and pairs it with the server end the service hands over. */
            void open() throws InterruptedException {
                closed = new CountDownLatch(1);
                final StreamObserver<InputStream> requests =
                        ClientCalls.asyncBidiStreamingCall(channel.newCall(CLIENT_METHOD, CallOptions.DEFAULT), this);
                // one message, so that the headers reach the server and it opens its end
                requests.onNext(new ByteArrayInputStream(new byte[0]));
                final StreamObserver<InputStream> serverEnd =
                        openedStreams.poll(STREAM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (serverEnd == null) {
                    throw new IllegalStateException("the sink did not open a stream");
                }
                subscription = new Subscription(serverEnd);
            }

            void resetBytes() {
                received.set(0);
                if (subscription != null) {
                    subscription.written.set(0);
                }
            }

            /** Waits until this client has received every byte its subscription wrote, and returns the count. */
            long awaitDelivery() {
                final long expected = subscription.written.get();
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DELIVERY_TIMEOUT_SECONDS);
                long got;
                while ((got = received.get()) < expected) {
                    if (failure != null) {
                        throw new IllegalStateException("a client failed", failure);
                    }
                    if (System.nanoTime() > deadline) {
                        throw new IllegalStateException(
                                "delivery timed out: received " + got + " of " + expected + " bytes");
                    }
                    LockSupport.parkNanos(20_000);
                }
                if (failure != null) {
                    throw new IllegalStateException("a client failed", failure);
                }
                return got;
            }

            void awaitClosed() throws InterruptedException {
                if (!closed.await(STREAM_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the stream did not close");
                }
                if (failure != null) {
                    throw new IllegalStateException("a client failed", failure);
                }
            }

            @Override
            public void onNext(final Long bytes) {
                received.addAndGet(bytes);
            }

            @Override
            public void onError(final Throwable t) {
                failure = t;
                closed.countDown();
            }

            @Override
            public void onCompleted() {
                closed.countDown();
            }
        }

        /**
         * The producer's view of one subscription. Each message's streams are handed to the gRPC server end as the
         * server's Flight service hands them, which serializes them into the transport's buffers on this thread.
         */
        final class Subscription implements StreamObserver<BarrageMessageWriter.MessageView> {
            private final StreamObserver<InputStream> serverEnd;
            final AtomicLong written = new AtomicLong();
            private boolean completed;

            Subscription(final StreamObserver<InputStream> serverEnd) {
                this.serverEnd = serverEnd;
            }

            @Override
            public void onNext(final BarrageMessageWriter.MessageView view) {
                firstWriteStartNanos.accumulateAndGet(System.nanoTime(), Math::min);
                synchronized (serverEnd) {
                    try {
                        view.forEachStream(stream -> {
                            try {
                                written.addAndGet(stream.available());
                            } catch (final IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            serverEnd.onNext(stream);
                        });
                    } catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                lastWriteEndNanos.accumulateAndGet(System.nanoTime(), Math::max);
            }

            @Override
            public void onError(final Throwable t) {
                synchronized (serverEnd) {
                    if (!completed) {
                        completed = true;
                        serverEnd.onError(t);
                    }
                }
            }

            @Override
            public void onCompleted() {
                completeIfOpen();
            }

            void completeIfOpen() {
                synchronized (serverEnd) {
                    if (!completed) {
                        completed = true;
                        serverEnd.onCompleted();
                    }
                }
            }
        }
    }

    /** Each invocation sends initial snapshots to {@code subscribers} subscriptions added just before it. */
    public static class SnapshotState extends ProducerState {
        /** The table's rows, all of which the snapshot carries. */
        @Param({"1000000", "5000000"})
        int rows;

        @Override
        int rows() {
            return rows;
        }

        @Setup(Level.Trial)
        public void setupTrial() throws IOException {
            setupProducer();
        }

        @Setup(Level.Invocation)
        public void setupInvocation() throws InterruptedException {
            subscribe();
            beginInvocation();
        }

        @TearDown(Level.Invocation)
        public void tearDownInvocation() throws InterruptedException {
            unsubscribe();
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() throws InterruptedException {
            tearDownProducer();
        }
    }

    /** Each invocation propagates one update that modifies {@code deltaRows} rows of every column. */
    public static class TickState extends ProducerState {
        /** The table's rows. */
        @Param({"5000000"})
        int rows;

        /** The rows each update modifies, in every column. */
        @Param({"100000", "1000000"})
        int deltaRows;

        private ColumnHolder<?>[] modifiedValues;
        private long nextModifiedRow;

        @Override
        int rows() {
            return rows;
        }

        @Setup(Level.Trial)
        public void setupTrial() throws IOException, InterruptedException {
            setupProducer();
            beginInvocation();
            subscribe();
            scheduler.runUntilQueueEmpty();
            // the initial snapshots are not part of what a tick measures
            for (final Client client : clients) {
                client.awaitDelivery();
            }
            // the modified values keep the run-end encoded column's runs, at values the table does not hold
            modifiedValues = makeColumns(deltaRows, new Random(0xFEEDL), strPool, symPool, rows + RLE_RUN_LENGTH);
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            beginInvocation();
            final long first = nextModifiedRow;
            nextModifiedRow = (nextModifiedRow + deltaRows) % (rows - deltaRows + 1);
            final long[] modifyNanos = new long[1];
            final long cycleStart = System.nanoTime();
            updateGraph.runWithinUnitTestCycle(() -> {
                final long modifyStart = System.nanoTime();
                final RowSet modified = RowSetFactory.fromRange(first, first + deltaRows - 1);
                TstUtils.addToTable(table, modified, modifiedValues);
                modifyNanos[0] = System.nanoTime() - modifyStart;
                table.notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(), modified);
            });
            // the cycle's time beyond the modification is the producer's listener recording the delta
            recordNanos = System.nanoTime() - cycleStart - modifyNanos[0];
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() throws InterruptedException {
            unsubscribe();
            tearDownProducer();
        }
    }

    @Benchmark
    public long snapshot(final SnapshotState state, final Counters counters) {
        return state.runJob(counters);
    }

    @Benchmark
    public long tick(final TickState state, final Counters counters) {
        return state.runJob(counters);
    }

    /** {@code size} distinct strings of {@code minLength} to {@code maxLength} letters. */
    static String[] makePool(final Random random, final int size, final int minLength, final int maxLength) {
        final Set<String> pool = new LinkedHashSet<>(size * 2);
        while (pool.size() < size) {
            final int length = minLength + random.nextInt(maxLength - minLength + 1);
            final char[] chars = new char[length];
            for (int ci = 0; ci < length; ++ci) {
                chars[ci] = (char) ('A' + random.nextInt(26));
            }
            pool.add(new String(chars));
        }
        return pool.toArray(new String[0]);
    }

    /**
     * The 30 columns, of {@code rows} rows of values drawn from {@code random}. The run-end encoded column's value
     * changes every {@value #RLE_RUN_LENGTH} rows, counting from {@code rleBase}.
     */
    static ColumnHolder<?>[] makeColumns(
            final int rows,
            final Random random,
            final String[] strPool,
            final String[] symPool,
            final long rleBase) {
        final List<ColumnHolder<?>> columns = new ArrayList<>(NUM_COLUMNS);
        for (int ci = 0; ci < 6; ++ci) {
            final long[] values = new long[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = random.nextLong();
            }
            columns.add(TableTools.longCol("L" + ci, values));
        }
        for (int ci = 0; ci < 5; ++ci) {
            final int[] values = new int[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = random.nextInt();
            }
            columns.add(TableTools.intCol("I" + ci, values));
        }
        for (int ci = 0; ci < 4; ++ci) {
            final double[] values = new double[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = random.nextDouble();
            }
            columns.add(TableTools.doubleCol("D" + ci, values));
        }
        for (int ci = 0; ci < 3; ++ci) {
            final short[] values = new short[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = (short) random.nextInt();
            }
            columns.add(TableTools.shortCol("S" + ci, values));
        }
        for (int ci = 0; ci < 2; ++ci) {
            final byte[] values = new byte[rows];
            random.nextBytes(values);
            columns.add(TableTools.byteCol("B" + ci, values));
        }
        for (int ci = 0; ci < 2; ++ci) {
            final char[] values = new char[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = (char) ('A' + random.nextInt(26));
            }
            columns.add(TableTools.charCol("C" + ci, values));
        }
        for (int ci = 0; ci < 2; ++ci) {
            final float[] values = new float[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = random.nextFloat();
            }
            columns.add(TableTools.floatCol("F" + ci, values));
        }
        {
            final byte[] values = new byte[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = (byte) random.nextInt(2);
            }
            columns.add(ColumnHolder.getBooleanColumnHolder("Bool", false, values));
        }
        for (int ci = 0; ci < 2; ++ci) {
            final long[] nanos = new long[rows];
            for (int ri = 0; ri < rows; ++ri) {
                nanos[ri] = EPOCH_NANOS + random.nextInt(1_000_000_000) * 1_000L;
            }
            columns.add(ColumnHolder.getInstantColumnHolder("T" + ci, false, nanos));
        }
        {
            final String[] values = new String[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = strPool[random.nextInt(strPool.length)];
            }
            columns.add(TableTools.stringCol(STR_COLUMN, values));
        }
        {
            final String[] values = new String[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = symPool[random.nextInt(symPool.length)];
            }
            columns.add(TableTools.stringCol(SYM_COLUMN, values));
        }
        {
            final int[] values = new int[rows];
            for (int ri = 0; ri < rows; ++ri) {
                values[ri] = (int) ((rleBase + ri) / RLE_RUN_LENGTH);
            }
            columns.add(TableTools.intCol(RLE_COLUMN, values));
        }
        if (columns.size() != NUM_COLUMNS) {
            throw new IllegalStateException("expected " + NUM_COLUMNS + " columns, made " + columns.size());
        }
        return columns.toArray(new ColumnHolder[0]);
    }

    /**
     * The table's natural Barrage schema with {@value #SYM_COLUMN} dictionary encoded and {@value #RLE_COLUMN} run-end
     * encoded, both with 32-bit indices; what a user sets as {@link Table#BARRAGE_SCHEMA_ATTRIBUTE} to ask for them.
     */
    static Schema encodedSchema(final TableDefinition definition) {
        final Schema natural =
                BarrageUtil.makeSchema(BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, definition, Map.of(), false);
        final ArrowType int32 = new ArrowType.Int(32, true);
        final List<Field> fields = natural.getFields().stream().map(field -> {
            switch (field.getName()) {
                case SYM_COLUMN:
                    return new Field(field.getName(),
                            new FieldType(field.isNullable(), field.getType(),
                                    new DictionaryEncoding(0, false, (ArrowType.Int) int32), field.getMetadata()),
                            field.getChildren());
                case RLE_COLUMN:
                    return new Field(field.getName(),
                            new FieldType(false, new ArrowType.RunEndEncoded(), null, field.getMetadata()),
                            List.of(
                                    new Field("run_ends", new FieldType(false, int32, null), List.of()),
                                    new Field("values",
                                            new FieldType(field.isNullable(), field.getType(), null,
                                                    field.getMetadata()),
                                            field.getChildren())));
                default:
                    return field;
            }
        }).collect(Collectors.toList());
        return new Schema(fields, natural.getCustomMetadata());
    }
}
