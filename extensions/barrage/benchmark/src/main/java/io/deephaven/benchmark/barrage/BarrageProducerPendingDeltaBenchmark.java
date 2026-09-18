//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
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
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates the memory behavior behind DH-21949: a {@link BarrageMessageProducer} records one {@code Delta} per
 * update graph cycle and keeps every one of them until the subscriber's update interval elapses. Nothing is coalesced
 * until the propagation job runs, so the memory a producer holds grows with the number of update graph cycles per
 * Barrage propagation rather than with the number of distinct cells that changed.
 *
 * <p>
 * The producer is driven exactly the way its unit tests drive it: a {@link ControlledUpdateGraph} runs one cycle per
 * call and a {@link TestControlledScheduler} runs the propagation job only when asked. Each benchmark invocation runs
 * {@code cyclesPerFlush} update graph cycles and then flushes the producer once, which is what a client with a long
 * update interval experiences.
 *
 * <p>
 * Three things are measured:
 * <ul>
 * <li>{@link #enqueueCycles}: the update graph-side cost of recording {@code cyclesPerFlush} deltas. The source data is
 * written once during setup and never rewritten, so the measured region is the update graph cycle plus the producer's
 * recording of it, not the cost of producing the source data. Run it with {@code subscriberMix=NONE} as a control: with
 * no subscriptions the producer records nothing, so that arm is the residual update graph overhead and the difference
 * between the arms is the producer's share.</li>
 * <li>{@link #flushPendingDeltas}: the propagation-side cost of coalescing those deltas into one message and
 * serializing it. Its aux counters report how much the producer was holding right before the flush: the number of
 * queued deltas, the chunk storage they own, the used-heap growth over the post-setup baseline after a GC, and how many
 * bytes actually went over the wire.</li>
 * <li>{@link #cyclesThenFlush}: the two compaction strategies head to head over the same run of cycles. ACCUMULATE is
 * today's behaviour, one coalescing at flush time; COMPACT_EVERY_CYCLE folds everything pending into one delta after
 * every cycle. The score is the total time for all cycles, compactions and the flush, and {@code peakPendingKiB} is the
 * most the producer held at any moment, including the transient during a compaction when the originals and their
 * replacement coexist. Run the {@code subscriberMix=NONE} arm alongside for the harness floor to subtract.</li>
 * <li>{@link #joinWithPendingDeltas}: the cost of a subscriber joining while deltas are queued. Joining schedules the
 * propagation job immediately, so this measures a snapshot plus the pre/post-snapshot split of the pending queue. It is
 * the path proactive coalescing must not regress, since a compacted delta may never merge across the snapshot
 * step.</li>
 * </ul>
 *
 * <p>
 * The update patterns bracket the opportunity for early coalescing. {@code MODIFY_SAME} re-modifies the same rows every
 * cycle, so a coalesced result is one cycle's worth of data no matter how many cycles elapsed; {@code MODIFY_ROTATE}
 * walks disjoint blocks of rows and only repeats once it wraps the table; {@code ROLLING} appends a block of rows and
 * removes the oldest block each cycle, so most adds are dead by the time the flush happens; {@code APPEND_ONLY} only
 * ever adds, so nothing is dead at flush time and compaction has nothing to remove. {@code ROLLING} combined with a
 * viewport subscriber is also what exercises the producer's "scoped adds" path, where rows are recorded because they
 * moved into a position-space viewport rather than because they were added upstream.
 *
 * <p>
 * The default parameter set is deliberately narrow -- one subscriber mix, one column type, one table shape -- so that
 * the default run stays around ten minutes. The other arms are reached with {@code -p}, for example
 * {@code -p subscriberMix=FULL,VIEWPORT -p columnType=String}.
 *
 * <p>
 * The chunk-storage counter is exact for the primitive column types. For {@code String} it counts the eight bytes of
 * each reference and not the referent, which is the right measure here: the delta chunks hold references to values the
 * column source already retains, so the referents are not memory the producer added.
 *
 * <p>
 * Chunk pooling is left at its configured default, which is what production runs with. Two pooling effects inflate the
 * heap-growth counter relative to the chunk-storage counter: pooled chunks are allocated at power-of-two capacities, so
 * a delta's last partial chunk carries unused capacity; and released chunks return to the pool that allocated them
 * (each update graph thread has its own) where they stay softly reachable for reuse rather than being freed. The
 * {@code jmhRunBarrageProducerPendingDeltasNoPool} task disables writable-chunk pooling so that released chunks are
 * plain garbage and the heap-growth counter reflects only what the producer itself still holds.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(1)
public class BarrageProducerPendingDeltaBenchmark {

    /**
     * The subscriber's update interval. Its value is irrelevant here because the propagation job only runs when the
     * benchmark drains the test scheduler; it never runs on its own.
     */
    private static final long UPDATE_INTERVAL_MS = 1_000;

    private static final long KIB = 1L << 10;

    /** Number of distinct values a {@code String} column draws from. */
    private static final int STRING_POOL_SIZE = 1024;

    public enum UpdatePattern {
        /** Modify rows {@code [0, rowsPerCycle)} every cycle. */
        MODIFY_SAME,
        /** Modify the next block of {@code rowsPerCycle} rows each cycle, wrapping around the table. */
        MODIFY_ROTATE,
        /** Append {@code rowsPerCycle} rows and remove the oldest {@code rowsPerCycle} rows each cycle. */
        ROLLING,
        /**
         * Append {@code rowsPerCycle} rows each cycle and never remove or modify anything. Nothing is ever superseded,
         * so coalescing cannot drop a single row and compaction declines the run. The rows appended by an invocation
         * are removed again, untimed, after it.
         */
        APPEND_ONLY,
        /**
         * Add {@code rowsPerCycle} rows each cycle and never remove or modify anything, but place each cycle's block at
         * a scrambled position in the key space so that later cycles add keys below earlier ones. Like
         * {@code APPEND_ONLY} nothing is ever superseded and compaction declines the run; this arm shows that the
         * decline does not depend on key order. The added rows are removed again, untimed, after each invocation.
         */
        ADD_ONLY
    }

    public enum CompactionStrategy {
        /** Queue every cycle's delta and coalesce once, at flush time; production behaviour before DH-21949. */
        ACCUMULATE,
        /** After every cycle, fold everything pending into one delta. The CPU worst case. */
        COMPACT_EVERY_CYCLE,
        /**
         * The producer's own policy at its production defaults: compact when the bytes recorded since the last
         * compaction exceed the larger of the floor and the compacted size, or the delta count reaches the cap. The
         * compaction job is run after each cycle, as the scheduler thread would.
         */
        GEOMETRIC
    }

    public enum SubscriberMix {
        /** No subscriptions: the producer records nothing, isolating the cost of ticking the source table. */
        NONE,
        /** One full subscription to every column. */
        FULL,
        /** One forward viewport over positions {@code [0, rowsPerCycle)}. */
        VIEWPORT,
        /** Both of the above, which is what makes the producer record full adds and viewport-scoped adds together. */
        FULL_AND_VIEWPORT
    }

    /**
     * The update graph side: run {@code cyclesPerFlush} cycles, each of which makes the producer record a delta. The
     * state flushes the producer after every invocation, untimed, so each invocation starts from an empty queue.
     */
    @Benchmark
    public void enqueueCycles(final EnqueueState state) {
        state.runCycles();
    }

    /**
     * The propagation side: coalesce the pending deltas into one message, serialize it, and deliver it. The state runs
     * the cycles before every invocation, untimed, and records what the producer holds at that moment.
     */
    @Benchmark
    public void flushPendingDeltas(final FlushState state) {
        state.flush();
    }

    /**
     * A subscriber joins while deltas are queued: snapshot, split the queue at the snapshot step, send the earlier half
     * to the existing subscribers and the snapshot to the newcomer.
     */
    @Benchmark
    public void joinWithPendingDeltas(final JoinState state) {
        state.joinAndFlush();
    }

    /** All of {@code cyclesPerFlush} cycles, any compactions the strategy calls for, and the final flush. */
    @Benchmark
    public void cyclesThenFlush(final StrategyState state) {
        state.runStrategy();
    }

    /**
     * State for {@link #cyclesThenFlush}. The aux counters follow the {@link FlushState} convention: per-iteration
     * maxima, divided by the measurement iteration count so JMH's summation reports a per-operation figure.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class StrategyState extends ProducerState {
        @Param({"ACCUMULATE", "COMPACT_EVERY_CYCLE", "GEOMETRIC"})
        CompactionStrategy strategy;

        /** KiB the producer held at its peak: pending deltas, plus whatever a compaction copied while both existed. */
        public double peakPendingKiB;
        /** Deltas queued when the final flush ran. */
        public double pendingDeltasAtFlush;
        /** Compactions performed per operation. */
        public double compactions;

        private int measurementIterations;
        private long maxPeakPendingBytes;
        private long maxPendingDeltasAtFlush;
        private long maxCompactions;

        @Setup(Level.Iteration)
        public void resetCounters(final BenchmarkParams params) {
            measurementIterations = Math.max(1, params.getMeasurement().getCount());
            maxPeakPendingBytes = 0;
            maxPendingDeltasAtFlush = 0;
            maxCompactions = 0;
            publishCounters();
        }

        private void publishCounters() {
            peakPendingKiB = maxPeakPendingBytes / (double) KIB / measurementIterations;
            pendingDeltasAtFlush = maxPendingDeltasAtFlush / (double) measurementIterations;
            compactions = maxCompactions / (double) measurementIterations;
        }

        @Override
        CompactionStrategy strategy() {
            return strategy;
        }

        void runStrategy() {
            long peak = 0;
            long numCompactions = 0;
            for (int ii = 0; ii < cyclesPerFlush; ++ii) {
                runOneCycle();
                final long pending = pendingDeltaBytes();
                peak = Math.max(peak, pending);
                // While a compaction copies, the originals and the copy coexist: the copied bytes are the transient
                // on top of what was pending.
                final long copiedBefore = producer.getCompactionCopiedBytes();
                if (strategy == CompactionStrategy.COMPACT_EVERY_CYCLE && pendingDeltaCount() >= 2) {
                    if (producer.compactPendingDeltasInline(pendingDeltaCount())) {
                        ++numCompactions;
                        peak = Math.max(peak, pending + (producer.getCompactionCopiedBytes() - copiedBefore));
                    }
                } else if (strategy == CompactionStrategy.GEOMETRIC) {
                    // Run whatever the producer scheduled for right now: the compaction job if the policy fired. The
                    // propagation job is due an update interval later and is left queued for flush().
                    scheduler.runThrough(scheduler.currentTimeMillis());
                    final long copied = producer.getCompactionCopiedBytes() - copiedBefore;
                    if (copied > 0) {
                        ++numCompactions;
                        peak = Math.max(peak, pending + copied);
                    }
                }
            }
            final int atFlush = pendingDeltaCount();
            flush();

            maxPeakPendingBytes = Math.max(maxPeakPendingBytes, peak);
            maxPendingDeltasAtFlush = Math.max(maxPendingDeltasAtFlush, atFlush);
            maxCompactions = Math.max(maxCompactions, numCompactions);
            publishCounters();
        }

        @TearDown(Level.Invocation)
        public void checkAfter() {
            checkObserversHealthy();
            if (pendingDeltaCount() != 0) {
                throw new IllegalStateException("Deltas still pending after the flush");
            }
            resetAppendedRows();
        }
    }

    /** State for {@link #enqueueCycles}. */
    @State(Scope.Thread)
    public static class EnqueueState extends ProducerState {
        @TearDown(Level.Invocation)
        public void flushAfterEnqueue() {
            flush();
            checkObserversHealthy();
            resetAppendedRows();
        }
    }

    /**
     * State for {@link #flushPendingDeltas}. Its public fields are aux counters. Each tracks the maximum observed
     * within the iteration, taken right before the flush while the producer still holds every delta. JMH sums event
     * counters over the measurement iterations, so each field holds its maximum divided by the measurement iteration
     * count; the reported total is then the mean over iterations of the per-iteration maximum, i.e. a per-flush figure.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class FlushState extends ProducerState {
        /**
         * How the cycles before the flush are run. {@code ACCUMULATE} queues every delta, so the flush coalesces them
         * all; {@code GEOMETRIC} lets the producer's policy compact between cycles, so the flush finds a short queue.
         * The flush itself is what is timed either way.
         */
        @Param({"ACCUMULATE", "GEOMETRIC"})
        CompactionStrategy strategy;

        /** Deltas queued in the producer before the flush. */
        public double pendingDeltas;
        /** KiB of chunk storage owned by those deltas (adds plus mods, all columns). */
        public double pendingChunkKiB;
        /** KiB of used-heap growth over the post-setup baseline, after a GC; zero when heap measurement is off. */
        public double heapGrowthKiB;
        /** KiB of serialized Barrage data delivered to all subscribers by the flush. */
        public double flushedWireKiB;

        private int measurementIterations;
        private long maxPendingDeltas;
        private long maxPendingChunkBytes;
        private long maxHeapGrowthBytes;
        private long maxFlushedWireBytes;

        private long wireBytesBeforeFlush;

        @Setup(Level.Iteration)
        public void resetCounters(final BenchmarkParams params) {
            measurementIterations = Math.max(1, params.getMeasurement().getCount());
            maxPendingDeltas = 0;
            maxPendingChunkBytes = 0;
            maxHeapGrowthBytes = 0;
            maxFlushedWireBytes = 0;
            publishCounters();
        }

        private void publishCounters() {
            pendingDeltas = maxPendingDeltas / (double) measurementIterations;
            pendingChunkKiB = maxPendingChunkBytes / (double) KIB / measurementIterations;
            heapGrowthKiB = maxHeapGrowthBytes / (double) KIB / measurementIterations;
            flushedWireKiB = maxFlushedWireBytes / (double) KIB / measurementIterations;
        }

        @Override
        CompactionStrategy strategy() {
            return strategy;
        }

        @Setup(Level.Invocation)
        public void enqueueBeforeFlush() {
            runCyclesUnder(strategy);

            final int deltaCount = pendingDeltaCount();
            if (subscriberMix != SubscriberMix.NONE && strategy == CompactionStrategy.ACCUMULATE
                    && deltaCount < cyclesPerFlush) {
                throw new IllegalStateException(
                        "Expected " + cyclesPerFlush + " pending deltas, found " + deltaCount);
            }
            maxPendingDeltas = Math.max(maxPendingDeltas, deltaCount);
            maxPendingChunkBytes = Math.max(maxPendingChunkBytes, pendingDeltaBytes());
            if (measureHeap) {
                maxHeapGrowthBytes = Math.max(maxHeapGrowthBytes, usedHeapAfterGc() - baselineHeapBytes);
            }

            wireBytesBeforeFlush = totalWireBytes();
        }

        @TearDown(Level.Invocation)
        public void recordAfterFlush() {
            maxFlushedWireBytes = Math.max(maxFlushedWireBytes, totalWireBytes() - wireBytesBeforeFlush);
            publishCounters();
            checkObserversHealthy();
            if (pendingDeltaCount() != 0) {
                throw new IllegalStateException("Deltas still pending after the flush");
            }
            resetAppendedRows();
        }
    }

    /**
     * State for {@link #joinWithPendingDeltas}. The newcomer is added by the timed method and removed afterwards, so
     * the subscriber population is the same at the start of every invocation.
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class JoinState extends ProducerState {
        /** How the cycles before the join are run; see {@link FlushState#strategy}. */
        @Param({"ACCUMULATE", "GEOMETRIC"})
        CompactionStrategy strategy;

        /** Deltas queued in the producer when the newcomer joined. */
        public double pendingDeltas;
        /** KiB of serialized Barrage data delivered to every subscriber (newcomer included) by the join. */
        public double joinWireKiB;

        private int measurementIterations;
        private long maxPendingDeltas;
        private long maxJoinWireBytes;

        private CountingObserver joiner;
        private long wireBytesBeforeJoin;

        @Setup(Level.Iteration)
        public void resetCounters(final BenchmarkParams params) {
            measurementIterations = Math.max(1, params.getMeasurement().getCount());
            maxPendingDeltas = 0;
            maxJoinWireBytes = 0;
            publishCounters();
        }

        private void publishCounters() {
            pendingDeltas = maxPendingDeltas / (double) measurementIterations;
            joinWireKiB = maxJoinWireBytes / (double) KIB / measurementIterations;
        }

        @Override
        CompactionStrategy strategy() {
            return strategy;
        }

        @Setup(Level.Invocation)
        public void enqueueBeforeJoin() {
            runCyclesUnder(strategy);
            maxPendingDeltas = Math.max(maxPendingDeltas, pendingDeltaCount());
            wireBytesBeforeJoin = totalWireBytes();
        }

        /** Adds a full subscriber and runs the propagation job it schedules. */
        void joinAndFlush() {
            joiner = addFullSubscriber();
            flush();
        }

        @TearDown(Level.Invocation)
        public void leaveAfterJoin() {
            if (joiner.messages < 2) {
                throw new IllegalStateException("The newcomer did not receive a snapshot");
            }
            maxJoinWireBytes = Math.max(maxJoinWireBytes, totalWireBytes() - wireBytesBeforeJoin);
            publishCounters();
            removeSubscriber(joiner);
            flush();
            checkObserversHealthy();
            resetAppendedRows();
        }
    }

    /**
     * A refreshing table, a producer over it with the configured subscriber mix, and the machinery to tick the table
     * and flush the producer on the benchmark thread. Never used directly; the per-method states extend it. JMH
     * requires the {@code @Param} fields to sit in a {@code @State}-annotated class, hence the annotation.
     */
    @State(Scope.Thread)
    public static class ProducerState {

        /** Update graph cycles recorded by the producer before each propagation. */
        @Param({"1", "10", "100", "1000"})
        int cyclesPerFlush;

        /** Rows touched per update graph cycle. */
        @Param({"10000"})
        int rowsPerCycle;

        /** Rows in the table; must be a multiple of {@code rowsPerCycle}. */
        @Param({"100000"})
        int tableSize;

        /** Number of columns, all of {@code columnType} and all subscribed. */
        @Param({"8"})
        int numColumns;

        /** Element type of every column: {@code long}, {@code double} or {@code String}. */
        @Param({"long"})
        String columnType;

        @Param({"MODIFY_SAME", "MODIFY_ROTATE", "ROLLING"})
        UpdatePattern updatePattern;

        @Param({"FULL"})
        SubscriberMix subscriberMix;

        /** Whether to GC and measure used heap before each flush; costs a full GC per invocation. */
        @Param({"true"})
        boolean measureHeap;

        /**
         * Writes one cell. Resolved once per trial so that the column type is not dispatched on per cell: the update
         * loop writes {@code rowsPerCycle * numColumns} cells per update graph cycle, and a type dispatch there would
         * show up as producer-side cost in {@link #enqueueCycles}.
         */
        @FunctionalInterface
        interface CellWriter {
            void write(int columnIndex, long rowKey, long cycle);
        }

        private CellWriter cellWriter;

        private SafeCloseable executionContext;
        private ControlledUpdateGraph updateGraph;
        private boolean oldSerialTableOperationsSafe;
        private SafeCloseable livenessScope;

        TestControlledScheduler scheduler;
        private QueryTable table;
        private WritableColumnSource<?>[] columnSources;
        BarrageMessageProducer producer;
        private final List<CountingObserver> observers = new ArrayList<>();

        /** The distinct values a {@code String} column draws from; null for the primitive types. */
        private String[] stringPool;

        long baselineHeapBytes;
        private long cycleCounter;

        /** {@code ROLLING} only: the key space wraps at {@code 2 * tableSize} so removed keys are reused. */
        private long rollingKeySpace;
        private long rollingNextKey;
        /** {@code ROLLING} only: live blocks, oldest first; each is removed after {@code tableSize / rowsPerCycle}. */
        private final ArrayDeque<RowSet> rollingLiveBlocks = new ArrayDeque<>();

        /** {@code APPEND_ONLY} only: the next key to append; reset to {@code tableSize} after each invocation. */
        private long appendNextKey;
        /** {@code ADD_ONLY} only: blocks added so far this invocation; reset to zero after each invocation. */
        private int addedBlocks;

        @Setup(Level.Trial)
        public void setupTrial() {
            if (rowsPerCycle <= 0 || tableSize % rowsPerCycle != 0) {
                throw new IllegalArgumentException("tableSize must be a positive multiple of rowsPerCycle");
            }

            executionContext = TestExecutionContext.createForUnitTests().open();
            updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.enableUnitTestMode();
            updateGraph.resetForUnitTests(false);
            oldSerialTableOperationsSafe = updateGraph.setSerialTableOperationsSafe(true);
            livenessScope = LivenessScopeStack.open(new LivenessScope(true), true);

            scheduler = new TestControlledScheduler();
            if ("String".equals(columnType)) {
                stringPool = makeStringPool();
            }

            // Build the source table: numColumns columns over keys [0, tableSize).
            rollingKeySpace = 2L * tableSize;
            final long capacity;
            switch (updatePattern) {
                case ROLLING:
                    capacity = rollingKeySpace;
                    break;
                case APPEND_ONLY:
                case ADD_ONLY:
                    // room for every row an invocation can add
                    capacity = tableSize + (long) cyclesPerFlush * rowsPerCycle;
                    break;
                default:
                    capacity = tableSize;
            }
            appendNextKey = tableSize;
            final Class<?> dataType = elementClass(columnType);
            columnSources = new WritableColumnSource[numColumns];
            final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
            for (int ci = 0; ci < numColumns; ++ci) {
                final WritableColumnSource<?> source =
                        ArrayBackedColumnSource.getMemoryColumnSource(capacity, dataType);
                source.ensureCapacity(capacity);
                columnSources[ci] = source;
                columns.put("C" + ci, source);
            }
            // Fill the whole key space, including the keys ROLLING will later add, so that no cell has to be
            // written once measurement begins. See runOneCycle for why.
            cellWriter = makeCellWriter();
            for (int ci = 0; ci < numColumns; ++ci) {
                for (long rowKey = 0; rowKey < capacity; ++rowKey) {
                    cellWriter.write(ci, rowKey, 0);
                }
            }
            table = new QueryTable(RowSetFactory.flat(tableSize).toTracking(), columns);
            table.setRefreshing(true);
            for (final WritableColumnSource<?> source : columnSources) {
                source.startTrackingPrevValues();
            }

            if (updatePattern == UpdatePattern.ROLLING) {
                for (long start = 0; start < tableSize; start += rowsPerCycle) {
                    rollingLiveBlocks.addLast(RowSetFactory.fromRange(start, start + rowsPerCycle - 1));
                }
                rollingNextKey = tableSize;
            }

            producer = table.getResult(new BarrageMessageProducer.Operation(
                    scheduler, new SessionService.ObfuscatingErrorTransformer(),
                    new BarrageMessageWriterImpl.Factory(), table, UPDATE_INTERVAL_MS));
            // The producer's own compaction policy is off unless the state's strategy turns it on, so accumulate arms
            // measure the pre-compaction baseline. Applied here, not in a subclass @Setup, because JMH does not order
            // trial-level setup methods across the class hierarchy.
            producer.setCompactionPolicy(false, Long.MAX_VALUE, 1.0, 0, false);
            applyStrategy(strategy());

            switch (subscriberMix) {
                case NONE:
                    break;
                case FULL:
                    addFullSubscriber();
                    break;
                case VIEWPORT:
                    addViewportSubscriber();
                    break;
                case FULL_AND_VIEWPORT:
                    addFullSubscriber();
                    addViewportSubscriber();
                    break;
            }

            // drain the scheduler so every initial (growing) snapshot completes and the subscriptions become active
            flush();
            checkObserversHealthy();
            if (pendingDeltaCount() != 0) {
                throw new IllegalStateException("Expected no pending deltas after the initial snapshots");
            }

            baselineHeapBytes = measureHeap ? usedHeapAfterGc() : 0;
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() {
            for (final CountingObserver observer : new ArrayList<>(observers)) {
                removeSubscriber(observer);
            }
            flush();
            rollingLiveBlocks.forEach(RowSet::close);
            rollingLiveBlocks.clear();
            livenessScope.close();
            updateGraph.setSerialTableOperationsSafe(oldSerialTableOperationsSafe);
            updateGraph.resetForUnitTests(true);
            executionContext.close();
        }

        private String[] makeStringPool() {
            final Random random = new Random(0xB33FCAFEL);
            final String[] pool = new String[STRING_POOL_SIZE];
            final char[] chars = new char[16];
            for (int pi = 0; pi < pool.length; ++pi) {
                for (int ci = 0; ci < chars.length; ++ci) {
                    chars[ci] = (char) ('a' + random.nextInt(26));
                }
                pool[pi] = new String(chars);
            }
            return pool;
        }

        private static Class<?> elementClass(final String columnType) {
            switch (columnType) {
                case "long":
                    return long.class;
                case "double":
                    return double.class;
                case "String":
                    return String.class;
                default:
                    throw new IllegalArgumentException("Unknown column type: " + columnType);
            }
        }

        /** The value column {@code ci} should hold for {@code rowKey} on {@code cycle}. */
        private long cellValue(final int ci, final long rowKey, final long cycle) {
            return (cycle + 1) * (numColumns + rowKey) + ci;
        }

        /** Binds the column type once, so the update loop does no per-cell type dispatch. */
        @SuppressWarnings("unchecked")
        private CellWriter makeCellWriter() {
            switch (columnType) {
                case "long": {
                    final WritableColumnSource<Long>[] sources =
                            (WritableColumnSource<Long>[]) columnSources;
                    return (ci, rowKey, cycle) -> sources[ci].set(rowKey, cellValue(ci, rowKey, cycle));
                }
                case "double": {
                    final WritableColumnSource<Double>[] sources =
                            (WritableColumnSource<Double>[]) columnSources;
                    return (ci, rowKey, cycle) -> sources[ci].set(rowKey, (double) cellValue(ci, rowKey, cycle));
                }
                case "String": {
                    final WritableColumnSource<String>[] sources =
                            (WritableColumnSource<String>[]) columnSources;
                    final String[] pool = stringPool;
                    return (ci, rowKey, cycle) -> sources[ci].set(rowKey,
                            pool[(int) Math.floorMod(cellValue(ci, rowKey, cycle), STRING_POOL_SIZE)]);
                }
                default:
                    throw new IllegalStateException("Unknown column type: " + columnType);
            }
        }

        private BitSet allColumns() {
            final BitSet columns = new BitSet();
            columns.set(0, numColumns);
            return columns;
        }

        private static BarrageSubscriptionOptions options() {
            return BarrageSubscriptionOptions.builder().useDeephavenNulls(true).build();
        }

        CountingObserver addFullSubscriber() {
            final CountingObserver observer = new CountingObserver();
            observers.add(observer);
            producer.addSubscription(observer, options(), allColumns(), null, false);
            return observer;
        }

        CountingObserver addViewportSubscriber() {
            final CountingObserver observer = new CountingObserver();
            observers.add(observer);
            try (final RowSet viewport = RowSetFactory.fromRange(0, rowsPerCycle - 1)) {
                producer.addSubscription(observer, options(), allColumns(), viewport.copy(), false);
            }
            return observer;
        }

        void removeSubscriber(final CountingObserver observer) {
            producer.removeSubscription(observer);
            observers.remove(observer);
        }

        long totalWireBytes() {
            long bytes = 0;
            for (final CountingObserver observer : observers) {
                bytes += observer.bytes;
            }
            return bytes;
        }

        void checkObserversHealthy() {
            for (final CountingObserver observer : observers) {
                observer.checkHealthy();
            }
        }

        void runCycles() {
            for (int ii = 0; ii < cyclesPerFlush; ++ii) {
                runOneCycle();
            }
        }

        /**
         * Runs the cycles as a producer under {@code strategy} would experience them: for {@code GEOMETRIC}, whatever
         * the producer scheduled for right now (the compaction job, if the policy fired) runs after each cycle, as it
         * would on the scheduler thread, while the propagation job stays queued for the flush.
         */
        void runCyclesUnder(final CompactionStrategy strategy) {
            for (int ii = 0; ii < cyclesPerFlush; ++ii) {
                runOneCycle();
                if (strategy == CompactionStrategy.GEOMETRIC) {
                    scheduler.runThrough(scheduler.currentTimeMillis());
                }
            }
        }

        /** The strategy this state runs its cycles under; states with a {@code strategy} parameter override it. */
        CompactionStrategy strategy() {
            return CompactionStrategy.ACCUMULATE;
        }

        /** Turns the producer's own compaction policy on, at production defaults, for the {@code GEOMETRIC} arm. */
        void applyStrategy(final CompactionStrategy strategy) {
            if (strategy == CompactionStrategy.GEOMETRIC) {
                producer.setCompactionPolicy(true, BarrageMessageProducer.COMPACTION_FLOOR_BYTES,
                        BarrageMessageProducer.COMPACTION_GROWTH_FACTOR,
                        BarrageMessageProducer.COMPACTION_MAX_PENDING_DELTAS, false);
            }
        }

        void runOneCycle() {
            final long cycle = cycleCounter++;

            final WritableRowSet added;
            final RowSet removed;
            final WritableRowSet modified;
            final ModifiedColumnSet modifiedColumnSet;
            switch (updatePattern) {
                case MODIFY_SAME:
                    added = RowSetFactory.empty();
                    removed = RowSetFactory.empty();
                    modified = RowSetFactory.fromRange(0, rowsPerCycle - 1);
                    modifiedColumnSet = ModifiedColumnSet.ALL;
                    break;
                case MODIFY_ROTATE: {
                    final long block = cycle % (tableSize / rowsPerCycle);
                    added = RowSetFactory.empty();
                    removed = RowSetFactory.empty();
                    modified = RowSetFactory.fromRange(block * rowsPerCycle, (block + 1) * rowsPerCycle - 1);
                    modifiedColumnSet = ModifiedColumnSet.ALL;
                    break;
                }
                case ROLLING:
                    added = RowSetFactory.fromRange(rollingNextKey, rollingNextKey + rowsPerCycle - 1);
                    rollingNextKey = (rollingNextKey + rowsPerCycle) % rollingKeySpace;
                    removed = rollingLiveBlocks.pollFirst();
                    rollingLiveBlocks.addLast(added.copy());
                    modified = RowSetFactory.empty();
                    modifiedColumnSet = ModifiedColumnSet.EMPTY;
                    break;
                case APPEND_ONLY:
                    added = RowSetFactory.fromRange(appendNextKey, appendNextKey + rowsPerCycle - 1);
                    appendNextKey += rowsPerCycle;
                    removed = RowSetFactory.empty();
                    modified = RowSetFactory.empty();
                    modifiedColumnSet = ModifiedColumnSet.EMPTY;
                    break;
                case ADD_ONLY: {
                    // 7919 is prime and coprime with every default cycle count, so this visits each block slot once
                    // per invocation in a scrambled order; consecutive cycles land far apart in the key space.
                    final long slot = (addedBlocks * 7919L) % cyclesPerFlush;
                    final long firstKey = tableSize + slot * rowsPerCycle;
                    added = RowSetFactory.fromRange(firstKey, firstKey + rowsPerCycle - 1);
                    ++addedBlocks;
                    removed = RowSetFactory.empty();
                    modified = RowSetFactory.empty();
                    modifiedColumnSet = ModifiedColumnSet.EMPTY;
                    break;
                }
                default:
                    throw new IllegalStateException("Unknown update pattern " + updatePattern);
            }

            // Deliberately no cell writes here. Updating the source data is the upstream producer's work, not the
            // Barrage producer's, and it would have to happen inside the update graph cycle for previous-value
            // tracking, which puts it inside the measured region. It is also an order of magnitude more expensive
            // than the work under test -- writing a cell at a time costs several nanoseconds per cell, while the
            // producer reads the same cells with a bulk chunk fill at well under one -- so leaving it in made
            // enqueueCycles a measurement of the harness. The producer records whatever values it finds, so
            // declaring rows modified without rewriting them exercises exactly the same recording path.
            updateGraph.runWithinUnitTestCycle(() -> {
                final TrackingWritableRowSet rowSet = table.getRowSet().writableCast();
                rowSet.remove(removed);
                rowSet.insert(added);
                table.notifyListeners(new TableUpdateImpl(added, removed, modified, RowSetShiftData.EMPTY,
                        modifiedColumnSet));
            });
        }

        /** Runs the producer's propagation job (and any growing-snapshot follow-ups) on the calling thread. */
        void flush() {
            scheduler.runUntilQueueEmpty();
        }

        /**
         * Undo an {@code APPEND_ONLY} invocation's appends with one removal cycle and a flush, so the table is the same
         * size at the start of every invocation. A no-op for the other patterns. Untimed: call it from teardown.
         */
        void resetAppendedRows() {
            final long lastKey;
            if (updatePattern == UpdatePattern.APPEND_ONLY && appendNextKey > tableSize) {
                lastKey = appendNextKey - 1;
                appendNextKey = tableSize;
            } else if (updatePattern == UpdatePattern.ADD_ONLY && addedBlocks > 0) {
                lastKey = tableSize + (long) addedBlocks * rowsPerCycle - 1;
                addedBlocks = 0;
            } else {
                return;
            }
            final WritableRowSet removed = RowSetFactory.fromRange(tableSize, lastKey);
            updateGraph.runWithinUnitTestCycle(() -> {
                table.getRowSet().writableCast().remove(removed);
                table.notifyListeners(new TableUpdateImpl(RowSetFactory.empty(), removed, RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
            });
            flush();
        }

        /** Update graph cycles the producer has recorded but not yet propagated. */
        int pendingDeltaCount() {
            return producer.getPendingDeltaCount();
        }

        /** Approximate heap footprint of the chunk storage those pending updates own. */
        long pendingDeltaBytes() {
            return producer.getPendingDeltaBytes();
        }
    }

    static long usedHeapAfterGc() {
        final Runtime runtime = Runtime.getRuntime();
        for (int ii = 0; ii < 3; ++ii) {
            System.gc();
        }
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /** A subscriber that drains every message it is handed, counting messages and wire bytes. */
    static final class CountingObserver implements StreamObserver<BarrageMessageWriter.MessageView> {
        long messages;
        long bytes;
        Throwable error;

        @Override
        public void onNext(final BarrageMessageWriter.MessageView view) {
            ++messages;
            try {
                view.forEachStream(drainable -> {
                    try {
                        bytes += drainable.drainTo(OutputStream.nullOutputStream());
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
            error = t;
        }

        @Override
        public void onCompleted() {}

        void checkHealthy() {
            if (error != null) {
                throw new IllegalStateException("The producer reported an error", error);
            }
        }
    }
}
