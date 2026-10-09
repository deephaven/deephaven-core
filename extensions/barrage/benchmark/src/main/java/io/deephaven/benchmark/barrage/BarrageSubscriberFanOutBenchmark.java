//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.MultiChunkPool;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.ExecutorJobScheduler;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistryImpl;
import io.deephaven.extensions.barrage.chunk.SharedWriterDictionary;
import io.deephaven.util.SafeCloseable;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Measures what one propagation phase of a {@code BarrageMessageProducer} costs once the message is in hand: a
 * {@link BarrageMessageWriter} is made for one snapshot message, and every subscriber's view of it is serialized, with
 * the subscribers' writes fanned out over {@code threads} threads by a {@link JobScheduler#invokeParallel} on an
 * {@link ExecutorJobScheduler}, exactly as the producer fans them out. A {@code threads} of one runs them in turn on an
 * {@link ImmediateJobScheduler}, the sequential loop the producer ran before parallel writes.
 *
 * <p>
 * Each subscriber drains its streams into its own sink, which copies every buffer it is handed into a small scratch
 * array. That stands in for the copy a gRPC transport makes into its write buffers without the memory a real buffer for
 * every subscriber would need.
 *
 * <p>
 * The {@code dictString} arm dictionary-encodes every column. Full subscribers then share one producer-level dictionary
 * per column, as they do in the producer, and each subscriber's fill registers values under the dictionary's monitor;
 * viewport subscribers each have their own.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
// Each fork runs measurably faster for its first ten seconds or so and then settles to a steady rate that it holds to
// within about one percent; the warmup is long enough to measure only the steady state.
@Warmup(iterations = 6, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"-Xms8g", "-Xmx8g"})
public class BarrageSubscriberFanOutBenchmark {

    private static final BarrageMessageWriter.Factory WRITER_FACTORY = new BarrageMessageWriterImpl.Factory();

    /** Number of distinct values a {@code String} column draws from. */
    private static final int STRING_POOL_SIZE = 1024;
    private static final int STRING_LENGTH = 16;

    /** The size of the scratch array each subscriber's sink copies into. */
    private static final int SINK_SCRATCH_BYTES = 1 << 20;

    /** The element type of every column: {@code long}, {@code String}, or {@code dictString}. */
    @Param({"long"})
    private String columnType;

    /** Number of subscribers that each receive their own view of the message. */
    @Param({"1", "2", "4", "8", "16"})
    private int subscribers;

    /** The most threads that write at once, the calling thread included; one writes sequentially. */
    @Param({"1", "2", "4", "8"})
    private int threads;

    /**
     * What the subscribers subscribe to: {@code full} (the whole table), {@code viewport} (a distinct tenth of the
     * table each), or {@code mixed} (alternating full, forward viewport, and reverse viewport).
     */
    @Param({"full"})
    private String mix;

    @Param({"200000"})
    private int numRows;

    @Param({"50"})
    private int numColumns;

    private SafeCloseable executionContext;
    private ThreadPoolExecutor pool;
    private Supplier<JobScheduler> schedulerFactory;

    private BarrageMessage message;
    private ChunkWriter<Chunk<Values>>[] chunkWriters;
    private Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaries;
    private List<Subscriber> subscriberList;

    @Setup(Level.Trial)
    @SuppressWarnings("unchecked")
    public void setup() {
        executionContext = TestExecutionContext.createForUnitTests().open();

        if (threads > 1) {
            final AtomicInteger threadCount = new AtomicInteger();
            pool = ExecutorJobScheduler.newHelperPool(threads - 1, runnable -> {
                final Thread thread = new Thread(() -> {
                    MultiChunkPool.enableDedicatedPoolForThisThread();
                    runnable.run();
                }, "fan-out-" + threadCount.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
            final ThreadPoolExecutor helperPool = pool;
            final int threadCountForScheduler = threads;
            schedulerFactory = () -> new ExecutorJobScheduler(helperPool, threadCountForScheduler);
        } else {
            schedulerFactory = ImmediateJobScheduler::new;
        }

        final boolean dictionaryEncoded = columnType.equals("dictString");
        final Class<?> dataType = columnType.equals("long") ? long.class : String.class;
        final ChunkType chunkType = ChunkType.fromElementType(dataType);

        chunkWriters = (ChunkWriter<Chunk<Values>>[]) new ChunkWriter[numColumns];
        for (int ci = 0; ci < numColumns; ++ci) {
            final ArrowType arrowType = dataType == long.class ? new ArrowType.Int(64, true) : new ArrowType.Utf8();
            final DictionaryEncoding encoding = dictionaryEncoded
                    ? new DictionaryEncoding(ci, false, new ArrowType.Int(32, true))
                    : null;
            final Field field = new Field("C" + ci, new FieldType(true, arrowType, encoding), Collections.emptyList());
            chunkWriters[ci] = DefaultChunkWriterFactory.INSTANCE.newWriterPojo(
                    BarrageTypeInfo.make(dataType, null, field));
        }
        sharedDictionaries = new Long2ObjectOpenHashMap<>();

        // The chunks are array-backed and their close() does nothing, so the message survives every writer made for it.
        final Random random = new Random(0xB33FCAFEL);
        final String[] stringPool = dataType == String.class ? makeStringPool(random) : null;
        final BarrageMessage.AddColumnData[] addColumnData = new BarrageMessage.AddColumnData[numColumns];
        for (int ci = 0; ci < numColumns; ++ci) {
            final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
            acd.type = dataType;
            acd.componentType = null;
            acd.chunkType = chunkType;
            // mutable, because closing the message clears it
            acd.data = new ArrayList<>(List.of(makeColumn(random, stringPool)));
            addColumnData[ci] = acd;
        }

        message = new BarrageMessage();
        message.isSnapshot = true;
        message.firstSeq = 0;
        message.lastSeq = 0;
        message.tableSize = numRows;
        message.rowsAdded = RowSetFactory.flat(numRows);
        message.rowsIncluded = RowSetFactory.flat(numRows);
        message.rowsRemoved = RowSetFactory.empty();
        message.shifted = RowSetShiftData.EMPTY;
        message.addColumnData = addColumnData;
        message.modColumnData = BarrageMessage.ZERO_MOD_COLUMNS;

        final BitSet allColumns = new BitSet(numColumns);
        allColumns.set(0, numColumns);
        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder().build();
        subscriberList = new ArrayList<>(subscribers);
        for (int si = 0; si < subscribers; ++si) {
            subscriberList.add(new Subscriber(si, options, allColumns));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        for (final Subscriber subscriber : subscriberList) {
            subscriber.close();
        }
        message.close();
        if (pool != null) {
            pool.shutdownNow();
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the helper pool did not terminate");
            }
        }
        executionContext.close();
    }

    /** Writes the message to every subscriber, as one producer propagation phase does, and returns the bytes sent. */
    @Benchmark
    public long propagate() {
        try (final BarrageMessageWriter writer =
                WRITER_FACTORY.newMessageWriter(message, chunkWriters,
                        BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            schedulerFactory.get().invokeParallel(ExecutionContext.getContext(), null,
                    JobScheduler.DEFAULT_CONTEXT_FACTORY, 0, subscriberList.size(),
                    (context, si, nec) -> subscriberList.get(si).write(writer));
        }
        long bytes = 0;
        for (final Subscriber subscriber : subscriberList) {
            bytes += subscriber.sink.takeBytes();
        }
        return bytes;
    }

    private String[] makeStringPool(final Random random) {
        final char[] chars = new char[STRING_LENGTH];
        final String[] pool = new String[STRING_POOL_SIZE];
        for (int pi = 0; pi < pool.length; ++pi) {
            for (int ci = 0; ci < STRING_LENGTH; ++ci) {
                chars[ci] = (char) ('a' + random.nextInt(26));
            }
            pool[pi] = new String(chars);
        }
        return pool;
    }

    private WritableChunk<Values> makeColumn(final Random random, final String[] stringPool) {
        if (stringPool == null) {
            final long[] values = new long[numRows];
            for (int ri = 0; ri < numRows; ++ri) {
                values[ri] = random.nextLong();
            }
            return WritableLongChunk.writableChunkWrap(values);
        }
        final String[] values = new String[numRows];
        for (int ri = 0; ri < numRows; ++ri) {
            values[ri] = stringPool[random.nextInt(stringPool.length)];
        }
        return WritableObjectChunk.writableChunkWrap(values);
    }

    /** One subscriber's subscription, as the producer would hand it to {@link BarrageMessageWriter#getSubView}. */
    private final class Subscriber implements SafeCloseable {
        private final BarrageSubscriptionOptions options;
        private final BitSet columns;
        private final boolean isFullSubscription;
        private final boolean reverseViewport;
        /** The position-space viewport; {@code null} for a full subscription. */
        private final RowSet viewport;
        /** The viewport in key space, which for a flat snapshot is the viewport's positions themselves. */
        private final RowSet keyspaceViewport;
        /** An initial snapshot has no previous viewport. */
        private final RowSet keyspaceViewportPrev;
        private final CopyingSink sink = new CopyingSink();

        private Subscriber(final int index, final BarrageSubscriptionOptions options, final BitSet columns) {
            this.options = options;
            this.columns = columns;
            final int kind = mix.equals("full") ? 0 : mix.equals("viewport") ? 1 + index % 2 : index % 3;
            isFullSubscription = kind == 0;
            reverseViewport = kind == 2;
            if (isFullSubscription) {
                viewport = null;
                keyspaceViewport = message.rowsAdded.copy();
                keyspaceViewportPrev = null;
            } else {
                final long width = Math.max(1, numRows / 10);
                final long start = (index * (long) numRows / Math.max(1, subscribers)) % (numRows - width + 1);
                viewport = RowSetFactory.fromRange(start, start + width - 1);
                keyspaceViewport = message.rowsAdded.subSetForPositions(viewport, reverseViewport);
                keyspaceViewportPrev = RowSetFactory.empty();
            }
        }

        private void write(@NotNull final BarrageMessageWriter writer) {
            // An initial snapshot starts every subscriber on a fresh dictionary registry. Full subscribers' registries
            // share the producer-level dictionaries; viewport subscribers' do not.
            final DictionaryWriterRegistry registry = isFullSubscription
                    ? new DictionaryWriterRegistryImpl(sharedDictionaries)
                    : new DictionaryWriterRegistryImpl();
            final BarrageMessageWriter.MessageView view = writer.getSubView(options, true, isFullSubscription,
                    viewport, reverseViewport, keyspaceViewportPrev, keyspaceViewport, columns, registry);
            try {
                view.forEachStream(stream -> {
                    try {
                        stream.drainTo(sink);
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
        public void close() {
            SafeCloseable.closeAll(viewport, keyspaceViewport, keyspaceViewportPrev);
        }
    }

    /** Copies every buffer it is given into a scratch array, wrapping around, and counts the bytes. */
    private static final class CopyingSink extends OutputStream {
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

        private long takeBytes() {
            final long result = bytes;
            bytes = 0;
            return result;
        }
    }
}
