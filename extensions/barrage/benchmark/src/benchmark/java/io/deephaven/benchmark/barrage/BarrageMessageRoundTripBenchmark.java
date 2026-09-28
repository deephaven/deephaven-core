//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory;
import io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks the cost of serializing and deserializing a Barrage snapshot message composed of a large number of random
 * primitive columns of a single type.
 * <p>
 * The write path mirrors what a Barrage producer does: a {@link BarrageMessage} is handed to a
 * {@link BarrageMessageWriter}, and its full-snapshot {@link BarrageMessageWriter.MessageView} is drained to a byte
 * buffer. The read path mirrors a Barrage client: {@link BarrageMessageReaderImpl#safelyParseFrom} reconstructs a
 * {@link BarrageMessage} from those bytes. The reader is stateful and must first observe a schema message (fed once
 * during setup) to establish its per-column {@link ChunkWriter}s before it can parse record batches.
 * <p>
 * The source data is held in array-backed chunks whose {@code close()} is a no-op, so the pre-built message survives
 * repeated serialization and each invocation measures serialization only (no per-invocation data marshalling).
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(1)
public class BarrageMessageRoundTripBenchmark {

    private static final BarrageMessageWriter.Factory WRITER_FACTORY = new BarrageMessageWriterImpl.Factory();

    /** The primitive element type of every column. */
    @Param({"double", "float", "long", "int", "short", "byte"})
    private String columnType;

    /** Number of columns in the message. */
    @Param({"5000"})
    private int numColumns;

    /** Number of rows per column. */
    @Param({"2000"})
    private int numRows;

    /** Fraction of cells that are null; exercises the validity-buffer / DH-null encoding paths. */
    @Param({"0.0", "0.1"})
    private double nullFraction;

    /** When true, nulls are encoded as the Deephaven sentinel value; when false, as an Arrow validity buffer. */
    @Param({"false", "true"})
    private boolean useDeephavenNulls;

    private SafeCloseable executionContext;

    private BarrageSnapshotOptions options;
    private BarrageMessageReaderImpl reader;

    // reader-side wire metadata (every column shares the same primitive type)
    private ChunkType[] wireChunkTypes;
    private Class<?>[] wireTypes;
    private Class<?>[] wireComponentTypes;

    // the reusable, pre-built message and its pre-serialized bytes
    private BarrageMessage message;
    private ChunkWriter<Chunk<Values>>[] chunkWriters;
    private byte[] serialized;

    @Setup(Level.Trial)
    @SuppressWarnings("unchecked")
    public void setup() {
        executionContext = TestExecutionContext.createForUnitTests().open();

        options = BarrageSnapshotOptions.builder()
                .useDeephavenNulls(useDeephavenNulls)
                .build();
        reader = new BarrageMessageReaderImpl();

        final Class<?> dataType = primitiveClass(columnType);
        final ChunkType chunkType = ChunkType.fromElementType(dataType);

        // Build the flat table schema of numColumns columns of the chosen type.
        final List<ColumnDefinition<?>> columns = new ArrayList<>(numColumns);
        for (int ci = 0; ci < numColumns; ++ci) {
            columns.add(ColumnDefinition.fromGenericType("C" + ci, dataType));
        }
        final TableDefinition definition = TableDefinition.of(columns);
        final Schema schema = BarrageUtil.makeSchema(options, definition, Map.of(), true);

        // Per-column wire metadata.
        wireChunkTypes = new ChunkType[numColumns];
        wireTypes = new Class<?>[numColumns];
        wireComponentTypes = new Class<?>[numColumns];
        Arrays.fill(wireChunkTypes, chunkType);
        Arrays.fill(wireTypes, dataType);

        chunkWriters = (ChunkWriter<Chunk<Values>>[]) new ChunkWriter[numColumns];
        for (int ci = 0; ci < numColumns; ++ci) {
            chunkWriters[ci] = DefaultChunkWriterFactory.INSTANCE.newWriterPojo(
                    BarrageTypeInfo.make(dataType, null, schema.getFields().get(ci)));
        }

        // Generate the random column data once. The chunks are array-backed and have a no-op close(), so the message
        // can be handed to a writer repeatedly without losing its data.
        final Random random = new Random(0xB33FCAFEL);
        final BarrageMessage.AddColumnData[] addColumnData = new BarrageMessage.AddColumnData[numColumns];
        for (int ci = 0; ci < numColumns; ++ci) {
            final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
            acd.type = dataType;
            acd.componentType = null;
            acd.chunkType = chunkType;
            acd.data = List.of(makeColumn(random));
            addColumnData[ci] = acd;
        }

        message = new BarrageMessage();
        message.isSnapshot = true;
        message.firstSeq = 0;
        message.lastSeq = 0;
        message.rowsAdded = RowSetFactory.flat(numRows);
        message.rowsIncluded = RowSetFactory.flat(numRows);
        message.rowsRemoved = RowSetFactory.empty();
        message.shifted = RowSetShiftData.EMPTY;
        message.addColumnData = addColumnData;
        message.modColumnData = BarrageMessage.ZERO_MOD_COLUMNS;

        // Prime the stateful reader with the schema so it can construct its per-column chunk readers. This message
        // carries no body and safelyParseFrom returns null for it.
        final byte[] schemaBytes = drain(WRITER_FACTORY.getSchemaView(schema::getSchema));
        deserialize(schemaBytes);

        // Serialize once for the deserialize benchmark and validate a single-batch round trip.
        serialized = serialize();
        try (final BarrageMessage roundTrip = deserialize(serialized)) {
            if (roundTrip == null || roundTrip.rowsAdded.size() != numRows) {
                throw new IllegalStateException("Round trip failed to reproduce " + numRows
                        + " rows; the message likely split into multiple record batches");
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        executionContext.close();
    }

    private static Class<?> primitiveClass(final String type) {
        switch (type) {
            case "double":
                return double.class;
            case "float":
                return float.class;
            case "long":
                return long.class;
            case "int":
                return int.class;
            case "short":
                return short.class;
            case "byte":
                return byte.class;
            default:
                throw new IllegalArgumentException("Unknown column type: " + type);
        }
    }

    private boolean nextIsNull(final Random random) {
        return nullFraction > 0.0 && random.nextDouble() < nullFraction;
    }

    private WritableChunk<Values> makeColumn(final Random random) {
        switch (columnType) {
            case "double": {
                final double[] values = new double[numRows];
                for (int ri = 0; ri < numRows; ++ri) {
                    values[ri] = nextIsNull(random) ? QueryConstants.NULL_DOUBLE : random.nextDouble();
                }
                return WritableDoubleChunk.writableChunkWrap(values);
            }
            case "float": {
                final float[] values = new float[numRows];
                for (int ri = 0; ri < numRows; ++ri) {
                    values[ri] = nextIsNull(random) ? QueryConstants.NULL_FLOAT : random.nextFloat();
                }
                return WritableFloatChunk.writableChunkWrap(values);
            }
            case "long": {
                final long[] values = new long[numRows];
                for (int ri = 0; ri < numRows; ++ri) {
                    values[ri] = nextIsNull(random) ? QueryConstants.NULL_LONG : random.nextLong();
                }
                return WritableLongChunk.writableChunkWrap(values);
            }
            case "int": {
                final int[] values = new int[numRows];
                for (int ri = 0; ri < numRows; ++ri) {
                    values[ri] = nextIsNull(random) ? QueryConstants.NULL_INT : random.nextInt();
                }
                return WritableIntChunk.writableChunkWrap(values);
            }
            case "short": {
                final short[] values = new short[numRows];
                for (int ri = 0; ri < numRows; ++ri) {
                    values[ri] = nextIsNull(random) ? QueryConstants.NULL_SHORT : (short) random.nextInt();
                }
                return WritableShortChunk.writableChunkWrap(values);
            }
            case "byte": {
                final byte[] values = new byte[numRows];
                for (int ri = 0; ri < numRows; ++ri) {
                    values[ri] = nextIsNull(random) ? QueryConstants.NULL_BYTE : (byte) random.nextInt();
                }
                return WritableByteChunk.writableChunkWrap(values);
            }
            default:
                throw new IllegalArgumentException("Unknown column type: " + columnType);
        }
    }

    private static byte[] drain(final BarrageMessageWriter.MessageView view) {
        try (final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream()) {
            view.forEachStream(stream -> {
                try {
                    stream.drainTo(out);
                    stream.close();
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            return out.toByteArray();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private byte[] serialize() {
        try (final BarrageMessageWriter writer =
                WRITER_FACTORY.newMessageWriter(message, chunkWriters,
                        BarrageMessageWriter.WriteMetricsConsumer.NO_OP)) {
            return drain(writer.getSnapshotView(options));
        }
    }

    private BarrageMessage deserialize(final byte[] bytes) {
        return reader.safelyParseFrom(options, wireChunkTypes, wireTypes, wireComponentTypes,
                new ByteArrayInputStream(bytes));
    }

    @Benchmark
    public void serialize(final Blackhole bh) {
        bh.consume(serialize());
    }

    @Benchmark
    public void deserialize(final Blackhole bh) {
        try (final BarrageMessage msg = deserialize(serialized)) {
            bh.consume(msg);
        }
    }

    @Benchmark
    public void roundTrip(final Blackhole bh) {
        try (final BarrageMessage msg = deserialize(serialize())) {
            bh.consume(msg);
        }
    }
}
