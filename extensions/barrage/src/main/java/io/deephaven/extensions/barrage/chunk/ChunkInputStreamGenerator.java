//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.base.Charsets;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.util.SafeCloseable;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

public interface ChunkInputStreamGenerator extends SafeCloseable {
    long MS_PER_DAY = 24 * 60 * 60 * 1000L;
    long MIN_LOCAL_DATE_VALUE = QueryConstants.MIN_LONG / MS_PER_DAY;
    long MAX_LOCAL_DATE_VALUE = QueryConstants.MAX_LONG / MS_PER_DAY;

    static <T> ChunkInputStreamGenerator makeInputStreamGenerator(
            final ChunkType chunkType,
            final Class<T> type,
            final Class<?> componentType,
            final Chunk<Values> chunk,
            final long rowOffset) {
        // TODO (deephaven-core#5453): pass in ArrowType to enable ser/deser of single java class in multiple formats
        switch (chunkType) {
            case Boolean:
                throw new UnsupportedOperationException("Booleans are reinterpreted as bytes");
            case Char:
                return new CharChunkInputStreamGenerator(chunk.asCharChunk(), Character.BYTES, rowOffset);
            case Byte:
                if (type == Boolean.class || type == boolean.class) {
                    // internally we represent booleans as bytes, but the wire format respects arrow's specification
                    return new BooleanChunkInputStreamGenerator(chunk.asByteChunk(), rowOffset);
                }
                return new ByteChunkInputStreamGenerator(chunk.asByteChunk(), Byte.BYTES, rowOffset);
            case Short:
                return new ShortChunkInputStreamGenerator(chunk.asShortChunk(), Short.BYTES, rowOffset);
            case Int:
                return new IntChunkInputStreamGenerator(chunk.asIntChunk(), Integer.BYTES, rowOffset);
            case Long:
                return new LongChunkInputStreamGenerator(chunk.asLongChunk(), Long.BYTES, rowOffset);
            case Float:
                return new FloatChunkInputStreamGenerator(chunk.asFloatChunk(), Float.BYTES, rowOffset);
            case Double:
                return new DoubleChunkInputStreamGenerator(chunk.asDoubleChunk(), Double.BYTES, rowOffset);
            case Object:
                if (type.isArray()) {
                    if (componentType == byte.class) {
                        return new VarBinaryChunkInputStreamGenerator<>(chunk.asObjectChunk(), rowOffset,
                                (out, item) -> out.write((byte[]) item));
                    } else {
                        return new VarListChunkInputStreamGenerator<>(type, chunk.asObjectChunk(), rowOffset);
                    }
                }
                if (Vector.class.isAssignableFrom(type)) {
                    // noinspection unchecked
                    return new VectorChunkInputStreamGenerator(
                            (Class<Vector<?>>) type, componentType, chunk.asObjectChunk(), rowOffset);
                }
                if (type == String.class) {
                    return new VarBinaryChunkInputStreamGenerator<String>(chunk.asObjectChunk(), rowOffset,
                            (out, str) -> out.write(str.getBytes(Charsets.UTF_8)));
                }
                if (type == BigInteger.class) {
                    return new VarBinaryChunkInputStreamGenerator<BigInteger>(chunk.asObjectChunk(), rowOffset,
                            (out, item) -> out.write(item.toByteArray()));
                }
                if (type == BigDecimal.class) {
                    return new VarBinaryChunkInputStreamGenerator<BigDecimal>(chunk.asObjectChunk(), rowOffset,
                            (out, item) -> {
                                final BigDecimal normal = item.stripTrailingZeros();
                                final int v = normal.scale();
                                // Write as little endian, arrow endianness.
                                out.write(0xFF & v);
                                out.write(0xFF & (v >> 8));
                                out.write(0xFF & (v >> 16));
                                out.write(0xFF & (v >> 24));
                                out.write(normal.unscaledValue().toByteArray());
                            });
                }
                if (type == Instant.class) {
                    // This code path is utilized for arrays and vectors of Instant, which cannot be reinterpreted.
                    ObjectChunk<Instant, Values> objChunk = chunk.asObjectChunk();
                    WritableLongChunk<Values> outChunk = WritableLongChunk.makeWritableChunk(objChunk.size());
                    for (int i = 0; i < objChunk.size(); ++i) {
                        outChunk.set(i, DateTimeUtils.epochNanos(objChunk.get(i)));
                    }
                    if (chunk instanceof PoolableChunk) {
                        ((PoolableChunk) chunk).close();
                    }
                    return new LongChunkInputStreamGenerator(outChunk, Long.BYTES, rowOffset);
                }
                if (type == ZonedDateTime.class) {
                    // This code path is utilized for arrays and vectors of Instant, which cannot be reinterpreted.
                    ObjectChunk<ZonedDateTime, Values> objChunk = chunk.asObjectChunk();
                    WritableLongChunk<Values> outChunk = WritableLongChunk.makeWritableChunk(objChunk.size());
                    for (int i = 0; i < objChunk.size(); ++i) {
                        outChunk.set(i, DateTimeUtils.epochNanos(objChunk.get(i)));
                    }
                    if (chunk instanceof PoolableChunk) {
                        ((PoolableChunk) chunk).close();
                    }
                    return new LongChunkInputStreamGenerator(outChunk, Long.BYTES, rowOffset);
                }
                if (type == Boolean.class) {
                    return BooleanChunkInputStreamGenerator.convertBoxed(chunk.asObjectChunk(), rowOffset);
                }
                if (type == Byte.class) {
                    return ByteChunkInputStreamGenerator.convertBoxed(chunk.asObjectChunk(), rowOffset);
                }
                if (type == Character.class) {
                    return CharChunkInputStreamGenerator.convertBoxed(chunk.asObjectChunk(), rowOffset);
                }
                if (type == Double.class) {
                    return DoubleChunkInputStreamGenerator.convertBoxed(chunk.asObjectChunk(), rowOffset);
                }
                if (type == Float.class) {
                    return FloatChunkInputStreamGenerator.convertBoxed(chunk.asObjectChunk(), rowOffset);
                }
                if (type == Integer.class) {
                    return IntChunkInputStreamGenerator.convertBoxed(chunk.asObjectChunk(), rowOffset);
                }
                if (type == Long.class) {
                    return LongChunkInputStreamGenerator.convertBoxed(chunk.asObjectChunk(), rowOffset);
                }
                if (type == Short.class) {
                    return ShortChunkInputStreamGenerator.convertBoxed(chunk.asObjectChunk(), rowOffset);
                }
                if (type == LocalDate.class) {
                    return LongChunkInputStreamGenerator.<LocalDate>convertWithTransform(chunk.asObjectChunk(),
                            rowOffset, date -> {
                                if (date == null) {
                                    return QueryConstants.NULL_LONG;
                                }
                                final long epochDay = date.toEpochDay();
                                if (epochDay < MIN_LOCAL_DATE_VALUE || epochDay > MAX_LOCAL_DATE_VALUE) {
                                    throw new IllegalArgumentException("Date out of range: " + date + " (" + epochDay
                                            + " not in [" + MIN_LOCAL_DATE_VALUE + ", " + MAX_LOCAL_DATE_VALUE + "])");
                                }
                                return epochDay * MS_PER_DAY;
                            });
                }
                if (type == LocalTime.class) {
                    return LongChunkInputStreamGenerator.<LocalTime>convertWithTransform(chunk.asObjectChunk(),
                            rowOffset, time -> {
                                if (time == null) {
                                    return QueryConstants.NULL_LONG;
                                }
                                final long nanoOfDay = time.toNanoOfDay();
                                if (nanoOfDay < 0) {
                                    throw new IllegalArgumentException("Time out of range: " + time);
                                }
                                return nanoOfDay;
                            });
                }
                // TODO (core#936): support column conversion modes

                return new VarBinaryChunkInputStreamGenerator<>(chunk.asObjectChunk(), rowOffset,
                        (out, item) -> out.write(item.toString().getBytes(Charsets.UTF_8)));
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns the number of rows that were sent before the first row in this generator.
     */
    long getRowOffset();

    /**
     * Returns the offset of the final row this generator can produce.
     */
    long getLastRowOffset();

    /**
     * Get an input stream optionally position-space filtered using the provided RowSet.
     *
     * @param options the serializable options for this subscription
     * @param subset if provided, is a position-space filter of source data
     * @return a single-use DrainableColumn ready to be drained via grpc
     */
    DrainableColumn getInputStream(final StreamReaderOptions options, @Nullable final RowSet subset) throws IOException;

    final class FieldNodeInfo {
        public final int numElements;
        public final int nullCount;

        public FieldNodeInfo(final int numElements, final int nullCount) {
            this.numElements = numElements;
            this.nullCount = nullCount;
        }

        public FieldNodeInfo(final org.apache.arrow.flatbuf.FieldNode node) {
            this(LongSizedDataStructure.intSize("FieldNodeInfo", node.length()),
                    LongSizedDataStructure.intSize("FieldNodeInfo", node.nullCount()));
        }
    }

    @FunctionalInterface
    interface FieldNodeListener {
        void noteLogicalFieldNode(final int numElements, final int nullCount);
    }

    @FunctionalInterface
    interface BufferListener {
        void noteLogicalBuffer(final long length);
    }

    abstract class DrainableColumn extends DefensiveDrainable {
        /**
         * Append the field nde to the flatbuffer payload via the supplied listener.
         * 
         * @param listener the listener to notify for each logical field node in this payload
         */
        public abstract void visitFieldNodes(final FieldNodeListener listener);

        /**
         * Append the buffer boundaries to the flatbuffer payload via the supplied listener.
         * 
         * @param listener the listener to notify for each sub-buffer in this payload
         */
        public abstract void visitBuffers(final BufferListener listener);

        /**
         * Count the number of null elements in the outer-most layer of this column (i.e. does not count nested nulls
         * inside of arrays)
         * 
         * @return the number of null elements -- 'useDeephavenNulls' counts are always 0 so that we may omit the
         *         validity buffer
         */
        public abstract int nullCount();
    }
}
