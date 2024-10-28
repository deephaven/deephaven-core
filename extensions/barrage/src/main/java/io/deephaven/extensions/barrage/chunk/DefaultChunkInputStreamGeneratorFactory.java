//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.base.Charsets;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.PoolableChunk;
import io.deephaven.extensions.barrage.util.ArrowIpcUtil;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.vector.Vector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import static io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator.MAX_LOCAL_DATE_VALUE;
import static io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator.MIN_LOCAL_DATE_VALUE;
import static io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator.MS_PER_DAY;

/**
 * JVM implementation of ChunkInputStreamGenerator.Factory, suitable for use in Java clients and servers.
 */
public class DefaultChunkInputStreamGeneratorFactory implements ChunkInputStreamGenerator.Factory {
    public static final DefaultChunkInputStreamGeneratorFactory INSTANCE =
            new DefaultChunkInputStreamGeneratorFactory();

    @Override
    public <T> ChunkInputStreamGenerator makeInputStreamGenerator(ChunkType chunkType, Class<T> type,
            Class<?> componentType, Chunk<Values> chunk, long rowOffset) {
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
                        return new VarListChunkInputStreamGenerator<>(this, type, chunk.asObjectChunk(), rowOffset);
                    }
                }
                if (Vector.class.isAssignableFrom(type)) {
                    // noinspection unchecked
                    return new VectorChunkInputStreamGenerator(this,
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
                // TODO (core#58): add custom barrage serialization/deserialization support
                // Migrate Schema to custom format when available.
                if (type == Schema.class) {
                    return new VarBinaryChunkInputStreamGenerator<>(chunk.asObjectChunk(), rowOffset,
                            ArrowIpcUtil::serialize);
                }
                // TODO (core#936): support column conversion modes
                return new VarBinaryChunkInputStreamGenerator<>(chunk.asObjectChunk(), rowOffset,
                        (out, item) -> out.write(item.toString().getBytes(Charsets.UTF_8)));
            default:
                throw new UnsupportedOperationException();
        }
    }
}
