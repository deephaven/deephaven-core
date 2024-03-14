//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.cast;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.SafeCloseable;

import java.math.BigDecimal;
import java.math.BigInteger;

public interface ToBigDecimalCast extends SafeCloseable {
    <T> ObjectChunk<BigDecimal, ? extends Any> cast(Chunk<? extends T> input);

    /**
     * Create a kernel that converts the values in an input chunk to a BigDecimal.
     *
     * @param type the type of chunk, can be an integral primitive type or BigInteger/BigDecimal
     * @param size the size of the largest chunk that can be cast by this kernel
     *
     * @return a {@link ToBigDecimalCast} that can be applied to chunks of type in order to produce a DoubleChunk of
     *         values
     */
    static ToBigDecimalCast makeToBigDecimalCast(ChunkType type, Class<?> sourceType, int size) {
        switch (type) {
            case Byte:
                return new ByteToBigDecimalCast(size);
            case Char:
                return new CharToBigDecimalCast(size);
            case Short:
                return new ShortToBigDecimalCast(size);
            case Int:
                return new IntToBigDecimalCast(size);
            case Long:
                return new LongToBigDecimalCast(size);
            case Float:
                return new FloatToBigDecimalCast(size);
            case Double:
                return new DoubleToBigDecimalCast(size);

            case Boolean:
            case Object:
                if (sourceType == BigInteger.class) {
                    return new BigNumberToBigDecimalCast(size);
                } else if (sourceType == BigDecimal.class) {
                    return IDENTITY;
                }
                throw new UnsupportedOperationException("ToBigDecimalCast column type not supported: " + sourceType);
        }
        throw new UnsupportedOperationException("Can not make toBigDecimalCast for " + type);
    }

    class Identity implements ToBigDecimalCast {
        @Override
        public <T> ObjectChunk<BigDecimal, ? extends T> cast(Chunk<? extends T> input) {
            return input.asObjectChunk();
        }

        @Override
        public void close() {}
    }

    ToBigDecimalCast IDENTITY = new Identity();
}
