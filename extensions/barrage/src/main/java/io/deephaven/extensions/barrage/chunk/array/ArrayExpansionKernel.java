//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.array;

import io.deephaven.chunk.ChunkType;
import io.deephaven.extensions.barrage.chunk.ExpansionKernel;

public interface ArrayExpansionKernel<T> extends ExpansionKernel<T> {
    /**
     * @return a kernel that expands a {@code Chunk<T[]>} to pair of {@code LongChunk, Chunk<T>}
     */
    @SuppressWarnings("unchecked")
    static <T> ArrayExpansionKernel<T> makeExpansionKernel(final ChunkType chunkType, final Class<?> componentType) {
        // Note: Internally booleans are passed around as bytes, but the wire format is packed bits.
        if (componentType == boolean.class) {
            return (ArrayExpansionKernel<T>) BooleanArrayExpansionKernel.INSTANCE;
        } else if (componentType == Boolean.class) {
            return (ArrayExpansionKernel<T>) BoxedBooleanArrayExpansionKernel.INSTANCE;
        }

        switch (chunkType) {
            case Char:
                return (ArrayExpansionKernel<T>) CharArrayExpansionKernel.INSTANCE;
            case Byte:
                return (ArrayExpansionKernel<T>) ByteArrayExpansionKernel.INSTANCE;
            case Short:
                return (ArrayExpansionKernel<T>) ShortArrayExpansionKernel.INSTANCE;
            case Int:
                return (ArrayExpansionKernel<T>) IntArrayExpansionKernel.INSTANCE;
            case Long:
                return (ArrayExpansionKernel<T>) LongArrayExpansionKernel.INSTANCE;
            case Float:
                return (ArrayExpansionKernel<T>) FloatArrayExpansionKernel.INSTANCE;
            case Double:
                return (ArrayExpansionKernel<T>) DoubleArrayExpansionKernel.INSTANCE;
            default:
                return (ArrayExpansionKernel<T>) new ObjectArrayExpansionKernel<>(componentType);
        }
    }
}
