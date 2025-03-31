//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.array;

import io.deephaven.chunk.ChunkType;
import io.deephaven.extensions.barrage.chunk.ExpansionKernel;

/**
 * The {@code ArrayExpansionKernel} interface provides a mechanism for expanding chunks containing arrays into a pair of
 * {@code LongChunk} and {@code Chunk<T>}, enabling efficient handling of array-typed columnar data. This interface is
 * part of the Deephaven Barrage extensions for processing structured data in Flight/Barrage streams.
 * <p>
 * An {@code ArrayExpansionKernel} is specialized for handling array-like data, where each element in the source chunk
 * may itself be an array. The kernel performs the transformation to a flattened format, suitable for further processing
 * or serialization.
 *
 * @param <T> The type of elements within the array being expanded.
 */
public interface ArrayExpansionKernel<T> extends ExpansionKernel<T> {
    /**
     * Creates an {@code ArrayExpansionKernel} for the specified {@link ChunkType} and component type.
     * <p>
     * The implementation is chosen based on the provided {@code chunkType} and {@code componentType}, with specialized
     * kernels for primitive types and boxed types, including {@code boolean} handling for packed bit representations.
     *
     * @param chunkType The {@link ChunkType} representing the type of data in the chunk.
     * @param componentType The class of the component type within the array.
     * @param <T> The type of elements within the array being expanded.
     * @return An {@code ArrayExpansionKernel} capable of expanding chunks of the specified type.
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
