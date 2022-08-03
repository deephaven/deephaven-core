/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.chunk.array;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

public interface ArrayExpansionKernel {
    /**
     * @return a kernel that expands a {@code Chunk<T[]>} to pair of {@code LongChunk, Chunk<T>}
     */
    static ArrayExpansionKernel makeExpansionKernel(final ChunkType chunkType, final Class<?> componentType) {
        switch (chunkType) {
            case Char:
                return CharArrayExpansionKernel.INSTANCE;
            case Byte:
                return ByteArrayExpansionKernel.INSTANCE;
            case Short:
                return ShortArrayExpansionKernel.INSTANCE;
            case Int:
                return IntArrayExpansionKernel.INSTANCE;
            case Long:
                return LongArrayExpansionKernel.INSTANCE;
            case Float:
                return FloatArrayExpansionKernel.INSTANCE;
            case Double:
                return DoubleArrayExpansionKernel.INSTANCE;
            default:
                return new ObjectArrayExpansionKernel(componentType);
        }
    }

    /**
     * This expands the source from a {@code T[]} per element to a flat {@code T} per element. The kernel records the number of
     * consecutive elements that belong to a row in {@code perElementLengthDest}. The returned chunk is owned by the caller.
     *
     * @param source the source chunk of T[] to expand
     * @param perElementLengthDest the destination IntChunk for which {@code dest.get(i + 1) - dest.get(i)} is equivalent to {@code source.get(i).length}
     * @return an unrolled/flattened chunk of T
     */
    <T, A extends Any> WritableChunk<A> expand(ObjectChunk<T, A> source, WritableIntChunk<ChunkPositions> perElementLengthDest);

    /**
     * This contracts the source from a pair of {@code LongChunk} and {@code Chunk<T>} and produces a {@code Chunk<T[]>}. The returned
     * chunk is owned by the caller.
     *
     * @param source the source chunk of T to contract
     * @param perElementLengthDest the source IntChunk for which {@code dest.get(i + 1) - dest.get(i)} is equivalent to {@code source.get(i).length}
     * @param outChunk the returned chunk from an earlier record batch
     * @param outOffset the offset to start writing into {@code outChunk}
     * @param totalRows the total known rows for this column; if known (else 0)
     * @return a result chunk of T[]
     */
    <T, A extends Any> WritableObjectChunk<T, A> contract(
            Chunk<A> source, IntChunk<ChunkPositions> perElementLengthDest, WritableChunk<A> outChunk, int outOffset, int totalRows);
}
