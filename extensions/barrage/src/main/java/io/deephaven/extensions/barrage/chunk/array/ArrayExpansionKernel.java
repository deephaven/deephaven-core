/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
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
     * @return a kernel that expands a `Chunk<T[]>` to pair of `LongChunk, Chunk<T>`
     */
    static ArrayExpansionKernel makeExpansionKernel(final ChunkType chunkType) {
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
                return ObjectArrayExpansionKernel.INSTANCE;
        }
    }

    /**
     * This expands the source from a `T[]` per element to a flat `T` per element. The kernel records the number of
     * consecutive elements that belong to a row in `perElementLengthDest`. The returned chunk is owned by the caller.
     *
     * @param source the source chunk of T[] to expand
     * @param perElementLengthDest the destination IntChunk for which `dest.get(i + 1) - dest.get(i)` is equivalent to `source.get(i).length`
     * @return an unrolled/flattened chunk of T
     */
    <T, A extends Any> WritableChunk<A> expand(ObjectChunk<T, A> source, WritableIntChunk<ChunkPositions> perElementLengthDest);

    /**
     * This contracts the source from a pair of `LongChunk` and `Chunk<T>` and produces a `Chunk<T[]>`. The returned
     * chunk is owned by the caller.
     *
     * @param source the source chunk of T to contract
     * @param perElementLengthDest the source IntChunk for which `dest.get(i + 1) - dest.get(i)` is equivalent to `source.get(i).length`
     * @return a result chunk of T[]
     */
    <T, A extends Any> WritableObjectChunk<T, A> contract(Chunk<A> source, IntChunk<ChunkPositions> perElementLengthDest);
}
