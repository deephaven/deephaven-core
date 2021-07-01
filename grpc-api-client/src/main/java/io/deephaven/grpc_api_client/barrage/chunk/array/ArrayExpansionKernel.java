/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.barrage.chunk.array;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.IntChunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;

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
    <T, A extends Attributes.Any> WritableChunk<A> expand(ObjectChunk<T, A> source, WritableIntChunk<Attributes.ChunkPositions> perElementLengthDest);

    /**
     * This contracts the source from a pair of `LongChunk` and `Chunk<T>` and produces a `Chunk<T[]>`. The returned
     * chunk is owned by the caller.
     *
     * @param source the source chunk of T to contract
     * @param perElementLengthDest the source IntChunk for which `dest.get(i + 1) - dest.get(i)` is equivalent to `source.get(i).length`
     * @return a result chunk of T[]
     */
    <T, A extends Attributes.Any> WritableObjectChunk<T, A> contract(Chunk<A> source, IntChunk<Attributes.ChunkPositions> perElementLengthDest);
}
