/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.chunk.vector;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.vector.BooleanVector;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.Vector;

public interface VectorExpansionKernel {

    static Class<?> getComponentType(final Class<?> type, final Class<?> componentType) {
        if (BooleanVector.class.isAssignableFrom(type)) {
            return boolean.class;
        }
        if (ByteVector.class.isAssignableFrom(type)) {
            return byte.class;
        }
        if (CharVector.class.isAssignableFrom(type)) {
            return char.class;
        }
        if (DoubleVector.class.isAssignableFrom(type)) {
            return double.class;
        }
        if (FloatVector.class.isAssignableFrom(type)) {
            return float.class;
        }
        if (IntVector.class.isAssignableFrom(type)) {
            return int.class;
        }
        if (LongVector.class.isAssignableFrom(type)) {
            return long.class;
        }
        if (ShortVector.class.isAssignableFrom(type)) {
            return short.class;
        }
        if (ObjectVector.class.isAssignableFrom(type)) {
            return componentType != null ? componentType : Object.class;
        }
        throw new IllegalStateException("Unexpected vector type: " + type.getCanonicalName());
    }

    /**
     * @return a kernel that expands a {@code Chunk<VectorT>} to pair of {@code LongChunk, Chunk<T>}
     */
    static <T> VectorExpansionKernel makeExpansionKernel(final ChunkType chunkType, final Class<T> componentType) {
        switch (chunkType) {
            case Char:
                return CharVectorExpansionKernel.INSTANCE;
            case Byte:
                return ByteVectorExpansionKernel.INSTANCE;
            case Short:
                return ShortVectorExpansionKernel.INSTANCE;
            case Int:
                return IntVectorExpansionKernel.INSTANCE;
            case Long:
                return LongVectorExpansionKernel.INSTANCE;
            case Float:
                return FloatVectorExpansionKernel.INSTANCE;
            case Double:
                return DoubleVectorExpansionKernel.INSTANCE;
            case Boolean:
                return BooleanVectorExpansionKernel.INSTANCE;
            default:
                return new ObjectVectorExpansionKernel<>(componentType);
        }
    }

    /**
     * This expands the source from a {@code TVector} per element to a flat {@code T} per element. The kernel records
     * the number of consecutive elements that belong to a row in {@code perElementLengthDest}. The returned chunk is
     * owned by the caller.
     *
     * @param source the source chunk of TVector to expand
     * @param perElementLengthDest the destination IntChunk for which {@code dest.get(i + 1) - dest.get(i)} is
     *        equivalent to {@code source.get(i).length}
     * @return an unrolled/flattened chunk of T
     */
    <A extends Any> WritableChunk<A> expand(ObjectChunk<Vector<?>, A> source,
            WritableIntChunk<ChunkPositions> perElementLengthDest);

    /**
     * This contracts the source from a pair of {@code LongChunk} and {@code Chunk<T>} and produces a
     * {@code Chunk<T[]>}. The returned chunk is owned by the caller.
     *
     * @param source the source chunk of T to contract
     * @param perElementLengthDest the source IntChunk for which {@code dest.get(i + 1) - dest.get(i)} is equivalent to
     *        {@code source.get(i).length}
     * @param outChunk the returned chunk from an earlier record batch
     * @param outOffset the offset to start writing into {@code outChunk}
     * @param totalRows the total known rows for this column; if known (else 0)
     * @return a result chunk of T[]
     */
    <A extends Any> WritableObjectChunk<Vector<?>, A> contract(
            Chunk<A> source, IntChunk<ChunkPositions> perElementLengthDest,
            WritableChunk<A> outChunk, int outOffset, int totalRows);
}
