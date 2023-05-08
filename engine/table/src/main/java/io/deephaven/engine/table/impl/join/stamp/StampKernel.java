/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.join.stamp;

import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

public interface StampKernel extends Context {
    static StampKernel makeStampKernel(ChunkType type, SortingOrder order, boolean disallowExactMatch) {
        if (disallowExactMatch) {
            if (order == SortingOrder.Descending) {
                return makeReverseStampKernelNoExact(type);
            } else {
                return makeStampKernelNoExact(type);
            }
        } else {
            if (order == SortingOrder.Descending) {
                return makeReverseStampKernel(type);
            } else {
                return makeStampKernel(type);
            }
        }
    }

    @NotNull
    static StampKernel makeStampKernel(ChunkType type) {
        switch (type) {
            case Char:
                return NullAwareCharStampKernel.INSTANCE;
            case Byte:
                return ByteStampKernel.INSTANCE;
            case Short:
                return ShortStampKernel.INSTANCE;
            case Int:
                return IntStampKernel.INSTANCE;
            case Long:
                return LongStampKernel.INSTANCE;
            case Float:
                return FloatStampKernel.INSTANCE;
            case Double:
                return DoubleStampKernel.INSTANCE;
            case Object:
                return ObjectStampKernel.INSTANCE;
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    @NotNull
    static StampKernel makeStampKernelNoExact(ChunkType type) {
        switch (type) {
            case Char:
                return NullAwareCharNoExactStampKernel.INSTANCE;
            case Byte:
                return ByteNoExactStampKernel.INSTANCE;
            case Short:
                return ShortNoExactStampKernel.INSTANCE;
            case Int:
                return IntNoExactStampKernel.INSTANCE;
            case Long:
                return LongNoExactStampKernel.INSTANCE;
            case Float:
                return FloatNoExactStampKernel.INSTANCE;
            case Double:
                return DoubleNoExactStampKernel.INSTANCE;
            case Object:
                return ObjectNoExactStampKernel.INSTANCE;
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    @NotNull
    static StampKernel makeReverseStampKernel(ChunkType type) {
        switch (type) {
            case Char:
                return NullAwareCharReverseStampKernel.INSTANCE;
            case Byte:
                return ByteReverseStampKernel.INSTANCE;
            case Short:
                return ShortReverseStampKernel.INSTANCE;
            case Int:
                return IntReverseStampKernel.INSTANCE;
            case Long:
                return LongReverseStampKernel.INSTANCE;
            case Float:
                return FloatReverseStampKernel.INSTANCE;
            case Double:
                return DoubleReverseStampKernel.INSTANCE;
            case Object:
                return ObjectReverseStampKernel.INSTANCE;
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    @NotNull
    static StampKernel makeReverseStampKernelNoExact(ChunkType type) {
        switch (type) {
            case Char:
                return NullAwareCharNoExactReverseStampKernel.INSTANCE;
            case Byte:
                return ByteNoExactReverseStampKernel.INSTANCE;
            case Short:
                return ShortNoExactReverseStampKernel.INSTANCE;
            case Int:
                return IntNoExactReverseStampKernel.INSTANCE;
            case Long:
                return LongNoExactReverseStampKernel.INSTANCE;
            case Float:
                return FloatNoExactReverseStampKernel.INSTANCE;
            case Double:
                return DoubleNoExactReverseStampKernel.INSTANCE;
            case Object:
                return ObjectNoExactReverseStampKernel.INSTANCE;
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Stamps the left-hand-side values with the corresponding right hand side.
     * <p>
     * The rightKeyIndices are parallel to the stamp values in rightStamps; and used to compute a new chunk of
     * redirections parallel to leftStamps.
     *
     * @param leftStamps the input lhs stamp values
     * @param rightStamps the input rhs stamp values
     * @param rightKeyIndices the input rhs stamp indices
     * @param leftRedirections the resulting redirections from the stamping operation
     */
    void computeRedirections(
            Chunk<Values> leftStamps,
            Chunk<Values> rightStamps,
            LongChunk<RowKeys> rightKeyIndices,
            WritableLongChunk<RowKeys> leftRedirections);
}
