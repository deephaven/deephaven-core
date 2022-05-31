package io.deephaven.engine.table.impl.util.reverse;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

public interface ReverseKernel {
    static ReverseKernel makeReverseKernel(ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharReverseKernel.INSTANCE;
            case Byte:
                return ByteReverseKernel.INSTANCE;
            case Short:
                return ShortReverseKernel.INSTANCE;
            case Int:
                return IntReverseKernel.INSTANCE;
            case Long:
                return LongReverseKernel.INSTANCE;
            case Float:
                return FloatReverseKernel.INSTANCE;
            case Double:
                return DoubleReverseKernel.INSTANCE;
            default:
                return ObjectReverseKernel.INSTANCE;
        }
    }

    /**
     * Reverse chunk values in place.
     *
     * @param chunkToReverse the chunk to reverse
     */
    <T extends Any> void reverse(WritableChunk<T> chunkToReverse);
}
