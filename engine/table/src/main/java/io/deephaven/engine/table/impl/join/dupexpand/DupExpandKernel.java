package io.deephaven.engine.table.impl.join.dupexpand;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;

public interface DupExpandKernel {

    static DupExpandKernel makeDupExpand(ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharDupExpandKernel.INSTANCE;
            case Byte:
                return ByteDupExpandKernel.INSTANCE;
            case Short:
                return ShortDupExpandKernel.INSTANCE;
            case Int:
                return IntDupExpandKernel.INSTANCE;
            case Long:
                return LongDupExpandKernel.INSTANCE;
            case Float:
                return FloatDupExpandKernel.INSTANCE;
            case Double:
                return DoubleDupExpandKernel.INSTANCE;
            case Object:
                return ObjectDupExpandKernel.INSTANCE;
            case Boolean:
                return BooleanDupExpandKernel.INSTANCE;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Expands entries values from chunkToExpand in-place, using keyRunLengths to determine how many copies. The
     * keyRunLengths chunk is parallel to the original chunkToExpand; it is never modified.
     *
     * @param expandedSize the sum of all entries in keyRunLengths
     * @param chunkToExpand the values to expand in-place (this writable chunk must have capacity >= expandedSize)
     * @param keyRunLengths the key run-lengths parallel to chunkToExpand
     */
    void expandDuplicates(int expandedSize, WritableChunk<? extends Any> chunkToExpand,
            IntChunk<ChunkLengths> keyRunLengths);
}
