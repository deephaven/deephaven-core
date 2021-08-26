package io.deephaven.db.v2.join.dupcompact;

import io.deephaven.db.v2.sources.chunk.*;

public interface DupCompactKernel extends Context {
    static DupCompactKernel makeDupCompact(ChunkType chunkType, boolean reverse) {
        if (reverse) {
            switch (chunkType) {
                case Char:
                    return NullAwareCharReverseDupCompactKernel.INSTANCE;
                case Byte:
                    return ByteReverseDupCompactKernel.INSTANCE;
                case Short:
                    return ShortReverseDupCompactKernel.INSTANCE;
                case Int:
                    return IntReverseDupCompactKernel.INSTANCE;
                case Long:
                    return LongReverseDupCompactKernel.INSTANCE;
                case Float:
                    return FloatReverseDupCompactKernel.INSTANCE;
                case Double:
                    return DoubleReverseDupCompactKernel.INSTANCE;
                case Object:
                    return ObjectReverseDupCompactKernel.INSTANCE;
                case Boolean:
                default:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (chunkType) {
                case Char:
                    return NullAwareCharDupCompactKernel.INSTANCE;
                case Byte:
                    return ByteDupCompactKernel.INSTANCE;
                case Short:
                    return ShortDupCompactKernel.INSTANCE;
                case Int:
                    return IntDupCompactKernel.INSTANCE;
                case Long:
                    return LongDupCompactKernel.INSTANCE;
                case Float:
                    return FloatDupCompactKernel.INSTANCE;
                case Double:
                    return DoubleDupCompactKernel.INSTANCE;
                case Object:
                    return ObjectDupCompactKernel.INSTANCE;
                case Boolean:
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * Remove all adjacent values from chunkToCompact, except the last value in any adjacent run. The keyIndices are
     * parallel to the chunkToCompact; and when a value is removed from chunkToCompact it is also removed from
     * keyIndices
     *
     * Additionally, verify that the elements are properly ordered; returning the first position of an out of order
     * element.
     *
     * @param chunkToCompact the values to remove duplicates from
     * @param keyIndices the key indices parallel to chunkToCompact
     *
     * @return the first position of an out-of-order element, or -1 if all elements are in order
     */
    int compactDuplicates(WritableChunk<? extends Attributes.Any> chunkToCompact,
            WritableLongChunk<Attributes.KeyIndices> keyIndices);
}
