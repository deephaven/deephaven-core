package io.deephaven.engine.v2.sortcheck;

import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.ChunkType;

public interface SortCheck {
    /**
     * Verify that the provided values are in order.
     *
     * @param valuesToCheck a chunk of values to check for out of order elements
     * @return the first position of an out-of-order element, or -1 if all elements are in order
     */
    int sortCheck(Chunk<? extends Attributes.Values> valuesToCheck);

    static SortCheck make(ChunkType chunkType, boolean reverse) {
        switch (chunkType) {
            case Char:
                return reverse ? CharReverseSortCheck.INSTANCE : CharSortCheck.INSTANCE;
            case Byte:
                return reverse ? ByteReverseSortCheck.INSTANCE : ByteSortCheck.INSTANCE;
            case Short:
                return reverse ? ShortReverseSortCheck.INSTANCE : ShortSortCheck.INSTANCE;
            case Int:
                return reverse ? IntReverseSortCheck.INSTANCE : IntSortCheck.INSTANCE;
            case Long:
                return reverse ? LongReverseSortCheck.INSTANCE : LongSortCheck.INSTANCE;
            case Float:
                return reverse ? FloatReverseSortCheck.INSTANCE : FloatSortCheck.INSTANCE;
            case Double:
                return reverse ? DoubleReverseSortCheck.INSTANCE : DoubleSortCheck.INSTANCE;
            case Object:
                return reverse ? ObjectReverseSortCheck.INSTANCE : ObjectSortCheck.INSTANCE;
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }
}
