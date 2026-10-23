//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.fill;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Fills a range of a chunk with one element of another chunk of the same type, without boxing the element.
 */
public interface FillFromElementKernel {

    static FillFromElementKernel makeFillFromElementKernel(ChunkType chunkType) {
        switch (chunkType) {
            case Char:
                return CharFillFromElementKernel.INSTANCE;
            case Byte:
                return ByteFillFromElementKernel.INSTANCE;
            case Short:
                return ShortFillFromElementKernel.INSTANCE;
            case Int:
                return IntFillFromElementKernel.INSTANCE;
            case Long:
                return LongFillFromElementKernel.INSTANCE;
            case Float:
                return FloatFillFromElementKernel.INSTANCE;
            case Double:
                return DoubleFillFromElementKernel.INSTANCE;
            case Boolean:
                return BooleanFillFromElementKernel.INSTANCE;
            case Object:
                return ObjectFillFromElementKernel.INSTANCE;
            default:
                throw new UnsupportedOperationException("Unexpected chunkType " + chunkType);
        }
    }

    /**
     * Set positions {@code [destOffset, destOffset + length)} of {@code dest} to the element at position
     * {@code srcOffset} of {@code src}.
     *
     * @param dest the chunk to fill, whose capacity must cover the filled positions
     * @param destOffset the first position of {@code dest} to fill
     * @param length the number of positions to fill
     * @param src the chunk holding the element, of the same type as {@code dest}
     * @param srcOffset the position of the element in {@code src}
     */
    void fillFromElement(WritableChunk<? extends Any> dest, int destOffset, int length, Chunk<? extends Any> src,
            int srcOffset);
}
