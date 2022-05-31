package io.deephaven.engine.table.impl.chunkfillers;

import io.deephaven.engine.table.ElementSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

public interface ChunkFiller {

    static ChunkFiller forChunkType(final ChunkType chunkType) {
        switch (chunkType) {
            case Boolean:
                return BooleanChunkFiller.INSTANCE;
            case Char:
                return CharChunkFiller.INSTANCE;
            case Byte:
                return ByteChunkFiller.INSTANCE;
            case Short:
                return ShortChunkFiller.INSTANCE;
            case Int:
                return IntChunkFiller.INSTANCE;
            case Long:
                return LongChunkFiller.INSTANCE;
            case Float:
                return FloatChunkFiller.INSTANCE;
            case Double:
                return DoubleChunkFiller.INSTANCE;
            case Object:
                return ObjectChunkFiller.INSTANCE;
            default:
                throw new UnsupportedOperationException("Unexpected chunkType " + chunkType);
        }
    }

    void fillByRanges(ElementSource src, RowSequence keys, WritableChunk<? super Values> dest);

    void fillByIndices(ElementSource src, RowSequence keys, WritableChunk<? super Values> dest);

    void fillByIndices(ElementSource src, LongChunk<? extends RowKeys> chunk,
            WritableChunk<? super Values> dest);

    void fillPrevByRanges(ElementSource src, RowSequence keys, WritableChunk<? super Values> dest);

    void fillPrevByIndices(ElementSource src, RowSequence keys, WritableChunk<? super Values> dest);

    void fillPrevByIndices(ElementSource src, LongChunk<? extends RowKeys> chunk,
            WritableChunk<? super Values> dest);

    /**
     * This doesn't really belong here but we are putting it here for now for implementation convenience. In the long
     * run we may want to generalize this functionality, or, at the very least, move it to some "ColumnSourceFiller"
     * class.
     */
    void fillFromSingleValue(ElementSource src, long srcKey, WritableColumnSource dest, RowSequence destKeys);
}
