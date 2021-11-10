package io.deephaven.engine.rftable.chunkfillers.chunkfillers;

import io.deephaven.engine.v2.sources.ElementSource;
import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.chunk.ChunkType;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.structures.RowSequence;

public interface ChunkFiller {
    static ChunkFiller fromChunkType(final ChunkType chunkType) {
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

    void fillByIndices(ElementSource src, LongChunk<? extends Attributes.RowKeys> chunk,
            WritableChunk<? super Values> dest);

    void fillPrevByRanges(ElementSource src, RowSequence keys, WritableChunk<? super Values> dest);

    void fillPrevByIndices(ElementSource src, RowSequence keys, WritableChunk<? super Values> dest);

    void fillPrevByIndices(ElementSource src, LongChunk<? extends Attributes.RowKeys> chunk,
            WritableChunk<? super Values> dest);

    /**
     * This doesn't really belong here but we are putting it here for now for implementation convenience. In the long
     * run we may want to generalize this functionality, or, at the very least, move it to some "ColumnSourceFiller"
     * class.
     */
    void fillFromSingleValue(ElementSource src, long srcKey, WritableSource dest, RowSequence destKeys);
}
