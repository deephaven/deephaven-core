package io.deephaven.engine.structures.chunk.util.chunkfillers;

import io.deephaven.engine.structures.chunk.Attributes.KeyIndices;
import io.deephaven.engine.structures.chunk.Attributes.Values;
import io.deephaven.engine.structures.chunk.ChunkType;
import io.deephaven.engine.structures.chunk.LongChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.structures.rowsequence.OrderedKeys;
import io.deephaven.engine.structures.source.ElementSource;
import io.deephaven.engine.structures.source.WritableSource;

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

    void fillByRanges(ElementSource src, OrderedKeys keys, WritableChunk<? super Values> dest);

    void fillByIndices(ElementSource src, OrderedKeys keys, WritableChunk<? super Values> dest);

    void fillByIndices(ElementSource src, LongChunk<? extends KeyIndices> chunk, WritableChunk<? super Values> dest);

    void fillPrevByRanges(ElementSource src, OrderedKeys keys, WritableChunk<? super Values> dest);

    void fillPrevByIndices(ElementSource src, OrderedKeys keys, WritableChunk<? super Values> dest);

    void fillPrevByIndices(ElementSource src, LongChunk<? extends KeyIndices> chunk,
            WritableChunk<? super Values> dest);

    /**
     * This doesn't really belong here but we are putting it here for now for implementation convenience. In the long
     * run we may want to generalize this functionality, or, at the very least, move it to some "ColumnSourceFiller"
     * class.
     */
    void fillFromSingleValue(ElementSource src, long srcKey, WritableSource dest, OrderedKeys destKeys);
}
