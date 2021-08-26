package io.deephaven.db.v2.ssa;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.RedirectionIndex;

public interface ChunkSsaStamp {
    static ChunkSsaStamp make(ChunkType type, boolean reverse) {
        if (reverse) {
            switch (type) {
                case Char:
                    return NullAwareCharReverseChunkSsaStamp.INSTANCE;
                case Byte:
                    return ByteReverseChunkSsaStamp.INSTANCE;
                case Short:
                    return ShortReverseChunkSsaStamp.INSTANCE;
                case Int:
                    return IntReverseChunkSsaStamp.INSTANCE;
                case Long:
                    return LongReverseChunkSsaStamp.INSTANCE;
                case Float:
                    return FloatReverseChunkSsaStamp.INSTANCE;
                case Double:
                    return DoubleReverseChunkSsaStamp.INSTANCE;
                case Object:
                    return ObjectReverseChunkSsaStamp.INSTANCE;
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (type) {
                case Char:
                    return NullAwareCharChunkSsaStamp.INSTANCE;
                case Byte:
                    return ByteChunkSsaStamp.INSTANCE;
                case Short:
                    return ShortChunkSsaStamp.INSTANCE;
                case Int:
                    return IntChunkSsaStamp.INSTANCE;
                case Long:
                    return LongChunkSsaStamp.INSTANCE;
                case Float:
                    return FloatChunkSsaStamp.INSTANCE;
                case Double:
                    return DoubleChunkSsaStamp.INSTANCE;
                case Object:
                    return ObjectChunkSsaStamp.INSTANCE;
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        }
    }

    void processEntry(Chunk<Values> leftStampValues, Chunk<KeyIndices> leftStampKeys, SegmentedSortedArray ssa,
            WritableLongChunk<KeyIndices> rightKeysForLeft, boolean disallowExactMatch);

    void processRemovals(Chunk<Values> leftStampValues, LongChunk<KeyIndices> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<KeyIndices> rightKeys,
            WritableLongChunk<KeyIndices> priorRedirections, RedirectionIndex redirectionIndex,
            Index.RandomBuilder modifiedBuilder, boolean disallowExactMatch);

    void processInsertion(Chunk<Values> leftStampValues, LongChunk<KeyIndices> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<KeyIndices> rightKeys, Chunk<Values> nextRightValue,
            RedirectionIndex redirectionIndex, Index.RandomBuilder modifiedBuilder, boolean endsWithLastValue,
            boolean disallowExactMatch);

    int findModified(int first, Chunk<Values> leftStampValues, LongChunk<KeyIndices> leftStampKeys,
            RedirectionIndex redirectionIndex, Chunk<? extends Values> rightStampChunk,
            LongChunk<KeyIndices> rightStampIndices, Index.RandomBuilder modifiedBuilder, boolean disallowExactMatch);

    void applyShift(Chunk<Values> leftStampValues, LongChunk<KeyIndices> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<KeyIndices> rightStampKeys, long shiftDelta,
            RedirectionIndex redirectionIndex, boolean disallowExactMatch);
}
