package io.deephaven.engine.v2.ssa;

import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.utils.Index;
import io.deephaven.engine.v2.utils.RedirectionIndex;

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

    void processEntry(Chunk<Values> leftStampValues, Chunk<RowKeys> leftStampKeys, SegmentedSortedArray ssa,
            WritableLongChunk<Attributes.RowKeys> rightKeysForLeft, boolean disallowExactMatch);

    void processRemovals(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys,
            WritableLongChunk<Attributes.RowKeys> priorRedirections, RedirectionIndex redirectionIndex,
            Index.RandomBuilder modifiedBuilder, boolean disallowExactMatch);

    void processInsertion(Chunk<Values> leftStampValues, LongChunk<Attributes.RowKeys> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, Chunk<Values> nextRightValue,
            RedirectionIndex redirectionIndex, Index.RandomBuilder modifiedBuilder, boolean endsWithLastValue,
            boolean disallowExactMatch);

    int findModified(int first, Chunk<Values> leftStampValues, LongChunk<Attributes.RowKeys> leftStampKeys,
            RedirectionIndex redirectionIndex, Chunk<? extends Values> rightStampChunk,
            LongChunk<RowKeys> rightStampIndices, Index.RandomBuilder modifiedBuilder, boolean disallowExactMatch);

    void applyShift(Chunk<Values> leftStampValues, LongChunk<Attributes.RowKeys> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta,
            RedirectionIndex redirectionIndex, boolean disallowExactMatch);
}
