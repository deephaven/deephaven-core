package io.deephaven.db.v2.ssa;

import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.RedirectionIndex;

public interface SsaSsaStamp {
    static SsaSsaStamp make(ChunkType type, boolean reverse) {
        if (reverse) {
            switch (type) {
                case Char:
                    return NullAwareCharReverseSsaSsaStamp.INSTANCE;
                case Byte:
                    return ByteReverseSsaSsaStamp.INSTANCE;
                case Short:
                    return ShortReverseSsaSsaStamp.INSTANCE;
                case Int:
                    return IntReverseSsaSsaStamp.INSTANCE;
                case Long:
                    return LongReverseSsaSsaStamp.INSTANCE;
                case Float:
                    return FloatReverseSsaSsaStamp.INSTANCE;
                case Double:
                    return DoubleReverseSsaSsaStamp.INSTANCE;
                case Object:
                    return ObjectReverseSsaSsaStamp.INSTANCE;
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (type) {
                case Char:
                    return NullAwareCharSsaSsaStamp.INSTANCE;
                case Byte:
                    return ByteSsaSsaStamp.INSTANCE;
                case Short:
                    return ShortSsaSsaStamp.INSTANCE;
                case Int:
                    return IntSsaSsaStamp.INSTANCE;
                case Long:
                    return LongSsaSsaStamp.INSTANCE;
                case Float:
                    return FloatSsaSsaStamp.INSTANCE;
                case Double:
                    return DoubleSsaSsaStamp.INSTANCE;
                case Object:
                    return ObjectSsaSsaStamp.INSTANCE;
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        }
    }

    void processEntry(SegmentedSortedArray leftSsa, SegmentedSortedArray ssa,
        RedirectionIndex redirectionIndex, boolean disallowExactMatch);

    void processRemovals(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk,
        LongChunk<KeyIndices> rightKeys, WritableLongChunk<KeyIndices> priorRedirections,
        RedirectionIndex redirectionIndex, Index.RandomBuilder modifiedBuilder,
        boolean disallowExactMatch);

    void processInsertion(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk,
        LongChunk<KeyIndices> rightKeys, Chunk<Values> nextRightValue,
        RedirectionIndex redirectionIndex, Index.RandomBuilder modifiedBuilder,
        boolean endsWithLastValue, boolean disallowExactMatch);

    void findModified(SegmentedSortedArray leftSsa, RedirectionIndex redirectionIndex,
        Chunk<? extends Values> rightStampChunk, LongChunk<KeyIndices> rightStampIndices,
        Index.RandomBuilder modifiedBuilder, boolean disallowExactMatch);

    void applyShift(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk,
        LongChunk<KeyIndices> rightStampKeys, long shiftDelta, RedirectionIndex redirectionIndex,
        boolean disallowExactMatch);
}
