package io.deephaven.engine.table.impl.ssa;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.rowset.RowSetBuilderRandom;

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
            WritableLongChunk<RowKeys> rightKeysForLeft, boolean disallowExactMatch);

    void processRemovals(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys,
            WritableLongChunk<RowKeys> priorRedirections, WritableRowRedirection rowRedirection,
            RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch);

    void processInsertion(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, Chunk<Values> nextRightValue,
            WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue,
            boolean disallowExactMatch);

    int findModified(int first, Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys,
            RowRedirection rowRedirection, Chunk<? extends Values> rightStampChunk,
            LongChunk<RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch);

    void applyShift(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta,
            WritableRowRedirection rowRedirection, boolean disallowExactMatch);
}
