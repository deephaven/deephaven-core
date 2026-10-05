//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.rowset.RowSetBuilderRandom;

public interface ChunkSsaStamp {
    /**
     * Make a ChunkSsaStamp for values of the given type, which must match the type of the SSAs it is given.
     *
     * @param type the chunk type of the values
     * @param equalsConsistent true when values of the data type compare equal exactly when they are equal (see
     *        {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}), which selects the
     *        EqualsConsistentObject stamp for Object values; it is the same decision passed to
     *        {@link SegmentedSortedArray#make(ChunkType, boolean, boolean, int)} for the SSAs this stamp is given.
     *        Other chunk types ignore it. An operation reads the registry once and passes the same decision to every
     *        kernel it creates, so its kernels come from one family.
     * @param reverse true for descending SSAs
     * @return the ChunkSsaStamp
     */
    static ChunkSsaStamp make(ChunkType type, boolean equalsConsistent, boolean reverse) {
        if (reverse) {
            switch (type) {
                case Char:
                    return CharReverseChunkSsaStamp.INSTANCE;
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
                    return equalsConsistent
                            ? EqualsConsistentObjectReverseChunkSsaStamp.INSTANCE
                            : ObjectReverseChunkSsaStamp.INSTANCE;
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (type) {
                case Char:
                    return CharChunkSsaStamp.INSTANCE;
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
                    return equalsConsistent
                            ? EqualsConsistentObjectChunkSsaStamp.INSTANCE
                            : ObjectChunkSsaStamp.INSTANCE;
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
