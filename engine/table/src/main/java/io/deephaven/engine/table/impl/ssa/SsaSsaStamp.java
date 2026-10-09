//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.rowset.RowSetBuilderRandom;

public interface SsaSsaStamp {
    /**
     * Make a SsaSsaStamp for values of the given type, which must match the type of the SSAs it is given.
     *
     * @param type the chunk type of the values
     * @param equalsConsistent true when values of the data type compare equal exactly when they are equal (see
     *        {@link BinarySearchKernelHelper#compareConsistentWithEquality(Class)}), which selects the
     *        EqualsConsistentObject stamp for Object values; it is the same decision passed to
     *        {@link SegmentedSortedArray#make(ChunkType, boolean, boolean, int)} for the SSAs this stamp is given.
     *        Other chunk types ignore it. An operation reads the registry once and passes the same decision to every
     *        kernel it creates, so its kernels come from one family.
     * @param reverse true for descending SSAs
     * @return the SsaSsaStamp
     */
    static SsaSsaStamp make(ChunkType type, boolean equalsConsistent, boolean reverse) {
        if (reverse) {
            switch (type) {
                case Char:
                    return CharReverseSsaSsaStamp.INSTANCE;
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
                    return equalsConsistent
                            ? EqualsConsistentObjectReverseSsaSsaStamp.INSTANCE
                            : ObjectReverseSsaSsaStamp.INSTANCE;
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        } else {
            switch (type) {
                case Char:
                    return CharSsaSsaStamp.INSTANCE;
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
                    return equalsConsistent
                            ? EqualsConsistentObjectSsaSsaStamp.INSTANCE
                            : ObjectSsaSsaStamp.INSTANCE;
                default:
                case Boolean:
                    throw new UnsupportedOperationException();
            }
        }
    }

    void processEntry(SegmentedSortedArray leftSsa, SegmentedSortedArray ssa, WritableRowRedirection rowRedirection,
            boolean disallowExactMatch);

    /**
     * Restamps the left rows that the removed right rows matched and adds them to {@code modifiedBuilder}.
     *
     * @param modifiedKeys scratch space for the modified left row keys, owned by the caller and reused across calls
     */
    void processRemovals(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk,
            LongChunk<RowKeys> rightKeys, WritableLongChunk<RowKeys> priorRedirections,
            WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder,
            SizedLongChunk<RowKeys> modifiedKeys, boolean disallowExactMatch);

    /**
     * Restamps the left rows that the inserted right rows now match and adds them to {@code modifiedBuilder}.
     *
     * @param modifiedKeys scratch space for the modified left row keys, owned by the caller and reused across calls
     */
    void processInsertion(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk,
            LongChunk<RowKeys> rightKeys, Chunk<Values> nextRightValue,
            WritableRowRedirection rowRedirection,
            RowSetBuilderRandom modifiedBuilder, SizedLongChunk<RowKeys> modifiedKeys, boolean endsWithLastValue,
            boolean disallowExactMatch);

    /**
     * Adds the left rows that the modified right rows match to {@code modifiedBuilder}.
     *
     * @param modifiedKeys scratch space for the modified left row keys, owned by the caller and reused across calls
     */
    void findModified(SegmentedSortedArray leftSsa, RowRedirection rowRedirection,
            Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampIndices,
            RowSetBuilderRandom modifiedBuilder, SizedLongChunk<RowKeys> modifiedKeys, boolean disallowExactMatch);

    void applyShift(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk,
            LongChunk<RowKeys> rightStampKeys, long shiftDelta, WritableRowRedirection rowRedirection,
            boolean disallowExactMatch);
}
