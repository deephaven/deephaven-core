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
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.util.SafeCloseable;

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
            RestampContext restampContext, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch);

    void processInsertion(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, Chunk<Values> nextRightValue,
            WritableRowRedirection rowRedirection, RestampContext restampContext, RowSetBuilderRandom modifiedBuilder,
            boolean endsWithLastValue, boolean disallowExactMatch);

    int findModified(int first, Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys,
            RowRedirection rowRedirection, Chunk<? extends Values> rightStampChunk,
            LongChunk<RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch);

    void applyShift(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys,
            Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta,
            WritableRowRedirection rowRedirection, boolean disallowExactMatch);

    /**
     * The reusable state with which {@link #processRemovals} and {@link #processInsertion} redirect each run of left
     * rows to one right row through a single {@link WritableRowRedirection}. Its chunks grow with the longest run
     * written, up to {@link ChunkPoolConstants#LARGEST_POOLED_CHUNK_CAPACITY}, so one context serves every call of an
     * update cycle.
     */
    final class RestampContext implements SafeCloseable {
        private final WritableRowRedirection rowRedirection;
        private final ResettableLongChunk<RowKeys> outerRowKeys = ResettableLongChunk.makeResettableChunk();
        private WritableLongChunk<RowKeys> innerRowKeys;
        private ChunkSink.FillFromContext fillFromContext;

        /**
         * @param rowRedirection the row redirection that every call given this context writes
         */
        public RestampContext(final WritableRowRedirection rowRedirection) {
            this.rowRedirection = rowRedirection;
        }

        /**
         * Redirect the left keys at positions {@code [start, end)} to {@code innerRowKey}, a
         * {@link io.deephaven.engine.rowset.RowSequence#NULL_ROW_KEY} removing their mappings, and add them to
         * {@code modifiedBuilder}.
         */
        void restamp(final WritableRowRedirection rowRedirection, final LongChunk<RowKeys> leftStampKeys,
                final int start, final int end, final long innerRowKey, final RowSetBuilderRandom modifiedBuilder) {
            assert rowRedirection == this.rowRedirection;
            final int runLength = end - start;
            if (runLength == 0) {
                return;
            }
            final int capacity = innerRowKeys == null ? 0 : innerRowKeys.capacity();
            if (runLength > capacity && capacity < ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY) {
                // doubling bounds the reallocations of a cycle by the log of its longest run
                final int newCapacity = Math.min(Math.max(runLength, 2 * capacity),
                        ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY);
                SafeCloseable.closeAll(innerRowKeys, fillFromContext);
                innerRowKeys = null;
                fillFromContext = null;
                innerRowKeys = WritableLongChunk.makeWritableChunk(newCapacity);
                fillFromContext = rowRedirection.makeFillFromContext(newCapacity);
            }
            // the inner row keys of a run are one value, so one chunk of them serves every slice of the run
            final int sliceCapacity = Math.min(runLength, innerRowKeys.capacity());
            innerRowKeys.fillWithValue(0, sliceCapacity, innerRowKey);
            for (int sliceStart = start; sliceStart < end; sliceStart += sliceCapacity) {
                final int sliceLength = Math.min(sliceCapacity, end - sliceStart);
                innerRowKeys.setSize(sliceLength);
                outerRowKeys.resetFromTypedChunk(leftStampKeys, sliceStart, sliceLength);
                rowRedirection.fillFromChunkUnordered(fillFromContext, innerRowKeys, outerRowKeys);
            }
            ChunkSsaStampRuns.addModified(leftStampKeys, start, end, modifiedBuilder);
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(outerRowKeys, innerRowKeys, fillFromContext);
            innerRowKeys = null;
            fillFromContext = null;
        }
    }
}
