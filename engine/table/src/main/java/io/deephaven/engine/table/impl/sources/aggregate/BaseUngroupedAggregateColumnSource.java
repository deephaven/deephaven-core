//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

/**
 * A base {@link UngroupedColumnSource} implementation for {@link AggregateColumnSource}s.
 */
abstract class BaseUngroupedAggregateColumnSource<DATA_TYPE, SOURCE_TYPE extends AggregateColumnSource<?, DATA_TYPE>>
        extends UngroupedColumnSource<DATA_TYPE> {

    final SOURCE_TYPE aggregateColumnSource;

    BaseUngroupedAggregateColumnSource(@NotNull final SOURCE_TYPE aggregateColumnSource,
            final Class<DATA_TYPE> colType) {
        super(colType);
        this.aggregateColumnSource = aggregateColumnSource;
    }

    // region Shared accessors

    @Override
    public DATA_TYPE get(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        // noinspection unchecked
        return (DATA_TYPE) aggregateColumnSource.getUngrouped(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public Boolean getBoolean(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedBoolean(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public byte getByte(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedByte(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public char getChar(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedChar(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public double getDouble(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedDouble(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public float getFloat(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedFloat(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public int getInt(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedInt(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public long getLong(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedLong(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public short getShort(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long groupRowKey = getGroupRowKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedShort(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public DATA_TYPE getPrev(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        // noinspection unchecked
        return (DATA_TYPE) aggregateColumnSource.getUngroupedPrev(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public Boolean getPrevBoolean(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevBoolean(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public byte getPrevByte(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevByte(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public char getPrevChar(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevChar(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public double getPrevDouble(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevDouble(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public float getPrevFloat(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevFloat(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public int getPrevInt(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevInt(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public long getPrevLong(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevLong(groupRowKey, (int) offsetInGroup);
    }

    @Override
    public short getPrevShort(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long prevBase = getPrevBase();
        final long groupRowKey = getGroupRowKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevShort(groupRowKey, (int) offsetInGroup);
    }
    // endregion

    static abstract class UngroupedFillContext implements FillContext {
        final FillContext aggregatedFillContext;
        final ResettableWritableChunk<Any> destinationSlice;

        UngroupedFillContext(final ColumnSource<?> aggregatedSource,
                final int chunkCapacity) {

            // NB: There's no reason to use a shared context for the values source. We'd have to reset it between each
            // sub-fill.
            aggregatedFillContext = aggregatedSource.makeFillContext(chunkCapacity);
            destinationSlice = aggregatedSource.getChunkType().makeResettableWritableChunk();
        }

        abstract Shareable getShareable();

        static abstract class Shareable extends SharedContext {

            final boolean shared;
            final GetContext groupGetContext;
            final WritableLongChunk<OrderedRowKeys> groupRowKeys;
            final WritableIntChunk<ChunkLengths> sameGroupRunLengths;
            final WritableLongChunk<OrderedRowKeys> componentRowKeys;
            final ResettableWritableLongChunk<OrderedRowKeys> componentRowKeySlice;

            boolean stateReusable;
            int currentIndex;

            Shareable(
                    final boolean shared,
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    final int chunkCapacity) {
                this.shared = shared;

                groupGetContext = groupRowSetSource.makeGetContext(chunkCapacity, this);
                groupRowKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);
                sameGroupRunLengths = WritableIntChunk.makeWritableChunk(chunkCapacity);
                componentRowKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);
                componentRowKeySlice = ResettableWritableLongChunk.makeResettableChunk();
            }

            @Override
            public void reset() {
                stateReusable = false;
                super.reset();
            }

            @Override
            public void close() {
                SafeCloseable.closeAll(
                        groupGetContext,
                        groupRowKeys,
                        sameGroupRunLengths,
                        componentRowKeys,
                        componentRowKeySlice,
                        super::close);
            }
        }

        void doFillChunk(
                @NotNull final ColumnSource<?> valueSource,
                final boolean usePrev,
                @NotNull final WritableChunk<? super Values> destination) {
            final Shareable shareable = getShareable();

            int componentRowKeysOffset = 0;
            for (int ii = 0; ii < shareable.sameGroupRunLengths.size(); ++ii) {
                final int lengthFromThisGroup = shareable.sameGroupRunLengths.get(ii);

                final WritableLongChunk<OrderedRowKeys> remappedComponentRowKeys =
                        shareable.componentRowKeySlice.resetFromTypedChunk(shareable.componentRowKeys,
                                componentRowKeysOffset, lengthFromThisGroup);

                try (final RowSequence componentRowSequence =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(remappedComponentRowKeys)) {
                    if (usePrev) {
                        valueSource.fillPrevChunk(
                                aggregatedFillContext,
                                destinationSlice.resetFromChunk(destination, componentRowKeysOffset,
                                        lengthFromThisGroup),
                                componentRowSequence);
                    } else {
                        valueSource.fillChunk(
                                aggregatedFillContext,
                                destinationSlice.resetFromChunk(destination, componentRowKeysOffset,
                                        lengthFromThisGroup),
                                componentRowSequence);
                    }
                }

                componentRowKeysOffset += lengthFromThisGroup;
            }
        }

        @Override
        public void close() {
            aggregatedFillContext.close();
            destinationSlice.close();

            final Shareable shareable = getShareable();
            if (!shareable.shared) {
                shareable.close();
            }
        }
    }

    public abstract FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext);

    public abstract void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence);

    public abstract void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence);

    @Override
    public boolean isImmutable() {
        return aggregateColumnSource.isImmutable();
    }

    static long getGroupRowKey(final long rowKey, final long base) {
        return rowKey >> base;
    }

    static long getOffsetInGroup(final long rowKey, final long base) {
        return rowKey & ((1L << base) - 1);
    }
}
