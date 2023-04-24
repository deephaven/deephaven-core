/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        // noinspection unchecked
        return (DATA_TYPE) aggregateColumnSource.getUngrouped(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public Boolean getBoolean(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedBoolean(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public byte getByte(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedByte(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public char getChar(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedChar(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public double getDouble(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedDouble(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public float getFloat(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedFloat(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public int getInt(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedInt(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public long getLong(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedLong(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public short getShort(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long groupIndexKey = getGroupIndexKey(rowKey, base);
        final long offsetInGroup = getOffsetInGroup(rowKey, base);
        return aggregateColumnSource.getUngroupedShort(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public DATA_TYPE getPrev(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        // noinspection unchecked
        return (DATA_TYPE) aggregateColumnSource.getUngroupedPrev(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public Boolean getPrevBoolean(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevBoolean(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public byte getPrevByte(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevByte(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public char getPrevChar(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevChar(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public double getPrevDouble(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevDouble(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public float getPrevFloat(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevFloat(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public int getPrevInt(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevInt(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public long getPrevLong(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevLong(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public short getPrevShort(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(rowKey, prevBase);
        final long offsetInGroup = getOffsetInGroup(rowKey, prevBase);
        return aggregateColumnSource.getUngroupedPrevShort(groupIndexKey, (int) offsetInGroup);
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
            final GetContext rowsetGetContext;
            final WritableLongChunk<OrderedRowKeys> rowKeys;
            final WritableIntChunk<ChunkLengths> sameIndexRunLengths;
            final WritableLongChunk<OrderedRowKeys> componentKeys;
            final ResettableWritableLongChunk<OrderedRowKeys> componentKeySlice;

            boolean stateReusable;
            int currentIndex;

            Shareable(final boolean shared,
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    final int chunkCapacity) {
                this.shared = shared;

                rowsetGetContext = groupRowSetSource.makeGetContext(chunkCapacity, this);
                rowKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);
                sameIndexRunLengths = WritableIntChunk.makeWritableChunk(chunkCapacity);
                componentKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);
                componentKeySlice = ResettableWritableLongChunk.makeResettableChunk();
            }

            @Override
            public void reset() {
                stateReusable = false;
                super.reset();
            }

            @Override
            public void close() {
                SafeCloseable.closeAll(
                        rowsetGetContext,
                        rowKeys,
                        sameIndexRunLengths,
                        componentKeys,
                        componentKeySlice);
                super.close();
            }
        }

        void doFillChunk(@NotNull final ColumnSource<?> valueSource, final boolean usePrev,
                @NotNull final WritableChunk<? super Values> destination) {
            final Shareable shareable = getShareable();

            int componentKeyIndicesPosition = 0;
            for (int ii = 0; ii < shareable.sameIndexRunLengths.size(); ++ii) {
                final int lengthFromThisIndex = shareable.sameIndexRunLengths.get(ii);

                final WritableLongChunk<OrderedRowKeys> remappedComponentKeys =
                        shareable.componentKeySlice.resetFromTypedChunk(shareable.componentKeys,
                                componentKeyIndicesPosition, lengthFromThisIndex);

                try (final RowSequence componentRowSequence =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(remappedComponentKeys)) {
                    if (usePrev) {
                        valueSource.fillPrevChunk(aggregatedFillContext, destinationSlice.resetFromChunk(destination,
                                componentKeyIndicesPosition, lengthFromThisIndex), componentRowSequence);
                    } else {
                        valueSource.fillChunk(aggregatedFillContext, destinationSlice.resetFromChunk(destination,
                                componentKeyIndicesPosition, lengthFromThisIndex), componentRowSequence);
                    }
                }

                componentKeyIndicesPosition += lengthFromThisIndex;
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

    static long getGroupIndexKey(final long rowKey, final long base) {
        return rowKey >> base;
    }

    static long getOffsetInGroup(final long rowKey, final long base) {
        return rowKey & ((1L << base) - 1);
    }
}
