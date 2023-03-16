/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.LongChunkAppender;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.stream.LongStream;

import static io.deephaven.util.QueryConstants.*;

/**
 * {@link UngroupedColumnSource} implementation for {@link AggregateColumnSource}s.
 */
final class UngroupedAggregateSlicedColumnSource<DATA_TYPE> extends UngroupedColumnSource<DATA_TYPE> {

    private final BaseAggregateSlicedColumnSource<?, DATA_TYPE> aggregateColumnSource;

    UngroupedAggregateSlicedColumnSource(
            @NotNull final BaseAggregateSlicedColumnSource<?, DATA_TYPE> aggregateColumnSource) {
        super(aggregateColumnSource.aggregatedSource.getType());
        this.aggregateColumnSource = aggregateColumnSource;
    }

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

    private static final class UngroupedFillContext implements FillContext {

        private final Shareable shareable;

        private final FillContext aggregatedFillContext;
        private final ResettableWritableChunk<Any> destinationSlice;

        private UngroupedFillContext(@NotNull final BaseAggregateSlicedColumnSource<?, ?> aggregateColumnSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            final ColumnSource<? extends RowSet> groupRowSetSource = aggregateColumnSource.groupRowSetSource;
            final ColumnSource<?> aggregatedSource = aggregateColumnSource.aggregatedSource;

            // Create the correct type of Shareable when start/end offsets are constant (vs dynamic for each row).
            if (aggregateColumnSource.startSource == null) {
                shareable = sharedContext == null
                        ? new Shareable(false, aggregateColumnSource.groupRowSetSource,
                                aggregateColumnSource.startOffset, aggregateColumnSource.endOffset, chunkCapacity)
                        : sharedContext.getOrCreate(new SharingKey(groupRowSetSource),
                                () -> new Shareable(true, aggregateColumnSource.groupRowSetSource,
                                        aggregateColumnSource.startOffset, aggregateColumnSource.endOffset,
                                        chunkCapacity));

            } else {
                shareable = sharedContext == null
                        ? new Shareable(false, aggregateColumnSource.groupRowSetSource,
                                aggregateColumnSource.startSource, aggregateColumnSource.endSource, chunkCapacity)
                        : sharedContext.getOrCreate(new SharingKey(groupRowSetSource),
                                () -> new Shareable(true, aggregateColumnSource.groupRowSetSource,
                                        aggregateColumnSource.startSource, aggregateColumnSource.endSource,
                                        chunkCapacity));
            }

            // NB: There's no reason to use a shared context for the values source. We'd have to reset it between each
            // sub-fill.
            aggregatedFillContext = aggregatedSource.makeFillContext(chunkCapacity);
            destinationSlice = aggregatedSource.getChunkType().makeResettableWritableChunk();
        }

        private static final class SharingKey extends SharedContext.ExactReferenceSharingKey<Shareable> {

            private SharingKey(@NotNull final ColumnSource groupRowSetSource) {
                super(groupRowSetSource);
            }
        }


        private static final class Shareable extends SharedContext {
            private final boolean shared;

            private final GetContext rowsetGetContext;
            private final WritableLongChunk<OrderedRowKeys> rowsetKeyIndices;
            private final GetContext startGetContext;
            private final GetContext endGetContext;
            final long startOffset;
            final long endOffset;
            private final WritableIntChunk<ChunkLengths> sameIndexRunLengths;
            private final WritableLongChunk<OrderedRowKeys> componentKeyIndices;
            private final ResettableWritableLongChunk<OrderedRowKeys> componentKeyIndicesSlice;

            private boolean stateReusable;
            private int currentIndexPosition;

            private Shareable(final boolean shared,
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    @NotNull final ColumnSource<Long> startSource,
                    @NotNull final ColumnSource<Long> endSource,
                    final int chunkCapacity) {
                this.shared = shared;

                rowsetGetContext = groupRowSetSource.makeGetContext(chunkCapacity, this);
                rowsetKeyIndices = WritableLongChunk.makeWritableChunk(chunkCapacity);
                sameIndexRunLengths = WritableIntChunk.makeWritableChunk(chunkCapacity);

                startGetContext = startSource.makeGetContext(chunkCapacity, this);
                endGetContext = endSource.makeGetContext(chunkCapacity, this);
                startOffset = 0;
                endOffset = 0;

                componentKeyIndices = WritableLongChunk.makeWritableChunk(chunkCapacity);
                componentKeyIndicesSlice = ResettableWritableLongChunk.makeResettableChunk();
            }

            private Shareable(final boolean shared,
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    final long startOffset,
                    final long endOffset,
                    final int chunkCapacity) {
                this.shared = shared;

                rowsetGetContext = groupRowSetSource.makeGetContext(chunkCapacity, this);
                rowsetKeyIndices = WritableLongChunk.makeWritableChunk(chunkCapacity);
                sameIndexRunLengths = WritableIntChunk.makeWritableChunk(chunkCapacity);

                startGetContext = null;
                endGetContext = null;
                this.startOffset = startOffset;
                this.endOffset = endOffset;

                componentKeyIndices = WritableLongChunk.makeWritableChunk(chunkCapacity);
                componentKeyIndicesSlice = ResettableWritableLongChunk.makeResettableChunk();
            }

            private void extractFillChunkInformation(@NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    @Nullable final ColumnSource<Long> startSource,
                    @Nullable final ColumnSource<Long> endSource,
                    final long base, final boolean usePrev, @NotNull final RowSequence rowSequence) {
                if (stateReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                currentIndexPosition = -1;
                componentKeyIndices.setSize(0);
                rowSequence.forAllRowKeys((final long rowKey) -> {
                    // Store the group rowset index in rowsetKeyIndices.
                    final long indexrowKey = getGroupIndexKey(rowKey, base);
                    if (currentIndexPosition == -1 || indexrowKey != rowsetKeyIndices.get(currentIndexPosition)) {
                        ++currentIndexPosition;
                        rowsetKeyIndices.set(currentIndexPosition, indexrowKey);
                        sameIndexRunLengths.set(currentIndexPosition, 1);
                    } else {
                        sameIndexRunLengths.set(currentIndexPosition,
                                sameIndexRunLengths.get(currentIndexPosition) + 1);
                    }
                    // Store the offset to the current key in componentKeyIndices.
                    final long componentrowKey = getOffsetInGroup(rowKey, base);
                    componentKeyIndices.add(componentrowKey);
                });
                rowsetKeyIndices.setSize(currentIndexPosition + 1);
                sameIndexRunLengths.setSize(currentIndexPosition + 1);

                // Preload a chunk of rowsets (and start/end offsets if appropriate).
                final ObjectChunk<RowSet, ? extends Values> indexes;
                final LongChunk<? extends Values> startOffsets;
                try (final RowSequence indexRowSequence =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(rowsetKeyIndices)) {
                    if (usePrev) {
                        indexes = groupRowSetSource.getPrevChunk(rowsetGetContext, indexRowSequence).asObjectChunk();
                        startOffsets = startSource != null
                                ? startSource.getPrevChunk(startGetContext, indexRowSequence).asLongChunk()
                                : null;
                    } else {
                        indexes = groupRowSetSource.getChunk(rowsetGetContext, indexRowSequence).asObjectChunk();
                        startOffsets = startSource != null
                                ? startSource.getChunk(startGetContext, indexRowSequence).asLongChunk()
                                : null;
                    }
                }

                int componentKeyIndicesPosition = 0;
                for (int ii = 0; ii < indexes.size(); ++ii) {
                    // Get the bucket rowset for this key
                    final RowSet currRowSet = indexes.get(ii);
                    Assert.neqNull(currRowSet, "currRowSet");
                    final boolean usePrevIndex = usePrev && currRowSet.isTracking();
                    final RowSet bucketRowSet = usePrevIndex ? currRowSet.trackingCast().copyPrev() : currRowSet;

                    final long indexrowKey = rowsetKeyIndices.get(ii);
                    final int rowPos = Math.toIntExact(currRowSet.find(indexrowKey));

                    final int startPos = Math.toIntExact(startOffsets != null ? startOffsets.get(ii) : startOffset);
                    final long size = bucketRowSet.size();
                    final long offsetInGroup = componentKeyIndices.get(componentKeyIndicesPosition);
                    final long start = ClampUtil.clampLong(0, size, rowPos + startPos) + offsetInGroup;

                    try {
                        final int lengthFromThisIndex = sameIndexRunLengths.get(ii);

                        final WritableLongChunk<OrderedRowKeys> remappedComponentKeys =
                                componentKeyIndicesSlice.resetFromTypedChunk(componentKeyIndices,
                                        componentKeyIndicesPosition, lengthFromThisIndex);
                        bucketRowSet.getKeysForPositions(
                                LongStream.range(start, start + lengthFromThisIndex).iterator(),
                                new LongChunkAppender(remappedComponentKeys));

                        componentKeyIndicesPosition += lengthFromThisIndex;
                    } finally {
                        if (usePrevIndex) {
                            bucketRowSet.close();
                        }
                    }
                }

                stateReusable = shared;
            }

            @Override
            public void reset() {
                stateReusable = false;
                super.reset();
            }

            @Override
            public void close() {
                SafeCloseable.closeArray(
                        rowsetGetContext,
                        rowsetKeyIndices,
                        sameIndexRunLengths,
                        componentKeyIndices,
                        componentKeyIndicesSlice,
                        startGetContext,
                        endGetContext);
                super.close();
            }
        }

        private void doFillChunk(@NotNull final ColumnSource<?> valueSource, final boolean usePrev,
                @NotNull final WritableChunk<? super Values> destination) {
            int componentKeyIndicesPosition = 0;
            for (int ii = 0; ii < shareable.sameIndexRunLengths.size(); ++ii) {
                final int lengthFromThisIndex = shareable.sameIndexRunLengths.get(ii);

                final WritableLongChunk<OrderedRowKeys> remappedComponentKeys =
                        shareable.componentKeyIndicesSlice.resetFromTypedChunk(shareable.componentKeyIndices,
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

            if (!shareable.shared) {
                shareable.close();
            }
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new UngroupedFillContext(aggregateColumnSource, chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        destination.setSize(rowSequence.intSize());
        if (destination.size() == 0) {
            return;
        }
        final UngroupedFillContext tc = (UngroupedFillContext) context;
        tc.shareable.extractFillChunkInformation(
                aggregateColumnSource.groupRowSetSource,
                aggregateColumnSource.startSource,
                aggregateColumnSource.endSource,
                base,
                false,
                rowSequence);
        tc.doFillChunk(aggregateColumnSource.aggregatedSource, false, destination);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        destination.setSize(rowSequence.intSize());
        if (destination.size() == 0) {
            return;
        }
        final UngroupedFillContext tc = (UngroupedFillContext) context;
        tc.shareable.extractFillChunkInformation(
                aggregateColumnSource.groupRowSetSource,
                aggregateColumnSource.startSource,
                aggregateColumnSource.endSource,
                getPrevBase(),
                true,
                rowSequence);
        tc.doFillChunk(aggregateColumnSource.aggregatedSource, true, destination);
    }

    @Override
    public boolean isImmutable() {
        return aggregateColumnSource.isImmutable();
    }

    private static long getGroupIndexKey(final long rowKey, final long base) {
        return rowKey >> base;
    }

    private static long getOffsetInGroup(final long rowKey, final long base) {
        return rowKey & ((1L << base) - 1);
    }
}
