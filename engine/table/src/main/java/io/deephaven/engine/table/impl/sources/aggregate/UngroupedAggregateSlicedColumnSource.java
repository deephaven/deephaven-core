/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link UngroupedColumnSource} implementation for {@link BaseAggregateSlicedColumnSource}s.
 */
final class UngroupedAggregateSlicedColumnSource<DATA_TYPE>
        extends BaseUngroupedAggregateColumnSource<DATA_TYPE, BaseAggregateSlicedColumnSource<?, DATA_TYPE>> {
    UngroupedAggregateSlicedColumnSource(
            @NotNull final BaseAggregateSlicedColumnSource<?, DATA_TYPE> aggregateColumnSource) {
        super(aggregateColumnSource, aggregateColumnSource.aggregatedSource.getType());
    }

    private static final class UngroupedFillContext extends BaseUngroupedAggregateColumnSource.UngroupedFillContext {
        private final Shareable shareable;

        private UngroupedFillContext(@NotNull final BaseAggregateSlicedColumnSource<?, ?> aggregateColumnSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            super(aggregateColumnSource.aggregatedSource, chunkCapacity);

            // Create the correct type of Shareable when start/end offsets are constant (vs dynamic for each row).
            if (aggregateColumnSource.startSource == null) {
                shareable = sharedContext == null
                        ? new Shareable(false, aggregateColumnSource.groupRowSetSource,
                                aggregateColumnSource.startOffset, aggregateColumnSource.endOffset, chunkCapacity)
                        : sharedContext.getOrCreate(new SharingKey(
                                aggregateColumnSource.groupRowSetSource,
                                aggregateColumnSource.startSource,
                                aggregateColumnSource.endSource,
                                aggregateColumnSource.startOffset,
                                aggregateColumnSource.endOffset),
                                () -> new Shareable(true, aggregateColumnSource.groupRowSetSource,
                                        aggregateColumnSource.startOffset, aggregateColumnSource.endOffset,
                                        chunkCapacity));

            } else {
                shareable = sharedContext == null
                        ? new Shareable(false, aggregateColumnSource.groupRowSetSource,
                                aggregateColumnSource.startSource, aggregateColumnSource.endSource, chunkCapacity)
                        : sharedContext.getOrCreate(new SharingKey(
                                aggregateColumnSource.groupRowSetSource,
                                aggregateColumnSource.startSource,
                                aggregateColumnSource.endSource,
                                aggregateColumnSource.startOffset,
                                aggregateColumnSource.endOffset),
                                () -> new Shareable(true, aggregateColumnSource.groupRowSetSource,
                                        aggregateColumnSource.startSource, aggregateColumnSource.endSource,
                                        chunkCapacity));
            }
        }

        @Override
        BaseUngroupedAggregateColumnSource.UngroupedFillContext.Shareable getShareable() {
            return shareable;
        }

        /**
         * We need a complex key for the sliced aggregate source.
         */
        private static final class SharingKey implements SharedContext.Key<Shareable> {
            @NotNull
            private final ColumnSource<? extends RowSet> groupRowSetSource;
            @Nullable
            private final ColumnSource<Long> startSource;
            @Nullable
            private final ColumnSource<Long> endSource;
            private final long startOffset;
            private final long endOffset;

            private SharingKey(
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    @Nullable final ColumnSource<Long> startSource,
                    @Nullable final ColumnSource<Long> endSource,
                    final long startOffset,
                    final long endOffset) {
                this.groupRowSetSource = groupRowSetSource;
                this.startSource = startSource;
                this.endSource = endSource;
                this.startOffset = startOffset;
                this.endOffset = endOffset;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;
                SharingKey that = (SharingKey) o;
                return groupRowSetSource == that.groupRowSetSource
                        && startSource == that.startSource
                        && endSource == that.endSource
                        && startOffset == that.startOffset
                        && endOffset == that.endOffset;
            }

            @Override
            public int hashCode() {
                int hash = System.identityHashCode(groupRowSetSource);
                if (startSource != null) {
                    hash = hash * 31 + System.identityHashCode(startSource);
                    hash = hash * 31 + System.identityHashCode(endSource);
                } else {
                    hash = hash * 31 + (int) startOffset;
                    hash = hash * 31 + (int) endOffset;
                }
                return hash;
            }
        }

        private static final class Shareable extends BaseUngroupedAggregateColumnSource.UngroupedFillContext.Shareable {
            private final GetContext startGetContext;
            private final GetContext endGetContext;
            private final long startOffset;
            private final long endOffset;

            private Shareable(final boolean shared,
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    @Nullable final ColumnSource<Long> startSource,
                    @Nullable final ColumnSource<Long> endSource,
                    final int chunkCapacity) {
                super(shared, groupRowSetSource, chunkCapacity);

                startGetContext = startSource.makeGetContext(chunkCapacity, this);
                endGetContext = endSource.makeGetContext(chunkCapacity, this);
                startOffset = 0;
                endOffset = 0;
            }

            private Shareable(final boolean shared,
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    final long startOffset,
                    final long endOffset,
                    final int chunkCapacity) {
                super(shared, groupRowSetSource, chunkCapacity);

                startGetContext = null;
                endGetContext = null;
                this.startOffset = startOffset;
                this.endOffset = endOffset;
            }

            private void extractFillChunkInformation(
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    @Nullable final ColumnSource<Long> startSource,
                    @Nullable final ColumnSource<Long> endSource,
                    final long base,
                    final boolean usePrev,
                    @NotNull final RowSequence rowSequence) {
                if (stateReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                currentIndex = -1;
                componentKeys.setSize(0);
                rowSequence.forAllRowKeys((final long rowKey) -> {
                    // Store the group rowset index in rowsetKeyIndices.
                    final long indexrowKey = getGroupIndexKey(rowKey, base);
                    if (currentIndex == -1 || indexrowKey != rowKeys.get(currentIndex)) {
                        ++currentIndex;
                        rowKeys.set(currentIndex, indexrowKey);
                        sameIndexRunLengths.set(currentIndex, 1);
                    } else {
                        sameIndexRunLengths.set(currentIndex,
                                sameIndexRunLengths.get(currentIndex) + 1);
                    }
                    // Store the offset to the current key in componentKeyIndices.
                    final long componentrowKey = getOffsetInGroup(rowKey, base);
                    componentKeys.add(componentrowKey);
                });
                rowKeys.setSize(currentIndex + 1);
                sameIndexRunLengths.setSize(currentIndex + 1);

                // Preload a chunk of rowsets (and start/end offsets if appropriate).
                final ObjectChunk<RowSet, ? extends Values> rowSets;
                final LongChunk<? extends Values> startOffsets;
                try (final RowSequence indexRowSequence =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(rowKeys)) {
                    if (usePrev) {
                        rowSets = groupRowSetSource.getPrevChunk(rowsetGetContext, indexRowSequence).asObjectChunk();
                        startOffsets = startSource != null
                                ? startSource.getPrevChunk(startGetContext, indexRowSequence).asLongChunk()
                                : null;
                    } else {
                        rowSets = groupRowSetSource.getChunk(rowsetGetContext, indexRowSequence).asObjectChunk();
                        startOffsets = startSource != null
                                ? startSource.getChunk(startGetContext, indexRowSequence).asLongChunk()
                                : null;
                    }
                }

                currentIndex = 0;
                for (int ii = 0; ii < rowSets.size(); ++ii) {
                    // Get the bucket rowset for the current group.
                    final RowSet currRowSet = rowSets.get(ii);
                    Assert.neqNull(currRowSet, "currRowSet");

                    // Get the previous rowset for the current group if needed.
                    final boolean usePrevIndex = usePrev && currRowSet.isTracking();
                    final RowSet bucketRowSet = usePrevIndex ? currRowSet.trackingCast().prev() : currRowSet;
                    final long bucketSize = bucketRowSet.size();

                    // Read the total length of items in this group.
                    final int lengthFromThisGroup = sameIndexRunLengths.get(ii);
                    // Determine when to stop iterating for the items in this group.
                    final long endIndex = currentIndex + lengthFromThisGroup;

                    // Get the row key and determine the starting position for the first entry of this group.
                    final long rowKey = rowKeys.get(ii);
                    final long rowPos = bucketRowSet.find(rowKey);
                    final long localStartOffset = startOffsets != null ? startOffsets.get(ii) : startOffset;
                    final long startPos = ClampUtil.clampLong(0, bucketSize, rowPos + localStartOffset);

                    while (currentIndex < endIndex) {
                        // Read the offset for this output row and determine the key in the underlying source.
                        final long offsetInGroup = componentKeys.get(currentIndex);
                        final long pos = startPos + offsetInGroup;
                        final long key = bucketRowSet.get(pos);

                        // Re-use 'componentKeyIndices' as the destination for the keys.
                        componentKeys.set(currentIndex, key);

                        currentIndex++;
                    }
                }

                stateReusable = shared;
            }

            @Override
            public void close() {
                SafeCloseable.closeAll(
                        startGetContext,
                        endGetContext);
                super.close();
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
}
