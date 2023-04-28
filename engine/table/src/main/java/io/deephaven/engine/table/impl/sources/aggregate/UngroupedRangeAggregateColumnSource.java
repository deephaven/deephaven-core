/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.base.ClampUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
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

import java.util.Objects;

/**
 * {@link UngroupedColumnSource} implementation for {@link RangeAggregateColumnSource}s.
 */
final class UngroupedRangeAggregateColumnSource<DATA_TYPE>
        extends BaseUngroupedAggregateColumnSource<DATA_TYPE, RangeAggregateColumnSource<?, DATA_TYPE>> {

    UngroupedRangeAggregateColumnSource(
            @NotNull final RangeAggregateColumnSource<?, DATA_TYPE> aggregateColumnSource) {
        super(aggregateColumnSource, aggregateColumnSource.aggregated.getType());
    }

    private static final class UngroupedFillContext extends BaseUngroupedAggregateColumnSource.UngroupedFillContext {

        private final Shareable shareable;

        private UngroupedFillContext(
                @NotNull final RangeAggregateColumnSource<?, ?> aggregateColumnSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            super(aggregateColumnSource.aggregated, chunkCapacity);
            shareable = sharedContext == null
                    ? new Shareable(false, aggregateColumnSource, chunkCapacity)
                    : sharedContext.getOrCreate(new SharingKey(aggregateColumnSource),
                            () -> new Shareable(true, aggregateColumnSource, chunkCapacity));
        }

        @Override
        BaseUngroupedAggregateColumnSource.UngroupedFillContext.Shareable getShareable() {
            return shareable;
        }

        /**
         * We need a complex key for the sliced aggregate source.
         */
        private static final class SharingKey implements SharedContext.Key<Shareable> {

            private final ColumnSource<? extends RowSet> rowSets;
            private final ColumnSource<Integer> startPositionsInclusive;
            private final ColumnSource<Integer> endPositionsExclusive;

            private final int hashCode;

            private SharingKey(@NotNull final RangeAggregateColumnSource<?, ?> aggregateColumnSource) {
                rowSets = aggregateColumnSource.rowSets;
                startPositionsInclusive = aggregateColumnSource.startPositionsInclusive;
                endPositionsExclusive = aggregateColumnSource.endPositionsExclusive;
                hashCode = Objects.hash(rowSets, startPositionsInclusive, endPositionsExclusive);
            }

            @Override
            public boolean equals(final Object other) {
                if (this == other) {
                    return true;
                }
                if (other == null || getClass() != other.getClass()) {
                    return false;
                }
                final SharingKey otherSharingKey = (SharingKey) other;
                return rowSets == otherSharingKey.rowSets
                        && startPositionsInclusive == otherSharingKey.startPositionsInclusive
                        && endPositionsExclusive == otherSharingKey.endPositionsExclusive;
            }

            @Override
            public int hashCode() {
                return hashCode;
            }
        }

        private static final class Shareable extends BaseUngroupedAggregateColumnSource.UngroupedFillContext.Shareable {

            private final GetContext startPositionsInclusiveGetContext;
            private final GetContext endPositionsExclusiveGetContext;

            private Shareable(
                    final boolean shared,
                    @NotNull final RangeAggregateColumnSource<?, ?> aggregateColumnSource,
                    final int chunkCapacity) {
                super(shared, aggregateColumnSource.rowSets, chunkCapacity);
                startPositionsInclusiveGetContext =
                        aggregateColumnSource.startPositionsInclusive.makeGetContext(chunkCapacity, this);
                endPositionsExclusiveGetContext =
                        aggregateColumnSource.endPositionsExclusive.makeGetContext(chunkCapacity, this);
            }

            private void extractFillChunkInformation(
                    @NotNull final ColumnSource<? extends RowSet> groupRowSets,
                    @NotNull final ColumnSource<Integer> startPositionsInclusive,
                    @NotNull final ColumnSource<Integer> endPositionsExclusive,
                    final long base,
                    final boolean usePrev,
                    @NotNull final RowSequence rowSequence) {
                if (stateReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                currentGroup = -1;
                componentKeys.setSize(0);
                rowSequence.forAllRowKeys((final long rowKey) -> {
                    // Store the group rowset index in rowKeys.
                    final long groupKey = getGroupIndexKey(rowKey, base);
                    if (currentGroup == -1 || groupKey != groupKeys.get(currentGroup)) {
                        ++currentGroup;
                        groupKeys.set(currentGroup, groupKey);
                        sameGroupRunLengths.set(currentGroup, 1);
                    } else {
                        sameGroupRunLengths.set(currentGroup,
                                sameGroupRunLengths.get(currentGroup) + 1);
                    }
                    // Store the offset to the current key in componentKeyIndices.
                    final long componentKey = getOffsetInGroup(rowKey, base);
                    componentKeys.add(componentKey);
                });
                groupKeys.setSize(currentGroup + 1);
                sameGroupRunLengths.setSize(currentGroup + 1);

                // Preload a chunk of group RowSets and start positions
                final ObjectChunk<RowSet, ? extends Values> rowSetsChunk;
                final IntChunk<? extends Values> startPositionsInclusiveChunk;
                try (final RowSequence groupRowSequence =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(groupKeys)) {
                    if (usePrev) {
                        rowSetsChunk = groupRowSets.getPrevChunk(groupGetContext, groupRowSequence).asObjectChunk();
                        startPositionsInclusiveChunk = startPositionsInclusive.getPrevChunk(startPositionsInclusiveGetContext, groupRowSequence).asIntChunk();
                    } else {
                        rowSetsChunk = groupRowSets.getChunk(groupGetContext, groupRowSequence).asObjectChunk();
                        startPositionsInclusiveChunk = startPositionsInclusive.getChunk(startPositionsInclusiveGetContext, groupRowSequence).asIntChunk();
                    }
                }

                // TODO-RWC: Resume cleanup from here.
                currentGroup = 0;
                for (int ii = 0; ii < rowSetsChunk.size(); ++ii) {
                    // Get the bucket rowset for the current group.
                    final RowSet currRowSet = rowSetsChunk.get(ii);
                    Assert.neqNull(currRowSet, "currRowSet");

                    // Get the previous rowset for the current group if needed.
                    final boolean usePrevIndex = usePrev && currRowSet.isTracking();
                    final RowSet bucketRowSet = usePrevIndex ? currRowSet.trackingCast().prev() : currRowSet;
                    final long bucketSize = bucketRowSet.size();

                    // Read the total length of items in this group.
                    final int lengthFromThisGroup = sameGroupRunLengths.get(ii);
                    // Determine when to stop iterating for the items in this group.
                    final long endIndex = currentGroup + lengthFromThisGroup;

                    // Get the row key and determine the starting position for the first entry of this group.
                    final long rowKey = groupKeys.get(ii);
                    final long rowPos = bucketRowSet.find(rowKey);
                    final long localStartOffset = startPositionsInclusiveChunk != null ? startPositionsInclusiveChunk.get(ii) : startOffset;
                    final long startPos = ClampUtil.clampLong(0, bucketSize, rowPos + localStartOffset);

                    while (currentGroup < endIndex) {
                        // Read the offset for this output row and determine the key in the underlying source.
                        final long offsetInGroup = componentKeys.get(currentGroup);
                        final long pos = startPos + offsetInGroup;
                        final long key = bucketRowSet.get(pos);

                        // Re-use 'componentKeyIndices' as the destination for the keys.
                        componentKeys.set(currentGroup, key);

                        currentGroup++;
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
                aggregateColumnSource.rowSets,
                aggregateColumnSource.startPositionsInclusive,
                aggregateColumnSource.endPositionsExclusive,
                base,
                false,
                rowSequence);
        tc.doFillChunk(aggregateColumnSource.aggregated, false, destination);
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
                aggregateColumnSource.rowSets,
                aggregateColumnSource.startPositionsInclusive,
                aggregateColumnSource.endPositionsExclusive,
                getPrevBase(),
                true,
                rowSequence);
        tc.doFillChunk(aggregateColumnSource.aggregated, true, destination);
    }
}
