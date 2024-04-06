//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.LongChunkAppender;
import io.deephaven.chunk.util.LongChunkIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
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

            private Shareable(
                    final boolean shared,
                    @NotNull final RangeAggregateColumnSource<?, ?> aggregateColumnSource,
                    final int chunkCapacity) {
                super(shared, aggregateColumnSource.rowSets, chunkCapacity);
                startPositionsInclusiveGetContext =
                        aggregateColumnSource.startPositionsInclusive.makeGetContext(chunkCapacity, this);
            }

            private void extractFillChunkInformation(
                    @NotNull final ColumnSource<? extends RowSet> groupRowSets,
                    @NotNull final ColumnSource<Integer> startPositionsInclusive,
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
                componentRowKeys.setSize(0);
                rowSequence.forAllRowKeys((final long rowKey) -> {
                    // Store the group rowset index in rowKeys.
                    final long groupKey = getGroupRowKey(rowKey, base);
                    if (currentIndex == -1 || groupKey != groupRowKeys.get(currentIndex)) {
                        ++currentIndex;
                        groupRowKeys.set(currentIndex, groupKey);
                        sameGroupRunLengths.set(currentIndex, 1);
                    } else {
                        sameGroupRunLengths.set(currentIndex, sameGroupRunLengths.get(currentIndex) + 1);
                    }
                    // Initially we fill componentRowKeys with positions, which will be inverted before use
                    final long componentRowPosition = getOffsetInGroup(rowKey, base);
                    componentRowKeys.add(componentRowPosition);
                });
                groupRowKeys.setSize(currentIndex + 1);
                sameGroupRunLengths.setSize(currentIndex + 1);

                // Preload a chunk of group RowSets and start positions
                final ObjectChunk<RowSet, ? extends Values> rowSetsChunk;
                final IntChunk<? extends Values> startPositionsInclusiveChunk;
                try (final RowSequence groupRowSequence =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(groupRowKeys)) {
                    if (usePrev) {
                        rowSetsChunk = groupRowSets.getPrevChunk(groupGetContext, groupRowSequence).asObjectChunk();
                        startPositionsInclusiveChunk = startPositionsInclusive.getPrevChunk(
                                startPositionsInclusiveGetContext, groupRowSequence).asIntChunk();
                    } else {
                        rowSetsChunk = groupRowSets.getChunk(groupGetContext, groupRowSequence).asObjectChunk();
                        startPositionsInclusiveChunk = startPositionsInclusive.getChunk(
                                startPositionsInclusiveGetContext, groupRowSequence).asIntChunk();
                    }
                }

                currentIndex = 0;
                for (int gi = 0; gi < rowSetsChunk.size(); ++gi) {
                    // Get the RowSet for the current group
                    final RowSet currRowSet = rowSetsChunk.get(gi);

                    // Get the previous rowset for the current group if needed
                    final boolean usePrevRowSet = usePrev && currRowSet.isTracking();
                    final RowSet groupRowSet = usePrevRowSet ? currRowSet.trackingCast().prev() : currRowSet;

                    // Read the total length of items in this group
                    final int lengthFromThisGroup = sameGroupRunLengths.get(gi);

                    // Get the starting position for the first entry of this group
                    final int startPositionInclusive = startPositionsInclusiveChunk.get(gi);

                    final WritableLongChunk<OrderedRowKeys> remappedComponentKeys = componentRowKeySlice
                            .resetFromTypedChunk(componentRowKeys, currentIndex, lengthFromThisGroup);

                    // Offset the component row positions by start position
                    for (int ci = 0; ci < lengthFromThisGroup; ++ci) {
                        remappedComponentKeys.set(ci, remappedComponentKeys.get(ci) + startPositionInclusive);
                    }
                    // Invert the component row positions to component row keys, in-place
                    groupRowSet.getKeysForPositions(
                            new LongChunkIterator(remappedComponentKeys),
                            new LongChunkAppender(remappedComponentKeys));

                    currentIndex += lengthFromThisGroup;
                }

                stateReusable = shared;
            }

            @Override
            public void close() {
                SafeCloseable.closeAll(
                        startPositionsInclusiveGetContext,
                        super::close);
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
                getPrevBase(),
                true,
                rowSequence);
        tc.doFillChunk(aggregateColumnSource.aggregated, true, destination);
    }
}
