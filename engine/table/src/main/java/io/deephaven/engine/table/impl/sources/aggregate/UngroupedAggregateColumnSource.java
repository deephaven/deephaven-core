//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.chunk.util.LongChunkAppender;
import io.deephaven.chunk.util.LongChunkIterator;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * {@link UngroupedColumnSource} implementation for {@link BaseAggregateColumnSource}s.
 */
final class UngroupedAggregateColumnSource<DATA_TYPE>
        extends BaseUngroupedAggregateColumnSource<DATA_TYPE, BaseAggregateColumnSource<?, DATA_TYPE>> {

    UngroupedAggregateColumnSource(@NotNull final BaseAggregateColumnSource<?, DATA_TYPE> aggregateColumnSource) {
        super(aggregateColumnSource, aggregateColumnSource.aggregatedSource.getType());
    }

    private static final class UngroupedFillContext extends BaseUngroupedAggregateColumnSource.UngroupedFillContext {
        private final Shareable shareable;

        private UngroupedFillContext(@NotNull final BaseAggregateColumnSource<?, ?> aggregateColumnSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            super(aggregateColumnSource.aggregatedSource, chunkCapacity);

            shareable = sharedContext == null
                    ? new Shareable(false, aggregateColumnSource.groupRowSetSource, chunkCapacity)
                    : sharedContext.getOrCreate(new SharingKey(aggregateColumnSource.groupRowSetSource),
                            () -> new Shareable(true, aggregateColumnSource.groupRowSetSource, chunkCapacity));
        }

        @Override
        BaseUngroupedAggregateColumnSource.UngroupedFillContext.Shareable getShareable() {
            return shareable;
        }

        /**
         * We can use a simple sharing key for the non-sliced aggregate source.
         */
        private static final class SharingKey extends SharedContext.ExactReferenceSharingKey<Shareable> {

            private SharingKey(@NotNull final ColumnSource<?> groupRowSetSource) {
                super(groupRowSetSource);
            }
        }

        private static final class Shareable extends BaseUngroupedAggregateColumnSource.UngroupedFillContext.Shareable {

            private Shareable(final boolean shared,
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    final int chunkCapacity) {
                super(shared, groupRowSetSource, chunkCapacity);
            }

            private void extractFillChunkInformation(
                    @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
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
                    final long groupRowKey = getGroupRowKey(rowKey, base);
                    if (currentIndex == -1 || groupRowKey != groupRowKeys.get(currentIndex)) {
                        ++currentIndex;
                        groupRowKeys.set(currentIndex, groupRowKey);
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

                final ObjectChunk<RowSet, ? extends Values> groups;
                try (final RowSequence groupRowSequence =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(groupRowKeys)) {
                    if (usePrev) {
                        groups = groupRowSetSource.getPrevChunk(groupGetContext, groupRowSequence).asObjectChunk();
                    } else {
                        groups = groupRowSetSource.getChunk(groupGetContext, groupRowSequence).asObjectChunk();
                    }
                }

                currentIndex = 0;
                for (int ii = 0; ii < groups.size(); ++ii) {
                    final RowSet currGroupRowSet = groups.get(ii);
                    Assert.neqNull(currGroupRowSet, "currGroupRowSet");
                    final boolean usePrevGroup = usePrev && currGroupRowSet.isTracking();
                    final RowSet groupRowSet = usePrevGroup ? currGroupRowSet.trackingCast().prev() : currGroupRowSet;
                    final int lengthFromGroup = sameGroupRunLengths.get(ii);

                    final WritableLongChunk<OrderedRowKeys> remappedComponentRowKeys =
                            componentRowKeySlice.resetFromTypedChunk(componentRowKeys, currentIndex, lengthFromGroup);
                    // Invert the component row positions to component row keys, in-place
                    groupRowSet.getKeysForPositions(
                            new LongChunkIterator(remappedComponentRowKeys),
                            new LongChunkAppender(remappedComponentRowKeys));

                    currentIndex += lengthFromGroup;
                }

                stateReusable = shared;
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
        tc.shareable.extractFillChunkInformation(aggregateColumnSource.groupRowSetSource, base, false, rowSequence);
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
        tc.shareable.extractFillChunkInformation(aggregateColumnSource.groupRowSetSource, getPrevBase(), true,
                rowSequence);
        tc.doFillChunk(aggregateColumnSource.aggregatedSource, true, destination);
    }
}
