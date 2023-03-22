/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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

            private void extractFillChunkInformation(@NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                    final long base, final boolean usePrev, @NotNull final RowSequence rowSequence) {
                if (stateReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                currentIndex = -1;
                componentKeys.setSize(0);
                rowSequence.forAllRowKeys((final long rowKey) -> {
                    final long indexrowKey = getGroupIndexKey(rowKey, base);
                    if (currentIndex == -1 || indexrowKey != rowKeys.get(currentIndex)) {
                        ++currentIndex;
                        rowKeys.set(currentIndex, indexrowKey);
                        sameIndexRunLengths.set(currentIndex, 1);
                    } else {
                        sameIndexRunLengths.set(currentIndex,
                                sameIndexRunLengths.get(currentIndex) + 1);
                    }
                    final long componentrowKey = getOffsetInGroup(rowKey, base);
                    componentKeys.add(componentrowKey);
                });
                rowKeys.setSize(currentIndex + 1);
                sameIndexRunLengths.setSize(currentIndex + 1);

                final ObjectChunk<RowSet, ? extends Values> indexes;
                try (final RowSequence indexRowSequence =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(rowKeys)) {
                    if (usePrev) {
                        indexes = groupRowSetSource.getPrevChunk(rowsetGetContext, indexRowSequence).asObjectChunk();
                    } else {
                        indexes = groupRowSetSource.getChunk(rowsetGetContext, indexRowSequence).asObjectChunk();
                    }
                }

                currentIndex = 0;
                for (int ii = 0; ii < indexes.size(); ++ii) {
                    final RowSet currRowSet = indexes.get(ii);
                    Assert.neqNull(currRowSet, "currRowSet");
                    final boolean usePrevIndex = usePrev && currRowSet.isTracking();
                    final RowSet rowSet = usePrevIndex ? currRowSet.trackingCast().prev() : currRowSet;
                    final int lengthFromThisIndex = sameIndexRunLengths.get(ii);

                    final WritableLongChunk<OrderedRowKeys> remappedComponentKeys =
                            componentKeySlice.resetFromTypedChunk(componentKeys,
                                    currentIndex, lengthFromThisIndex);
                    rowSet.getKeysForPositions(new LongChunkIterator(componentKeySlice),
                            new LongChunkAppender(remappedComponentKeys));

                    currentIndex += lengthFromThisIndex;
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
