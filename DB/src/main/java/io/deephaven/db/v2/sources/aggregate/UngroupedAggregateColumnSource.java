package io.deephaven.db.v2.sources.aggregate;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.UngroupedColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.util.LongChunkAppender;
import io.deephaven.db.v2.sources.chunk.util.LongChunkIterator;
import io.deephaven.db.v2.utils.CurrentOnlyIndex;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.db.v2.utils.Index.NULL_KEY;
import static io.deephaven.util.QueryConstants.*;

/**
 * {@link UngroupedColumnSource} implementation for {@link AggregateColumnSource}s.
 */
final class UngroupedAggregateColumnSource<DATA_TYPE> extends UngroupedColumnSource<DATA_TYPE> {

    private final BaseAggregateColumnSource<?, DATA_TYPE> aggregateColumnSource;

    UngroupedAggregateColumnSource(@NotNull final BaseAggregateColumnSource<?, DATA_TYPE> aggregateColumnSource) {
        super(aggregateColumnSource.aggregatedSource.getType());
        this.aggregateColumnSource = aggregateColumnSource;
    }

    @Override
    public final DATA_TYPE get(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return null;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        // noinspection unchecked
        return (DATA_TYPE) aggregateColumnSource.getUngrouped(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final Boolean getBoolean(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_BOOLEAN;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        return aggregateColumnSource.getUngroupedBoolean(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final byte getByte(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_BYTE;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        return aggregateColumnSource.getUngroupedByte(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final char getChar(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_CHAR;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        return aggregateColumnSource.getUngroupedChar(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final double getDouble(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_DOUBLE;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        return aggregateColumnSource.getUngroupedDouble(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final float getFloat(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_FLOAT;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        return aggregateColumnSource.getUngroupedFloat(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final int getInt(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_INT;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        return aggregateColumnSource.getUngroupedInt(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final long getLong(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_LONG;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        return aggregateColumnSource.getUngroupedLong(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final short getShort(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_SHORT;
        }
        final long groupIndexKey = getGroupIndexKey(keyIndex, base);
        final long offsetInGroup = getOffsetInGroup(keyIndex, base);
        return aggregateColumnSource.getUngroupedShort(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final DATA_TYPE getPrev(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return null;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        // noinspection unchecked
        return (DATA_TYPE) aggregateColumnSource.getUngroupedPrev(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final Boolean getPrevBoolean(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_BOOLEAN;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        return aggregateColumnSource.getUngroupedPrevBoolean(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final byte getPrevByte(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_BYTE;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        return aggregateColumnSource.getUngroupedPrevByte(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final char getPrevChar(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_CHAR;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        return aggregateColumnSource.getUngroupedPrevChar(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final double getPrevDouble(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_DOUBLE;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        return aggregateColumnSource.getUngroupedPrevDouble(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final float getPrevFloat(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_FLOAT;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        return aggregateColumnSource.getUngroupedPrevFloat(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final int getPrevInt(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_INT;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        return aggregateColumnSource.getUngroupedPrevInt(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final long getPrevLong(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_LONG;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        return aggregateColumnSource.getUngroupedPrevLong(groupIndexKey, (int) offsetInGroup);
    }

    @Override
    public final short getPrevShort(final long keyIndex) {
        if (keyIndex == NULL_KEY) {
            return NULL_SHORT;
        }
        final long prevBase = getPrevBase();
        final long groupIndexKey = getGroupIndexKey(keyIndex, prevBase);
        final long offsetInGroup = getOffsetInGroup(keyIndex, prevBase);
        return aggregateColumnSource.getUngroupedPrevShort(groupIndexKey, (int) offsetInGroup);
    }

    private static final class UngroupedFillContext implements FillContext {

        private final Shareable shareable;

        private final FillContext aggregatedFillContext;
        private final ResettableWritableChunk<Any> destinationSlice;

        private UngroupedFillContext(@NotNull final BaseAggregateColumnSource<?, ?> aggregateColumnSource,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            final ColumnSource<Index> indexSource = aggregateColumnSource.indexSource;
            final ColumnSource<?> aggregatedSource = aggregateColumnSource.aggregatedSource;

            shareable = sharedContext == null ? new Shareable(false, indexSource, chunkCapacity)
                    : sharedContext.getOrCreate(new SharingKey(indexSource),
                            () -> new Shareable(true, indexSource, chunkCapacity));

            // NB: There's no reason to use a shared context for the values source. We'd have to reset it between each
            // sub-fill.
            aggregatedFillContext = aggregatedSource.makeFillContext(chunkCapacity);
            destinationSlice = aggregatedSource.getChunkType().makeResettableWritableChunk();
        }

        private static final class SharingKey extends SharedContext.ExactReferenceSharingKey<Shareable> {

            private SharingKey(@NotNull final ColumnSource indexSource) {
                super(indexSource);
            }
        }


        private static final class Shareable extends SharedContext {

            private final boolean shared;

            private final GetContext indexGetContext;
            private final WritableLongChunk<OrderedKeyIndices> indexKeyIndices;
            private final WritableIntChunk<ChunkLengths> sameIndexRunLengths;
            private final WritableLongChunk<OrderedKeyIndices> componentKeyIndices;
            private final ResettableWritableLongChunk<OrderedKeyIndices> componentKeyIndicesSlice;

            private boolean stateReusable;
            private int currentIndexPosition;

            private Shareable(final boolean shared,
                    @NotNull final ColumnSource<Index> indexSource,
                    final int chunkCapacity) {
                this.shared = shared;

                indexGetContext = indexSource.makeGetContext(chunkCapacity, this);
                indexKeyIndices = WritableLongChunk.makeWritableChunk(chunkCapacity);
                sameIndexRunLengths = WritableIntChunk.makeWritableChunk(chunkCapacity);
                componentKeyIndices = WritableLongChunk.makeWritableChunk(chunkCapacity);
                componentKeyIndicesSlice = ResettableWritableLongChunk.makeResettableChunk();
            }

            private void extractFillChunkInformation(@NotNull final ColumnSource<? extends Index> indexSource,
                    final long base, final boolean usePrev, @NotNull final OrderedKeys orderedKeys) {
                if (stateReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                currentIndexPosition = -1;
                componentKeyIndices.setSize(0);
                orderedKeys.forAllLongs((final long keyIndex) -> {
                    final long indexKeyIndex = getGroupIndexKey(keyIndex, base);
                    if (currentIndexPosition == -1 || indexKeyIndex != indexKeyIndices.get(currentIndexPosition)) {
                        ++currentIndexPosition;
                        indexKeyIndices.set(currentIndexPosition, indexKeyIndex);
                        sameIndexRunLengths.set(currentIndexPosition, 1);
                    } else {
                        sameIndexRunLengths.set(currentIndexPosition,
                                sameIndexRunLengths.get(currentIndexPosition) + 1);
                    }
                    final long componentKeyIndex = getOffsetInGroup(keyIndex, base);
                    componentKeyIndices.add(componentKeyIndex);
                });
                indexKeyIndices.setSize(currentIndexPosition + 1);
                sameIndexRunLengths.setSize(currentIndexPosition + 1);

                final ObjectChunk<Index, ? extends Values> indexes;
                try (final OrderedKeys indexOrderedKeys =
                        OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(indexKeyIndices)) {
                    if (usePrev) {
                        indexes = indexSource.getPrevChunk(indexGetContext, indexOrderedKeys).asObjectChunk();
                    } else {
                        indexes = indexSource.getChunk(indexGetContext, indexOrderedKeys).asObjectChunk();
                    }
                }

                int componentKeyIndicesPosition = 0;
                for (int ii = 0; ii < indexes.size(); ++ii) {
                    final Index currIndex = indexes.get(ii);
                    Assert.neqNull(currIndex, "currIndex");
                    final boolean usePrevIndex = usePrev && !(currIndex instanceof CurrentOnlyIndex);
                    final Index index = usePrevIndex ? currIndex.getPrevIndex() : currIndex;
                    try {
                        final int lengthFromThisIndex = sameIndexRunLengths.get(ii);

                        final WritableLongChunk<OrderedKeyIndices> remappedComponentKeys =
                                componentKeyIndicesSlice.resetFromTypedChunk(componentKeyIndices,
                                        componentKeyIndicesPosition, lengthFromThisIndex);
                        index.getKeysForPositions(new LongChunkIterator(componentKeyIndicesSlice),
                                new LongChunkAppender(remappedComponentKeys));

                        componentKeyIndicesPosition += lengthFromThisIndex;
                    } finally {
                        if (usePrevIndex) {
                            index.close();
                        }
                    }
                }

                stateReusable = shared;
            }

            @Override
            public final void reset() {
                stateReusable = false;
                super.reset();
            }

            @Override
            public final void close() {
                indexGetContext.close();
                indexKeyIndices.close();
                sameIndexRunLengths.close();
                componentKeyIndices.close();
                componentKeyIndicesSlice.close();

                super.close();
            }
        }

        private void doFillChunk(@NotNull final ColumnSource<?> valueSource, final boolean usePrev,
                @NotNull final WritableChunk<? super Values> destination) {
            int componentKeyIndicesPosition = 0;
            for (int ii = 0; ii < shareable.sameIndexRunLengths.size(); ++ii) {
                final int lengthFromThisIndex = shareable.sameIndexRunLengths.get(ii);

                final WritableLongChunk<OrderedKeyIndices> remappedComponentKeys =
                        shareable.componentKeyIndicesSlice.resetFromTypedChunk(shareable.componentKeyIndices,
                                componentKeyIndicesPosition, lengthFromThisIndex);

                try (final OrderedKeys componentOrderedKeys =
                        OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(remappedComponentKeys)) {
                    if (usePrev) {
                        valueSource.fillPrevChunk(aggregatedFillContext, destinationSlice.resetFromChunk(destination,
                                componentKeyIndicesPosition, lengthFromThisIndex), componentOrderedKeys);
                    } else {
                        valueSource.fillChunk(aggregatedFillContext, destinationSlice.resetFromChunk(destination,
                                componentKeyIndicesPosition, lengthFromThisIndex), componentOrderedKeys);
                    }
                }

                componentKeyIndicesPosition += lengthFromThisIndex;
            }
        }

        @Override
        public final void close() {
            aggregatedFillContext.close();
            destinationSlice.close();

            if (!shareable.shared) {
                shareable.close();
            }
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new UngroupedFillContext(aggregateColumnSource, chunkCapacity, sharedContext);
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        destination.setSize(orderedKeys.intSize());
        if (destination.size() == 0) {
            return;
        }
        final UngroupedFillContext tc = (UngroupedFillContext) context;
        tc.shareable.extractFillChunkInformation(aggregateColumnSource.indexSource, base, false, orderedKeys);
        tc.doFillChunk(aggregateColumnSource.aggregatedSource, false, destination);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        destination.setSize(orderedKeys.intSize());
        if (destination.size() == 0) {
            return;
        }
        final UngroupedFillContext tc = (UngroupedFillContext) context;
        tc.shareable.extractFillChunkInformation(aggregateColumnSource.indexSource, getPrevBase(), true, orderedKeys);
        tc.doFillChunk(aggregateColumnSource.aggregatedSource, true, destination);
    }

    @Override
    public final boolean isImmutable() {
        return aggregateColumnSource.isImmutable();
    }

    private static long getGroupIndexKey(final long keyIndex, final long base) {
        return keyIndex >> base;
    }

    private static long getOffsetInGroup(final long keyIndex, final long base) {
        return keyIndex & ((1 << base) - 1);
    }
}
