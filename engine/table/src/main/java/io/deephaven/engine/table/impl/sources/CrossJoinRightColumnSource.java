//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.CrossJoinStateManager;
import io.deephaven.engine.table.impl.join.dupexpand.DupExpandKernel;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.sort.timsort.LongIntTimsortKernel;
import io.deephaven.chunk.ChunkStream;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.ResettableWritableLongChunk;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class CrossJoinRightColumnSource<T> extends AbstractColumnSource<T> implements UngroupableColumnSource {

    /**
     * Wrap the innerSource if it is not agnostic to redirection. Otherwise, return the innerSource.
     *
     * @param crossJoinManager The cross join manager to use
     * @param innerSource The column source to redirect
     * @param rightIsLive Whether the right side is live
     */
    public static <T> ColumnSource<T> maybeWrap(
            @NotNull final CrossJoinStateManager crossJoinManager,
            @NotNull final ColumnSource<T> innerSource,
            boolean rightIsLive) {
        // Force wrapping if this is a leftOuterJoin or else we will not see the nulls; unless every row is null.
        if ((!crossJoinManager.leftOuterJoin() && innerSource instanceof RowKeyAgnosticChunkSource)
                || innerSource instanceof NullValueColumnSource) {
            return innerSource;
        }
        return new CrossJoinRightColumnSource<>(crossJoinManager, innerSource, rightIsLive);
    }

    private final boolean rightIsLive;
    private final CrossJoinStateManager crossJoinManager;
    protected final ColumnSource<T> innerSource;

    protected CrossJoinRightColumnSource(
            @NotNull final CrossJoinStateManager crossJoinManager,
            @NotNull final ColumnSource<T> innerSource,
            boolean rightIsLive) {
        super(innerSource.getType());
        this.rightIsLive = rightIsLive;
        this.crossJoinManager = crossJoinManager;
        this.innerSource = innerSource;
    }

    @Override
    public Class<?> getComponentType() {
        return innerSource.getComponentType();
    }

    @Override
    public void startTrackingPrevValues() {}

    @Override
    public T get(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.get(redirect(rowKey));
    }

    @Override
    public Boolean getBoolean(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getBoolean(redirect(rowKey));
    }

    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        return innerSource.getByte(redirect(rowKey));
    }

    @Override
    public char getChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        return innerSource.getChar(redirect(rowKey));
    }

    @Override
    public double getDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getDouble(redirect(rowKey));
    }

    @Override
    public float getFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getFloat(redirect(rowKey));
    }

    @Override
    public int getInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        return innerSource.getInt(redirect(rowKey));
    }

    @Override
    public long getLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        return innerSource.getLong(redirect(rowKey));
    }

    @Override
    public short getShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return innerSource.getShort(redirect(rowKey));
    }

    @Override
    public T getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getPrev(redirectPrev(rowKey));
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getPrevBoolean(redirectPrev(rowKey));
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        return innerSource.getPrevByte(redirectPrev(rowKey));
    }

    @Override
    public char getPrevChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        return innerSource.getPrevChar(redirectPrev(rowKey));
    }

    @Override
    public double getPrevDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getPrevDouble(redirectPrev(rowKey));
    }

    @Override
    public float getPrevFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getPrevFloat(redirectPrev(rowKey));
    }

    @Override
    public int getPrevInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        return innerSource.getPrevInt(redirectPrev(rowKey));
    }

    @Override
    public long getPrevLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        return innerSource.getPrevLong(redirectPrev(rowKey));
    }

    @Override
    public short getPrevShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return innerSource.getPrevShort(redirectPrev(rowKey));
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public boolean isUngroupable() {
        return innerSource instanceof UngroupableColumnSource
                && ((UngroupableColumnSource) innerSource).isUngroupable();
    }

    @Override
    public long getUngroupedSize(long groupRowKey) {
        return ((UngroupableColumnSource) innerSource).getUngroupedSize(redirect(groupRowKey));
    }

    @Override
    public long getUngroupedPrevSize(long groupRowKey) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevSize(redirectPrev(groupRowKey));
    }

    @Override
    public T getUngrouped(long groupRowKey, int offsetInGroup) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngrouped(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public T getUngroupedPrev(long groupRowKey, int offsetInGroup) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngroupedPrev(redirectPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public Boolean getUngroupedBoolean(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedBoolean(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevBoolean(redirectPrev(groupRowKey),
                offsetInGroup);
    }

    @Override
    public double getUngroupedDouble(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedDouble(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public double getUngroupedPrevDouble(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevDouble(redirectPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public float getUngroupedFloat(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedFloat(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public float getUngroupedPrevFloat(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevFloat(redirectPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public byte getUngroupedByte(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedByte(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public byte getUngroupedPrevByte(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevByte(redirectPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public char getUngroupedChar(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedChar(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public char getUngroupedPrevChar(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevChar(redirectPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public short getUngroupedShort(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedShort(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public short getUngroupedPrevShort(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevShort(redirectPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public int getUngroupedInt(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedInt(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public int getUngroupedPrevInt(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevInt(redirectPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public long getUngroupedLong(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedLong(redirect(groupRowKey), offsetInGroup);
    }

    @Override
    public long getUngroupedPrevLong(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevLong(redirectPrev(groupRowKey), offsetInGroup);
    }

    @Override
    public void releaseCachedResources() {
        super.releaseCachedResources();
        innerSource.releaseCachedResources();
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return innerSource.allowsReinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return new CrossJoinRightColumnSource<>(
                crossJoinManager, innerSource.reinterpret(alternateDataType), rightIsLive);
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(this, chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        doFillChunk(context, destination, rowSequence, false);
    }

    @Override
    public void fillPrevChunk(@NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        doFillChunk(context, destination, rowSequence, true);
    }

    private long redirect(long outerKey) {
        final long leftKey = crossJoinManager.getShifted(outerKey);
        final RowSet rowSet = crossJoinManager.getRightRowSetFromLeftRow(leftKey);
        final long rightKey = crossJoinManager.getMasked(outerKey);
        return rowSet.get(rightKey);
    }

    private long redirectPrev(long outerKey) {
        final long leftKey = crossJoinManager.getPrevShifted(outerKey);
        final TrackingRowSet rowSet = crossJoinManager.getRightRowSetFromPrevLeftRow(leftKey);
        final long rightKey = crossJoinManager.getPrevMasked(outerKey);
        return rightIsLive ? rowSet.getPrev(rightKey) : rowSet.get(rightKey);
    }

    private void doFillChunk(@NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            final boolean usePrev) {
        final int size = rowSequence.intSize();
        if (size <= 0) {
            destination.setSize(0);
            return;
        }
        final FillContext effectiveContext = (FillContext) context;

        effectiveContext.shareable.ensureMappedKeysInitialized(crossJoinManager, usePrev, rowSequence);

        if (FillUnordered.providesFillUnordered(innerSource)) {
            // noinspection unchecked
            effectiveContext.doUnorderedFill((FillUnordered<Values>) innerSource, usePrev, destination);
        } else {
            effectiveContext.doOrderedFillAndPermute(crossJoinManager, innerSource, usePrev, destination);
        }

        destination.setSize(size);
    }

    private static class FillContext implements ColumnSource.FillContext {

        private final FillContext.Shareable shareable;
        private final ColumnSource.FillContext innerFillContext;
        private final WritableChunk<Values> innerOrderedValues;
        private final ResettableWritableChunk<Values> innerOrderedValuesSlice;
        private final DupExpandKernel dupExpandKernel;
        private final PermuteKernel permuteKernel;

        FillContext(final CrossJoinRightColumnSource<?> cs, final int chunkCapacity,
                final SharedContext sharedContext) {
            if (sharedContext == null) {
                shareable = new Shareable(cs.rightIsLive, false, chunkCapacity);
            } else {
                shareable = sharedContext.getOrCreate(new SharingKey(cs.crossJoinManager),
                        () -> new Shareable(cs.rightIsLive, true, chunkCapacity));
            }
            innerFillContext = cs.innerSource.makeFillContext(chunkCapacity, shareable);

            if (FillUnordered.providesFillUnordered(cs.innerSource)) {
                innerOrderedValues = null;
                innerOrderedValuesSlice = null;
                dupExpandKernel = null;
                permuteKernel = null;
            } else {
                innerOrderedValues = cs.getChunkType().makeWritableChunk(chunkCapacity);
                innerOrderedValuesSlice = cs.getChunkType().makeResettableWritableChunk();
                dupExpandKernel = DupExpandKernel.makeDupExpand(cs.getChunkType());
                permuteKernel = PermuteKernel.makePermuteKernel(cs.getChunkType());
            }
        }

        @Override
        public void close() {
            innerFillContext.close();
            if (innerOrderedValues != null) {
                innerOrderedValues.close();
            }
            if (innerOrderedValuesSlice != null) {
                innerOrderedValuesSlice.close();
            }
            if (!shareable.shared) {
                shareable.close();
            }
        }

        private static final class SharingKey extends SharedContext.ExactReferenceSharingKey<FillContext.Shareable> {

            private SharingKey(@NotNull final CrossJoinStateManager crossJoinManager) {
                super(crossJoinManager);
            }
        }

        private static final class Shareable extends SharedContext {

            private final boolean rightIsLive;
            private final boolean shared;

            private final WritableLongChunk<RowKeys> mappedKeys;

            private final LongIntTimsortKernel.LongIntSortKernelContext<RowKeys, ChunkPositions> sortKernelContext;
            private final WritableLongChunk<RowKeys> sortedMappedKeys;
            private final WritableIntChunk<ChunkPositions> mappedKeysOrder;
            private final WritableLongChunk<RowKeys> compactedMappedKeys;
            private final ResettableWritableLongChunk<RowKeys> nonNullCompactedMappedKeys;
            private final WritableIntChunk<ChunkLengths> runLengths;

            private boolean mappedKeysReusable;
            private int totalKeyCount;
            private int uniqueLeftCount;
            private boolean permuteRequired;

            private boolean sortedFillContextReusable;
            private int uniqueKeyCount;
            private boolean hasNulls;
            private RowSequence innerRowSequence;

            private Shareable(final boolean rightIsLive, final boolean shared, final int chunkCapacity) {
                this.rightIsLive = rightIsLive;
                this.shared = shared;

                mappedKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);

                sortKernelContext = LongIntTimsortKernel.createContext(chunkCapacity);
                sortedMappedKeys = shared ? WritableLongChunk.makeWritableChunk(chunkCapacity) : mappedKeys;
                mappedKeysOrder = WritableIntChunk.makeWritableChunk(chunkCapacity);
                // Note that we can't just compact mappedKeys in place, in case we're sharing with another
                // source with an inner source that is a FillUnordered.
                compactedMappedKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);
                nonNullCompactedMappedKeys = ResettableWritableLongChunk.makeResettableChunk();
                runLengths = WritableIntChunk.makeWritableChunk(chunkCapacity);
            }

            private void ensureMappedKeysInitialized(@NotNull final CrossJoinStateManager crossJoinManager,
                    final boolean usePrev, @NotNull final RowSequence rowSequence) {
                if (mappedKeysReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                totalKeyCount = rowSequence.intSize();
                Assert.gtZero(totalKeyCount, "totalKeyCount");
                mappedKeys.setSize(totalKeyCount);

                final MutableInt preMapOffset = new MutableInt();
                final MutableInt postMapOffset = new MutableInt();
                final MutableLong lastLeftIndex = new MutableLong(RowSequence.NULL_ROW_KEY);

                final Runnable flush = () -> {
                    if (lastLeftIndex.get() == RowSequence.NULL_ROW_KEY) {
                        return;
                    }

                    RowSet rightGroup;
                    if (usePrev) {
                        final TrackingRowSet fromTable =
                                crossJoinManager.getRightRowSetFromPrevLeftRow(lastLeftIndex.get());
                        rightGroup = rightIsLive ? fromTable.copyPrev() : fromTable;
                    } else {
                        rightGroup = crossJoinManager.getRightRowSetFromLeftRow(lastLeftIndex.get());
                    }

                    final int alreadyWritten = postMapOffset.get();
                    final int inRightGroup = preMapOffset.get();
                    rightGroup.getKeysForPositions(
                            ChunkStream.of(mappedKeys, alreadyWritten, inRightGroup - alreadyWritten).iterator(),
                            destKey -> {
                                mappedKeys.set(postMapOffset.get(), destKey);
                                postMapOffset.increment();
                            });
                    if (usePrev && rightIsLive) {
                        rightGroup.close();
                    }
                };

                final MutableInt uniqueLeftSideValues = new MutableInt(0);
                rowSequence.forAllRowKeys(ii -> {
                    final long leftIndex =
                            usePrev ? crossJoinManager.getPrevShifted(ii) : crossJoinManager.getShifted(ii);
                    if (leftIndex != lastLeftIndex.get()) {
                        flush.run();
                        lastLeftIndex.set(leftIndex);
                        uniqueLeftSideValues.increment();
                    }
                    mappedKeys.set(preMapOffset.get(),
                            usePrev ? crossJoinManager.getPrevMasked(ii) : crossJoinManager.getMasked(ii));
                    preMapOffset.increment();
                });
                flush.run();
                uniqueLeftCount = uniqueLeftSideValues.get();

                mappedKeysReusable = shared;
            }

            private void ensureSortedFillContextInitialized(final CrossJoinStateManager crossJoinManager) {
                if (sortedFillContextReusable) {
                    return;
                }

                // if we only had one left side value, then we must be exactly
                permuteRequired = uniqueLeftCount > 1;
                if (!permuteRequired) {
                    uniqueKeyCount = mappedKeys.size();
                    hasNulls = mappedKeys.get(0) == RowSequence.NULL_ROW_KEY;
                    if (hasNulls) {
                        Assert.assertion(crossJoinManager.leftOuterJoin(), "crossJoinManager.leftOuterJoin()");
                        innerRowSequence = RowSequenceFactory.EMPTY;
                        Assert.eq(mappedKeys.size(), "mappedKeys.size()", 1);
                    } else {
                        innerRowSequence = RowSequenceFactory.wrapRowKeysChunkAsRowSequence(
                                LongChunk.downcast(mappedKeys));
                    }
                    sortedFillContextReusable = shared;
                    return;
                }

                // Sort keys, and keep track of the positions for permuting the result
                if (shared) {
                    sortedMappedKeys.copyFromTypedChunk(mappedKeys, 0, 0, mappedKeys.size());
                }
                mappedKeysOrder.setSize(totalKeyCount);
                ChunkUtils.fillInOrder(mappedKeysOrder);
                LongIntTimsortKernel.sort(sortKernelContext, mappedKeysOrder, sortedMappedKeys);

                // Compact out duplicates while calculating run lengths
                int currentRunIndex = 0;
                long currentRunKey = sortedMappedKeys.get(0);
                int currentRunLength = 1;
                for (int ki = 1; ki < totalKeyCount; ++ki) {
                    final long currentKey = sortedMappedKeys.get(ki);
                    if (currentKey == currentRunKey) {
                        ++currentRunLength;
                    } else {
                        compactedMappedKeys.set(currentRunIndex, currentRunKey);
                        runLengths.set(currentRunIndex, currentRunLength);
                        ++currentRunIndex;
                        currentRunKey = currentKey;
                        currentRunLength = 1;
                    }
                }
                compactedMappedKeys.set(currentRunIndex, currentRunKey);
                runLengths.set(currentRunIndex, currentRunLength);

                uniqueKeyCount = currentRunIndex + 1;
                compactedMappedKeys.setSize(uniqueKeyCount);
                runLengths.setSize(uniqueKeyCount);

                hasNulls = compactedMappedKeys.get(0) == RowSequence.NULL_ROW_KEY;
                if (hasNulls) {
                    Assert.eqTrue(crossJoinManager.leftOuterJoin(), "crossJoinManager.leftOuterJoin()");
                }
                final int keysToSkip = hasNulls ? 1 : 0;
                innerRowSequence = RowSequenceFactory.wrapRowKeysChunkAsRowSequence(
                        LongChunk.downcast(nonNullCompactedMappedKeys.resetFromTypedChunk(compactedMappedKeys,
                                keysToSkip, uniqueKeyCount - keysToSkip)));

                sortedFillContextReusable = shared;
            }

            @Override
            public void reset() {
                mappedKeysReusable = false;
                totalKeyCount = -1;

                sortedFillContextReusable = false;
                uniqueKeyCount = -1;
                hasNulls = false;
                if (innerRowSequence != null) {
                    innerRowSequence.close();
                    innerRowSequence = null;
                }

                super.reset();
            }

            @Override
            public void close() {
                if (innerRowSequence != null) {
                    innerRowSequence.close();
                    innerRowSequence = null;
                }

                mappedKeys.close();

                sortKernelContext.close();
                if (sortedMappedKeys != mappedKeys) {
                    sortedMappedKeys.close();
                }
                mappedKeysOrder.close();
                compactedMappedKeys.close();
                nonNullCompactedMappedKeys.close();
                runLengths.close();

                super.close();
            }
        }

        private void doUnorderedFill(@NotNull final FillUnordered<Values> innerSource, final boolean usePrev,
                @NotNull final WritableChunk<? super Values> destination) {
            if (usePrev) {
                innerSource.fillPrevChunkUnordered(innerFillContext, destination, shareable.mappedKeys);
            } else {
                innerSource.fillChunkUnordered(innerFillContext, destination, shareable.mappedKeys);
            }
            destination.setSize(shareable.totalKeyCount);
        }

        private void doOrderedFillAndPermute(
                @NotNull final CrossJoinStateManager crossJoinManager,
                @NotNull final ColumnSource<?> innerSource,
                final boolean usePrev,
                @NotNull final WritableChunk<? super Values> destination) {
            shareable.ensureSortedFillContextInitialized(crossJoinManager);

            innerOrderedValues.setSize(shareable.uniqueKeyCount);

            final WritableChunk<? super Values> compactedOrderedValuesDestination;
            if (shareable.hasNulls) {
                if (shareable.permuteRequired) {
                    innerOrderedValues.fillWithNullValue(0, 1);
                    compactedOrderedValuesDestination = innerOrderedValuesSlice.resetFromChunk(
                            innerOrderedValues, 1, shareable.uniqueKeyCount - 1);
                } else {
                    // this can be the only thing we care about
                    Assert.assertion(shareable.innerRowSequence.isEmpty(), "shareable.innerRowSequence.isEmpty()");
                    destination.setSize(shareable.totalKeyCount);
                    destination.fillWithNullValue(0, shareable.totalKeyCount);
                    return;
                }
            } else if (shareable.permuteRequired) {
                compactedOrderedValuesDestination = innerOrderedValues;
            } else {
                compactedOrderedValuesDestination = destination;
            }

            // Read compacted, ordered keys
            if (usePrev) {
                innerSource.fillPrevChunk(innerFillContext, compactedOrderedValuesDestination,
                        shareable.innerRowSequence);
            } else {
                innerSource.fillChunk(innerFillContext, compactedOrderedValuesDestination, shareable.innerRowSequence);
            }

            if (shareable.permuteRequired) {
                // Expand unique values if necessary
                if (shareable.uniqueKeyCount != shareable.totalKeyCount) {
                    dupExpandKernel.expandDuplicates(shareable.totalKeyCount, innerOrderedValues, shareable.runLengths);
                    innerOrderedValues.setSize(shareable.totalKeyCount);
                }

                // Permute expanded, ordered result into destination
                destination.setSize(shareable.totalKeyCount);
                permuteKernel.permute(innerOrderedValues, shareable.mappedKeysOrder, destination);
            }
        }
    }

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
