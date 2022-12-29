/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class CrossJoinRightColumnSource<T> extends AbstractColumnSource<T> implements UngroupableColumnSource {
    private final boolean rightIsLive;
    private final CrossJoinStateManager crossJoinManager;
    protected final ColumnSource<T> innerSource;


    public CrossJoinRightColumnSource(@NotNull final CrossJoinStateManager crossJoinManager,
            @NotNull final ColumnSource<T> innerSource, boolean rightIsLive) {
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
    public long getUngroupedSize(long columnIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedSize(redirect(columnIndex));
    }

    @Override
    public long getUngroupedPrevSize(long columnIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevSize(redirectPrev(columnIndex));
    }

    @Override
    public T getUngrouped(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngrouped(redirect(columnIndex), arrayIndex);
    }

    @Override
    public T getUngroupedPrev(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngroupedPrev(redirectPrev(columnIndex), arrayIndex);
    }

    @Override
    public Boolean getUngroupedBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedBoolean(redirect(columnIndex), arrayIndex);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevBoolean(redirectPrev(columnIndex), arrayIndex);
    }

    @Override
    public double getUngroupedDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedDouble(redirect(columnIndex), arrayIndex);
    }

    @Override
    public double getUngroupedPrevDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevDouble(redirectPrev(columnIndex), arrayIndex);
    }

    @Override
    public float getUngroupedFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedFloat(redirect(columnIndex), arrayIndex);
    }

    @Override
    public float getUngroupedPrevFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevFloat(redirectPrev(columnIndex), arrayIndex);
    }

    @Override
    public byte getUngroupedByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedByte(redirect(columnIndex), arrayIndex);
    }

    @Override
    public byte getUngroupedPrevByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevByte(redirectPrev(columnIndex), arrayIndex);
    }

    @Override
    public char getUngroupedChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedChar(redirect(columnIndex), arrayIndex);
    }

    @Override
    public char getUngroupedPrevChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevChar(redirectPrev(columnIndex), arrayIndex);
    }

    @Override
    public short getUngroupedShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedShort(redirect(columnIndex), arrayIndex);
    }

    @Override
    public short getUngroupedPrevShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevShort(redirectPrev(columnIndex), arrayIndex);
    }

    @Override
    public int getUngroupedInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedInt(redirect(columnIndex), arrayIndex);
    }

    @Override
    public int getUngroupedPrevInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevInt(redirectPrev(columnIndex), arrayIndex);
    }

    @Override
    public long getUngroupedLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedLong(redirect(columnIndex), arrayIndex);
    }

    @Override
    public long getUngroupedPrevLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevLong(redirectPrev(columnIndex), arrayIndex);
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
        return new ReinterpretToOriginal<>(alternateDataType);
    }

    private class ReinterpretToOriginal<ALTERNATE_DATA_TYPE> extends CrossJoinRightColumnSource<ALTERNATE_DATA_TYPE> {
        private ReinterpretToOriginal(Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            super(CrossJoinRightColumnSource.this.crossJoinManager,
                    CrossJoinRightColumnSource.this.innerSource.reinterpret(alternateDataType), rightIsLive);
        }

        @Override
        public <INNER_ALTERNATIVE_DATA_TYPE> boolean allowsReinterpret(
                @NotNull Class<INNER_ALTERNATIVE_DATA_TYPE> alternateDataType) {
            return alternateDataType == CrossJoinRightColumnSource.this.getType();
        }

        @Override
        protected <ORIGINAL_TYPE> ColumnSource<ORIGINAL_TYPE> doReinterpret(
                @NotNull Class<ORIGINAL_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ORIGINAL_TYPE>) CrossJoinRightColumnSource.this;
        }
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
        final RowSet rowSet = crossJoinManager.getRightRowSetFromLeftIndex(leftKey);
        final long rightKey = crossJoinManager.getMasked(outerKey);
        return rowSet.get(rightKey);
    }

    private long redirectPrev(long outerKey) {
        final long leftKey = crossJoinManager.getPrevShifted(outerKey);
        final TrackingRowSet rowSet = crossJoinManager.getRightRowSetFromPrevLeftIndex(leftKey);
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
            effectiveContext.doOrderedFillAndPermute(innerSource, usePrev, destination);
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
                    if (lastLeftIndex.longValue() == RowSequence.NULL_ROW_KEY) {
                        return;
                    }

                    RowSet rightGroup;
                    if (usePrev) {
                        final TrackingRowSet fromTable =
                                crossJoinManager.getRightRowSetFromPrevLeftIndex(lastLeftIndex.getValue());
                        rightGroup = rightIsLive ? fromTable.copyPrev() : fromTable;
                    } else {
                        rightGroup = crossJoinManager.getRightRowSetFromLeftIndex(lastLeftIndex.getValue());
                    }

                    final int alreadyWritten = postMapOffset.intValue();
                    final int inRightGroup = preMapOffset.intValue();
                    rightGroup.getKeysForPositions(
                            ChunkStream.of(mappedKeys, alreadyWritten, inRightGroup - alreadyWritten).iterator(),
                            destKey -> {
                                mappedKeys.set(postMapOffset.intValue(), destKey);
                                postMapOffset.increment();
                            });
                    if (usePrev && rightIsLive) {
                        rightGroup.close();
                    }
                };

                rowSequence.forAllRowKeys(ii -> {
                    final long leftIndex =
                            usePrev ? crossJoinManager.getPrevShifted(ii) : crossJoinManager.getShifted(ii);
                    if (leftIndex != lastLeftIndex.longValue()) {
                        flush.run();
                        lastLeftIndex.setValue(leftIndex);
                    }
                    mappedKeys.set(preMapOffset.intValue(),
                            usePrev ? crossJoinManager.getPrevMasked(ii) : crossJoinManager.getMasked(ii));
                    preMapOffset.increment();
                });
                flush.run();

                mappedKeysReusable = shared;
            }

            private void ensureSortedFillContextInitialized() {
                if (sortedFillContextReusable) {
                    return;
                }

                // Sort keys, and keep track of the positions for permuting the result
                if (shared) {
                    sortedMappedKeys.copyFromTypedChunk(mappedKeys, 0, 0, mappedKeys.size());
                }
                for (int ki = 0; ki < totalKeyCount; ++ki) {
                    mappedKeysOrder.set(ki, ki);
                }
                mappedKeysOrder.setSize(totalKeyCount);
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

        private void doOrderedFillAndPermute(@NotNull final ColumnSource<?> innerSource, final boolean usePrev,
                @NotNull final WritableChunk<? super Values> destination) {
            shareable.ensureSortedFillContextInitialized();

            innerOrderedValues.setSize(shareable.uniqueKeyCount);

            final WritableChunk<Values> compactedOrderedValuesDestination;
            if (shareable.hasNulls) {
                innerOrderedValues.fillWithNullValue(0, 1);
                compactedOrderedValuesDestination =
                        innerOrderedValuesSlice.resetFromChunk(innerOrderedValues, 1, shareable.uniqueKeyCount - 1);
            } else {
                compactedOrderedValuesDestination = innerOrderedValues;
            }

            // Read compacted, ordered keys
            if (usePrev) {
                innerSource.fillPrevChunk(innerFillContext, compactedOrderedValuesDestination,
                        shareable.innerRowSequence);
            } else {
                innerSource.fillChunk(innerFillContext, compactedOrderedValuesDestination, shareable.innerRowSequence);
            }

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

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
