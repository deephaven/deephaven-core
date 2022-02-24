package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.table.impl.join.dupexpand.DupExpandKernel;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.sort.timsort.LongIntTimsortKernel;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import io.deephaven.chunk.attributes.Values;
import static io.deephaven.util.QueryConstants.*;

public class RedirectedColumnSource<T> extends AbstractColumnSource<T> implements UngroupableColumnSource {
    protected final RowRedirection rowRedirection;
    protected final ColumnSource<T> innerSource;

    public RedirectedColumnSource(@NotNull final RowRedirection rowRedirection,
            @NotNull final ColumnSource<T> innerSource) {
        super(innerSource.getType());
        this.rowRedirection = rowRedirection;
        this.innerSource = innerSource;
    }

    @Override
    public Class<?> getComponentType() {
        return innerSource.getComponentType();
    }

    public RowRedirection getRowRedirection() {
        return rowRedirection;
    }

    @Override
    public void startTrackingPrevValues() {}

    @Override
    public T get(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.get(rowRedirection.get(index));
    }

    @Override
    public Boolean getBoolean(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getBoolean(rowRedirection.get(index));
    }

    @Override
    public byte getByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }
        return innerSource.getByte(rowRedirection.get(index));
    }

    @Override
    public char getChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        return innerSource.getChar(rowRedirection.get(index));
    }

    @Override
    public double getDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getDouble(rowRedirection.get(index));
    }

    @Override
    public float getFloat(long index) {
        if (index < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getFloat(rowRedirection.get(index));
    }

    @Override
    public int getInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        return innerSource.getInt(rowRedirection.get(index));
    }

    @Override
    public long getLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        return innerSource.getLong(rowRedirection.get(index));
    }

    @Override
    public short getShort(long index) {
        if (index < 0) {
            return NULL_SHORT;
        }
        return innerSource.getShort(rowRedirection.get(index));
    }

    @Override
    public T getPrev(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getPrev(rowRedirection.getPrev(index));
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getPrevBoolean(rowRedirection.getPrev(index));
    }

    @Override
    public byte getPrevByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }
        return innerSource.getPrevByte(rowRedirection.getPrev(index));
    }

    @Override
    public char getPrevChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        return innerSource.getPrevChar(rowRedirection.getPrev(index));
    }

    @Override
    public double getPrevDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getPrevDouble(rowRedirection.getPrev(index));
    }

    @Override
    public float getPrevFloat(long index) {
        if (index < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getPrevFloat(rowRedirection.getPrev(index));
    }

    @Override
    public int getPrevInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        return innerSource.getPrevInt(rowRedirection.getPrev(index));
    }

    @Override
    public long getPrevLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        return innerSource.getPrevLong(rowRedirection.getPrev(index));
    }

    @Override
    public short getPrevShort(long index) {
        if (index < 0) {
            return NULL_SHORT;
        }
        return innerSource.getPrevShort(rowRedirection.getPrev(index));
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
        return ((UngroupableColumnSource) innerSource).getUngroupedSize(rowRedirection.get(columnIndex));
    }

    @Override
    public long getUngroupedPrevSize(long columnIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevSize(rowRedirection.getPrev(columnIndex));
    }

    @Override
    public T getUngrouped(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngrouped(rowRedirection.get(columnIndex), arrayIndex);
    }

    @Override
    public T getUngroupedPrev(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngroupedPrev(rowRedirection.getPrev(columnIndex),
                arrayIndex);
    }

    @Override
    public Boolean getUngroupedBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedBoolean(rowRedirection.get(columnIndex),
                arrayIndex);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevBoolean(rowRedirection.getPrev(columnIndex),
                arrayIndex);
    }

    @Override
    public double getUngroupedDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedDouble(rowRedirection.get(columnIndex),
                arrayIndex);
    }

    @Override
    public double getUngroupedPrevDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevDouble(rowRedirection.getPrev(columnIndex),
                arrayIndex);
    }

    @Override
    public float getUngroupedFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedFloat(rowRedirection.get(columnIndex), arrayIndex);
    }

    @Override
    public float getUngroupedPrevFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevFloat(rowRedirection.getPrev(columnIndex),
                arrayIndex);
    }

    @Override
    public byte getUngroupedByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedByte(rowRedirection.get(columnIndex), arrayIndex);
    }

    @Override
    public byte getUngroupedPrevByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevByte(rowRedirection.getPrev(columnIndex),
                arrayIndex);
    }

    @Override
    public char getUngroupedChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedChar(rowRedirection.get(columnIndex), arrayIndex);
    }

    @Override
    public char getUngroupedPrevChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevChar(rowRedirection.getPrev(columnIndex),
                arrayIndex);
    }

    @Override
    public short getUngroupedShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedShort(rowRedirection.get(columnIndex), arrayIndex);
    }

    @Override
    public short getUngroupedPrevShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevShort(rowRedirection.getPrev(columnIndex),
                arrayIndex);
    }

    @Override
    public int getUngroupedInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedInt(rowRedirection.get(columnIndex), arrayIndex);
    }

    @Override
    public int getUngroupedPrevInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevInt(rowRedirection.getPrev(columnIndex),
                arrayIndex);
    }

    @Override
    public long getUngroupedLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedLong(rowRedirection.get(columnIndex), arrayIndex);
    }

    @Override
    public long getUngroupedPrevLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevLong(rowRedirection.getPrev(columnIndex),
                arrayIndex);
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
        if (TypeUtils.getUnboxedTypeIfBoxed(alternateDataType) == byte.class && getType() == Boolean.class) {
            return new ReinterpretToOriginalForBoolean<>(alternateDataType);
        }
        // noinspection unchecked
        return new ReinterpretToOriginal(alternateDataType);
    }

    private class ReinterpretToOriginal<ALTERNATE_DATA_TYPE>
            extends RedirectedColumnSource<ALTERNATE_DATA_TYPE> {
        private ReinterpretToOriginal(Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            super(RedirectedColumnSource.this.rowRedirection,
                    RedirectedColumnSource.this.innerSource.reinterpret(alternateDataType));
        }

        @Override
        public boolean allowsReinterpret(@NotNull Class alternateDataType) {
            return alternateDataType == RedirectedColumnSource.this.getType();
        }

        @Override
        protected <ORIGINAL_TYPE> ColumnSource<ORIGINAL_TYPE> doReinterpret(
                @NotNull Class<ORIGINAL_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ORIGINAL_TYPE>) RedirectedColumnSource.this;
        }
    }

    private class ReinterpretToOriginalForBoolean<ALTERNATE_DATA_TYPE>
            extends ReinterpretToOriginal<ALTERNATE_DATA_TYPE> {
        private ReinterpretToOriginalForBoolean(Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            super(alternateDataType);
        }

        @Override
        public byte getByte(long index) {
            if (index < 0) {
                return BooleanUtils.NULL_BOOLEAN_AS_BYTE;
            }
            return super.getByte(index);
        }

        @Override
        public byte getPrevByte(long index) {
            if (index < 0) {
                return BooleanUtils.NULL_BOOLEAN_AS_BYTE;
            }
            return super.getPrevByte(index);
        }

        @Override
        public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
            return new FillContext(this, chunkCapacity, sharedContext, true);
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new FillContext(this, chunkCapacity, sharedContext, false);
    }

    @Override
    public void fillChunk(@NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        doFillChunk(context, destination, rowSequence, false);
    }

    @Override
    public void fillPrevChunk(@NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        doFillChunk(context, destination, rowSequence, true);
    }

    private void doFillChunk(@NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence,
            final boolean usePrev) {
        final int size = rowSequence.intSize();
        if (size <= 0) {
            destination.setSize(0);
            return;
        }
        final FillContext effectiveContext = (FillContext) context;

        effectiveContext.shareable.ensureMappedKeysInitialized(rowRedirection, usePrev, rowSequence);

        if (FillUnordered.providesFillUnordered(innerSource)) {
            effectiveContext.doUnorderedFill((FillUnordered) innerSource, usePrev, destination);
        } else {
            effectiveContext.doOrderedFillAndPermute(innerSource, usePrev, destination);
        }

        destination.setSize(size);
    }

    private static class FillContext implements ColumnSource.FillContext {

        private final Shareable shareable;
        private final ColumnSource.FillContext innerFillContext;
        private final WritableChunk<Values> innerOrderedValues;
        private final ResettableWritableChunk<Values> innerOrderedValuesSlice;
        private final DupExpandKernel dupExpandKernel;
        private final PermuteKernel permuteKernel;
        private final boolean booleanNullByte;

        FillContext(final RedirectedColumnSource cs, final int chunkCapacity, final SharedContext sharedContext,
                boolean booleanNullByte) {
            this.booleanNullByte = booleanNullByte;
            shareable = sharedContext == null ? new Shareable(false, cs, chunkCapacity)
                    : sharedContext.getOrCreate(new SharingKey(cs.rowRedirection),
                            () -> new Shareable(true, cs, chunkCapacity));
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

        private static final class SharingKey extends SharedContext.ExactReferenceSharingKey<Shareable> {

            private SharingKey(@NotNull final RowRedirection rowRedirection) {
                super(rowRedirection);
            }
        }

        private static final class Shareable extends SharedContext {

            private final boolean shared;

            private final ChunkSource.FillContext rowRedirectionFillContext;
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

            private Shareable(final boolean shared, final RedirectedColumnSource cs, final int chunkCapacity) {
                this.shared = shared;

                rowRedirectionFillContext = cs.rowRedirection.makeFillContext(chunkCapacity, this);
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

            private void ensureMappedKeysInitialized(@NotNull final RowRedirection rowRedirection,
                    final boolean usePrev, @NotNull final RowSequence rowSequence) {
                if (mappedKeysReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                totalKeyCount = rowSequence.intSize();
                Assert.gtZero(totalKeyCount, "totalKeyCount");

                if (usePrev) {
                    rowRedirection.fillPrevChunk(rowRedirectionFillContext, mappedKeys, rowSequence);
                } else {
                    rowRedirection.fillChunk(rowRedirectionFillContext, mappedKeys, rowSequence);
                }

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

                rowRedirectionFillContext.close();
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

        private void doUnorderedFill(@NotNull final FillUnordered innerSource, final boolean usePrev,
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
                if (booleanNullByte) {
                    innerOrderedValues.asWritableByteChunk().fillWithValue(0, 1, BooleanUtils.NULL_BOOLEAN_AS_BYTE);
                } else {
                    innerOrderedValues.fillWithNullValue(0, 1);
                }
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
    public boolean preventsParallelism() {
        return innerSource.preventsParallelism();
    }

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
