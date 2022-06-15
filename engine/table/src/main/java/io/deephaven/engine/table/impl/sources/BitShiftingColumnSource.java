/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.CrossJoinShiftState;
import io.deephaven.engine.table.impl.join.dupexpand.DupExpandKernel;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class BitShiftingColumnSource<T> extends AbstractColumnSource<T> implements UngroupableColumnSource {

    private final CrossJoinShiftState shiftState;
    private final ColumnSource<T> innerSource;

    public BitShiftingColumnSource(final CrossJoinShiftState shiftState, @NotNull final ColumnSource<T> innerSource) {
        super(innerSource.getType());
        this.shiftState = shiftState;
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
        return innerSource.get(shiftState.getShifted(rowKey));
    }

    @Override
    public Boolean getBoolean(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getBoolean(shiftState.getShifted(rowKey));
    }

    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        return innerSource.getByte(shiftState.getShifted(rowKey));
    }

    @Override
    public char getChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        return innerSource.getChar(shiftState.getShifted(rowKey));
    }

    @Override
    public double getDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getDouble(shiftState.getShifted(rowKey));
    }

    @Override
    public float getFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getFloat(shiftState.getShifted(rowKey));
    }

    @Override
    public int getInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        return innerSource.getInt(shiftState.getShifted(rowKey));
    }

    @Override
    public long getLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        return innerSource.getLong(shiftState.getShifted(rowKey));
    }

    @Override
    public short getShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return innerSource.getShort(shiftState.getShifted(rowKey));
    }

    @Override
    public T getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getPrev(shiftState.getPrevShifted(rowKey));
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getPrevBoolean(shiftState.getPrevShifted(rowKey));
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        return innerSource.getPrevByte(shiftState.getPrevShifted(rowKey));
    }

    @Override
    public char getPrevChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        return innerSource.getPrevChar(shiftState.getPrevShifted(rowKey));
    }

    @Override
    public double getPrevDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getPrevDouble(shiftState.getPrevShifted(rowKey));
    }

    @Override
    public float getPrevFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getPrevFloat(shiftState.getPrevShifted(rowKey));
    }

    @Override
    public int getPrevInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        return innerSource.getPrevInt(shiftState.getPrevShifted(rowKey));
    }

    @Override
    public long getPrevLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        return innerSource.getPrevLong(shiftState.getPrevShifted(rowKey));
    }

    @Override
    public short getPrevShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return innerSource.getPrevShort(shiftState.getPrevShifted(rowKey));
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
        return ((UngroupableColumnSource) innerSource).getUngroupedSize(shiftState.getShifted(columnIndex));
    }

    @Override
    public long getUngroupedPrevSize(long columnIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevSize(shiftState.getPrevShifted(columnIndex));
    }

    @Override
    public T getUngrouped(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngrouped(shiftState.getShifted(columnIndex), arrayIndex);
    }

    @Override
    public T getUngroupedPrev(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngroupedPrev(shiftState.getPrevShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public Boolean getUngroupedBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedBoolean(shiftState.getShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevBoolean(shiftState.getPrevShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public double getUngroupedDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedDouble(shiftState.getShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public double getUngroupedPrevDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevDouble(shiftState.getPrevShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public float getUngroupedFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedFloat(shiftState.getShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public float getUngroupedPrevFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevFloat(shiftState.getPrevShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public byte getUngroupedByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedByte(shiftState.getShifted(columnIndex), arrayIndex);
    }

    @Override
    public byte getUngroupedPrevByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevByte(shiftState.getPrevShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public char getUngroupedChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedChar(shiftState.getShifted(columnIndex), arrayIndex);
    }

    @Override
    public char getUngroupedPrevChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevChar(shiftState.getPrevShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public short getUngroupedShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedShort(shiftState.getShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public short getUngroupedPrevShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevShort(shiftState.getPrevShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public int getUngroupedInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedInt(shiftState.getShifted(columnIndex), arrayIndex);
    }

    @Override
    public int getUngroupedPrevInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevInt(shiftState.getPrevShifted(columnIndex),
                arrayIndex);
    }

    @Override
    public long getUngroupedLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedLong(shiftState.getShifted(columnIndex), arrayIndex);
    }

    @Override
    public long getUngroupedPrevLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevLong(shiftState.getPrevShifted(columnIndex),
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
        // noinspection unchecked
        return new ReinterpretToOriginal(alternateDataType);
    }

    private class ReinterpretToOriginal<ALTERNATE_DATA_TYPE> extends BitShiftingColumnSource<ALTERNATE_DATA_TYPE> {
        private ReinterpretToOriginal(Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            super(BitShiftingColumnSource.this.shiftState,
                    BitShiftingColumnSource.this.innerSource.reinterpret(alternateDataType));
        }

        @Override
        public boolean allowsReinterpret(@NotNull Class alternateDataType) {
            return alternateDataType == BitShiftingColumnSource.this.getType();
        }

        @Override
        protected <ORIGINAL_TYPE> ColumnSource<ORIGINAL_TYPE> doReinterpret(
                @NotNull Class<ORIGINAL_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ORIGINAL_TYPE>) BitShiftingColumnSource.this;
        }
    }

    private static class FillContext implements ColumnSource.FillContext {

        private final Shareable shareable;
        private final ColumnSource.FillContext innerFillContext;
        private final DupExpandKernel dupExpandKernel;

        private FillContext(final BitShiftingColumnSource cs, final int chunkCapacity,
                final SharedContext sharedContext) {
            shareable = sharedContext == null ? new Shareable(false, chunkCapacity)
                    : sharedContext.getOrCreate(new SharingKey(cs.shiftState),
                            () -> new Shareable(true, chunkCapacity));
            innerFillContext = cs.innerSource.makeFillContext(chunkCapacity, shareable);
            dupExpandKernel = DupExpandKernel.makeDupExpand(cs.getChunkType());
        }

        @Override
        public void close() {
            innerFillContext.close();
            if (!shareable.shared) {
                shareable.close();
            }
        }

        private static final class SharingKey extends SharedContext.ExactReferenceSharingKey<Shareable> {

            private SharingKey(@NotNull final CrossJoinShiftState crossJoinShiftState) {
                super(crossJoinShiftState);
            }
        }

        private static final class Shareable extends SharedContext {

            private final boolean shared;

            private final WritableIntChunk<ChunkLengths> runLengths;
            private final WritableLongChunk<OrderedRowKeys> uniqueIndices;
            private final MutableInt currentRunLength = new MutableInt();
            private final MutableInt currentRunPosition = new MutableInt();
            private final MutableLong currentRunInnerIndexKey = new MutableLong();

            private boolean keysAndLengthsReusable;

            private Shareable(final boolean shared, final int chunkCapacity) {
                this.shared = shared;
                runLengths = WritableIntChunk.makeWritableChunk(chunkCapacity);
                uniqueIndices = WritableLongChunk.makeWritableChunk(chunkCapacity);
            }

            private void ensureKeysAndLengthsInitialized(@NotNull final CrossJoinShiftState shiftState,
                    final boolean usePrev, @NotNull final RowSequence rowSequence) {
                if (keysAndLengthsReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                currentRunLength.setValue(0);
                currentRunPosition.setValue(0);
                currentRunInnerIndexKey.setValue(RowSequence.NULL_ROW_KEY);

                rowSequence.forAllRowKeys((final long indexKey) -> {
                    final long lastInnerIndexKey = currentRunInnerIndexKey.longValue();
                    final long innerIndexKey =
                            usePrev ? shiftState.getPrevShifted(indexKey) : shiftState.getShifted(indexKey);
                    if (innerIndexKey != lastInnerIndexKey) {
                        if (lastInnerIndexKey != RowSequence.NULL_ROW_KEY) {
                            uniqueIndices.set(currentRunPosition.intValue(), lastInnerIndexKey);
                            runLengths.set(currentRunPosition.intValue(), currentRunLength.intValue());
                            currentRunPosition.increment();
                        }
                        currentRunLength.setValue(1);
                        currentRunInnerIndexKey.setValue(innerIndexKey);
                    } else {
                        currentRunLength.increment();
                    }
                });

                uniqueIndices.set(currentRunPosition.intValue(), currentRunInnerIndexKey.longValue());
                runLengths.set(currentRunPosition.intValue(), currentRunLength.intValue());
                uniqueIndices.setSize(currentRunPosition.intValue() + 1);
                runLengths.setSize(currentRunPosition.intValue() + 1);

                keysAndLengthsReusable = shared;
            }

            @Override
            public void reset() {
                keysAndLengthsReusable = false;
                super.reset();
            }

            @Override
            public void close() {
                runLengths.close();
                uniqueIndices.close();
                super.close();
            }
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

    private void doFillChunk(@NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence,
            boolean usePrev) {
        final long sz = rowSequence.size();
        if (sz <= 0) {
            destination.setSize(0);
            return;
        }

        final FillContext effectiveContext = (FillContext) context;
        effectiveContext.shareable.ensureKeysAndLengthsInitialized(shiftState, usePrev, rowSequence);

        try (final RowSequence innerOK =
                RowSequenceFactory.wrapRowKeysChunkAsRowSequence(effectiveContext.shareable.uniqueIndices)) {
            if (usePrev) {
                innerSource.fillPrevChunk(effectiveContext.innerFillContext, destination, innerOK);
            } else {
                innerSource.fillChunk(effectiveContext.innerFillContext, destination, innerOK);
            }
        }

        effectiveContext.dupExpandKernel.expandDuplicates(rowSequence.intSize(), destination,
                effectiveContext.shareable.runLengths);
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
