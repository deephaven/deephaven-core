//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

public class BitShiftingColumnSource<T> extends AbstractColumnSource<T> implements UngroupableColumnSource {

    /**
     * Wrap the innerSource if it is not agnostic to redirection. Otherwise, return the innerSource.
     *
     * @param shiftState The cross join shift state to use
     * @param innerSource The column source to redirect
     */
    public static <T> ColumnSource<T> maybeWrap(
            @NotNull final CrossJoinShiftState shiftState,
            @NotNull final ColumnSource<T> innerSource) {
        if (innerSource instanceof RowKeyAgnosticChunkSource) {
            return innerSource;
        }
        return new BitShiftingColumnSource<>(shiftState, innerSource);
    }

    private final CrossJoinShiftState shiftState;
    private final ColumnSource<T> innerSource;

    protected BitShiftingColumnSource(
            @NotNull final CrossJoinShiftState shiftState,
            @NotNull final ColumnSource<T> innerSource) {
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
    public long getUngroupedSize(long groupRowKey) {
        return ((UngroupableColumnSource) innerSource).getUngroupedSize(shiftState.getShifted(groupRowKey));
    }

    @Override
    public long getUngroupedPrevSize(long groupRowKey) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevSize(shiftState.getPrevShifted(groupRowKey));
    }

    @Override
    public T getUngrouped(long groupRowKey, int offsetInGroup) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngrouped(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public T getUngroupedPrev(long groupRowKey, int offsetInGroup) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngroupedPrev(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public Boolean getUngroupedBoolean(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedBoolean(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevBoolean(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public double getUngroupedDouble(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedDouble(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public double getUngroupedPrevDouble(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevDouble(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public float getUngroupedFloat(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedFloat(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public float getUngroupedPrevFloat(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevFloat(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public byte getUngroupedByte(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedByte(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public byte getUngroupedPrevByte(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevByte(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public char getUngroupedChar(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedChar(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public char getUngroupedPrevChar(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevChar(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public short getUngroupedShort(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedShort(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public short getUngroupedPrevShort(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevShort(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public int getUngroupedInt(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedInt(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public int getUngroupedPrevInt(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevInt(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public long getUngroupedLong(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedLong(shiftState.getShifted(groupRowKey),
                offsetInGroup);
    }

    @Override
    public long getUngroupedPrevLong(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevLong(shiftState.getPrevShifted(groupRowKey),
                offsetInGroup);
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
        return new BitShiftingColumnSource<>(shiftState, innerSource.reinterpret(alternateDataType));
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

                final MutableInt currentRunLength = new MutableInt(0);
                final MutableInt currentRunPosition = new MutableInt(0);
                final MutableLong currentRunInnerIndexKey = new MutableLong(RowSequence.NULL_ROW_KEY);

                rowSequence.forAllRowKeys((final long rowKey) -> {
                    final long lastInnerIndexKey = currentRunInnerIndexKey.get();
                    final long innerIndexKey =
                            usePrev ? shiftState.getPrevShifted(rowKey) : shiftState.getShifted(rowKey);
                    if (innerIndexKey != lastInnerIndexKey) {
                        if (lastInnerIndexKey != RowSequence.NULL_ROW_KEY) {
                            uniqueIndices.set(currentRunPosition.get(), lastInnerIndexKey);
                            runLengths.set(currentRunPosition.get(), currentRunLength.get());
                            currentRunPosition.increment();
                        }
                        currentRunLength.set(1);
                        currentRunInnerIndexKey.set(innerIndexKey);
                    } else {
                        currentRunLength.increment();
                    }
                });

                uniqueIndices.set(currentRunPosition.get(), currentRunInnerIndexKey.get());
                runLengths.set(currentRunPosition.get(), currentRunLength.get());
                uniqueIndices.setSize(currentRunPosition.get() + 1);
                runLengths.setSize(currentRunPosition.get() + 1);

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
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
