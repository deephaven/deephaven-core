/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import io.deephaven.engine.table.impl.CrossJoinShiftState;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import io.deephaven.chunk.attributes.Values;
import static io.deephaven.util.QueryConstants.*;

public class BitMaskingColumnSource<T> extends AbstractColumnSource<T> implements UngroupableColumnSource {

    private final CrossJoinShiftState shiftState;
    private final ColumnSource<T> innerSource;

    public BitMaskingColumnSource(final CrossJoinShiftState shiftState, @NotNull final ColumnSource<T> innerSource) {
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
        return innerSource.get(shiftState.getMasked(rowKey));
    }

    @Override
    public Boolean getBoolean(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getBoolean(shiftState.getMasked(rowKey));
    }

    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        return innerSource.getByte(shiftState.getMasked(rowKey));
    }

    @Override
    public char getChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        return innerSource.getChar(shiftState.getMasked(rowKey));
    }

    @Override
    public double getDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getDouble(shiftState.getMasked(rowKey));
    }

    @Override
    public float getFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getFloat(shiftState.getMasked(rowKey));
    }

    @Override
    public int getInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        return innerSource.getInt(shiftState.getMasked(rowKey));
    }

    @Override
    public long getLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        return innerSource.getLong(shiftState.getMasked(rowKey));
    }

    @Override
    public short getShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return innerSource.getShort(shiftState.getMasked(rowKey));
    }

    @Override
    public T getPrev(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getPrev(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        if (rowKey < 0) {
            return null;
        }
        return innerSource.getPrevBoolean(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0) {
            return NULL_BYTE;
        }
        return innerSource.getPrevByte(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public char getPrevChar(long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        return innerSource.getPrevChar(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public double getPrevDouble(long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getPrevDouble(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public float getPrevFloat(long rowKey) {
        if (rowKey < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getPrevFloat(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public int getPrevInt(long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        return innerSource.getPrevInt(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public long getPrevLong(long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        return innerSource.getPrevLong(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public short getPrevShort(long rowKey) {
        if (rowKey < 0) {
            return NULL_SHORT;
        }
        return innerSource.getPrevShort(shiftState.getPrevMasked(rowKey));
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
        return ((UngroupableColumnSource) innerSource).getUngroupedSize(shiftState.getMasked(columnIndex));
    }

    @Override
    public long getUngroupedPrevSize(long columnIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevSize(shiftState.getPrevMasked(columnIndex));
    }

    @Override
    public T getUngrouped(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngrouped(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public T getUngroupedPrev(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngroupedPrev(shiftState.getPrevMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public Boolean getUngroupedBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedBoolean(shiftState.getMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevBoolean(shiftState.getPrevMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public double getUngroupedDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedDouble(shiftState.getMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public double getUngroupedPrevDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevDouble(shiftState.getPrevMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public float getUngroupedFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedFloat(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public float getUngroupedPrevFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevFloat(shiftState.getPrevMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public byte getUngroupedByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedByte(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public byte getUngroupedPrevByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevByte(shiftState.getPrevMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public char getUngroupedChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedChar(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public char getUngroupedPrevChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevChar(shiftState.getPrevMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public short getUngroupedShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedShort(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public short getUngroupedPrevShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevShort(shiftState.getPrevMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public int getUngroupedInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedInt(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public int getUngroupedPrevInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevInt(shiftState.getPrevMasked(columnIndex),
                arrayIndex);
    }

    @Override
    public long getUngroupedLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedLong(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public long getUngroupedPrevLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevLong(shiftState.getPrevMasked(columnIndex),
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

    private class ReinterpretToOriginal<ALTERNATE_DATA_TYPE> extends BitMaskingColumnSource<ALTERNATE_DATA_TYPE> {
        private ReinterpretToOriginal(Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            super(BitMaskingColumnSource.this.shiftState,
                    BitMaskingColumnSource.this.innerSource.reinterpret(alternateDataType));
        }

        @Override
        public boolean allowsReinterpret(@NotNull Class alternateDataType) {
            return alternateDataType == BitMaskingColumnSource.this.getType();
        }

        @Override
        protected <ORIGINAL_TYPE> ColumnSource<ORIGINAL_TYPE> doReinterpret(
                @NotNull Class<ORIGINAL_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ORIGINAL_TYPE>) BitMaskingColumnSource.this;
        }
    }

    private static class FillContext implements ColumnSource.FillContext {

        private final Shareable shareable;
        private final ColumnSource.FillContext innerFillContext;

        private FillContext(final BitMaskingColumnSource cs, final int chunkCapacity,
                final SharedContext sharedContext) {
            shareable = sharedContext == null ? new Shareable(false, chunkCapacity)
                    : sharedContext.getOrCreate(new SharingKey(cs.shiftState),
                            () -> new Shareable(true, chunkCapacity));
            if (FillUnordered.providesFillUnordered(cs.innerSource)) {
                innerFillContext = cs.innerSource.makeFillContext(chunkCapacity, shareable);
            } else {
                innerFillContext = null;
            }
        }

        @Override
        public void close() {
            if (innerFillContext != null) {
                innerFillContext.close();
            }
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

            private final WritableLongChunk<RowKeys> maskedKeys;

            private boolean maskedKeysReusable;

            private Shareable(final boolean shared, final int chunkCapacity) {
                this.shared = shared;
                maskedKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);
            }

            private void ensureMaskedKeysInitialized(@NotNull final CrossJoinShiftState shiftState,
                    final boolean usePrev, @NotNull final RowSequence rowSequence) {
                if (maskedKeysReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                maskedKeys.setSize(0);
                rowSequence.forAllRowKeys((final long indexKey) -> {
                    final long innerIndexKey =
                            usePrev ? shiftState.getPrevMasked(indexKey) : shiftState.getMasked(indexKey);
                    maskedKeys.add(innerIndexKey);
                });

                maskedKeysReusable = shared;
            }

            @Override
            public void reset() {
                maskedKeysReusable = false;
                super.reset();
            }

            @Override
            public void close() {
                maskedKeys.close();
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
        // TODO (nate): revisit and decide if it is worth generating all right-side indexes, sorting, compacting,
        // and then permuting back. (Note: fillChunk takes rowSequence which are unique.)
        final long sz = rowSequence.size();
        if (sz <= 0) {
            destination.setSize(0);
            return;
        }

        final FillContext effectiveContext = (FillContext) context;
        effectiveContext.shareable.ensureMaskedKeysInitialized(shiftState, usePrev, rowSequence);
        final WritableLongChunk<RowKeys> maskedKeys = effectiveContext.shareable.maskedKeys;

        if (FillUnordered.providesFillUnordered(innerSource)) {
            //noinspection unchecked
            final FillUnordered<Values> cs = (FillUnordered<Values>) innerSource;
            if (usePrev) {
                cs.fillPrevChunkUnordered(effectiveContext.innerFillContext, destination, maskedKeys);
            } else {
                cs.fillChunkUnordered(effectiveContext.innerFillContext, destination, maskedKeys);
            }
        } else {
            // TODO: Apply the same approach as in RORCS
            final ChunkFiller filler = ChunkFiller.forChunkType(destination.getChunkType());
            if (usePrev) {
                filler.fillPrevByIndices(innerSource, maskedKeys, destination);
            } else {
                filler.fillByIndices(innerSource, maskedKeys, destination);
            }
        }

        destination.setSize((int) sz);
    }

    @Override
    public boolean isStateless() {
        return innerSource.isStateless();
    }
}
