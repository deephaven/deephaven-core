//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.SharedContext;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ZeroKeyCrossJoinShiftState;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import io.deephaven.engine.table.impl.CrossJoinShiftState;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import io.deephaven.chunk.attributes.Values;
import static io.deephaven.util.QueryConstants.*;

public class BitMaskingColumnSource<T> extends AbstractColumnSource<T> implements UngroupableColumnSource {

    /**
     * Wrap the innerSource if it is not agnostic to redirection. Otherwise, return the innerSource.
     *
     * @param shiftState The cross join shift state to use
     * @param innerSource The column source to redirect
     */
    public static <T> ColumnSource<T> maybeWrap(
            final ZeroKeyCrossJoinShiftState shiftState,
            @NotNull final ColumnSource<T> innerSource) {
        if (innerSource instanceof NullValueColumnSource) {
            return innerSource;
        }
        // We must wrap all other sources to leverage shiftState.rightEmpty() and shiftState.rightEmptyPrev()
        // before calling the inner source.
        return new BitMaskingColumnSource<>(shiftState, innerSource);
    }

    private final ZeroKeyCrossJoinShiftState shiftState;
    private final ColumnSource<T> innerSource;

    protected BitMaskingColumnSource(
            final ZeroKeyCrossJoinShiftState shiftState,
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
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return null;
        }
        return innerSource.get(shiftState.getMasked(rowKey));
    }

    @Override
    public Boolean getBoolean(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return null;
        }
        return innerSource.getBoolean(shiftState.getMasked(rowKey));
    }

    @Override
    public byte getByte(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return NULL_BYTE;
        }
        return innerSource.getByte(shiftState.getMasked(rowKey));
    }

    @Override
    public char getChar(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return NULL_CHAR;
        }
        return innerSource.getChar(shiftState.getMasked(rowKey));
    }

    @Override
    public double getDouble(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return NULL_DOUBLE;
        }
        return innerSource.getDouble(shiftState.getMasked(rowKey));
    }

    @Override
    public float getFloat(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return NULL_FLOAT;
        }
        return innerSource.getFloat(shiftState.getMasked(rowKey));
    }

    @Override
    public int getInt(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return NULL_INT;
        }
        return innerSource.getInt(shiftState.getMasked(rowKey));
    }

    @Override
    public long getLong(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return NULL_LONG;
        }
        return innerSource.getLong(shiftState.getMasked(rowKey));
    }

    @Override
    public short getShort(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmpty()) {
            return NULL_SHORT;
        }
        return innerSource.getShort(shiftState.getMasked(rowKey));
    }

    @Override
    public T getPrev(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
            return null;
        }
        return innerSource.getPrev(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public Boolean getPrevBoolean(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
            return null;
        }
        return innerSource.getPrevBoolean(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public byte getPrevByte(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
            return NULL_BYTE;
        }
        return innerSource.getPrevByte(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public char getPrevChar(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
            return NULL_CHAR;
        }
        return innerSource.getPrevChar(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public double getPrevDouble(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
            return NULL_DOUBLE;
        }
        return innerSource.getPrevDouble(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public float getPrevFloat(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
            return NULL_FLOAT;
        }
        return innerSource.getPrevFloat(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public int getPrevInt(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
            return NULL_INT;
        }
        return innerSource.getPrevInt(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public long getPrevLong(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
            return NULL_LONG;
        }
        return innerSource.getPrevLong(shiftState.getPrevMasked(rowKey));
    }

    @Override
    public short getPrevShort(long rowKey) {
        if (rowKey < 0 || shiftState.rightEmptyPrev()) {
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
    public long getUngroupedSize(long groupRowKey) {
        return ((UngroupableColumnSource) innerSource).getUngroupedSize(shiftState.getMasked(groupRowKey));
    }

    @Override
    public long getUngroupedPrevSize(long groupRowKey) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevSize(shiftState.getPrevMasked(groupRowKey));
    }

    @Override
    public T getUngrouped(long groupRowKey, int offsetInGroup) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngrouped(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public T getUngroupedPrev(long groupRowKey, int offsetInGroup) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource).getUngroupedPrev(shiftState.getPrevMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public Boolean getUngroupedBoolean(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedBoolean(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevBoolean(shiftState.getPrevMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public double getUngroupedDouble(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedDouble(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public double getUngroupedPrevDouble(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevDouble(shiftState.getPrevMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public float getUngroupedFloat(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedFloat(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public float getUngroupedPrevFloat(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevFloat(shiftState.getPrevMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public byte getUngroupedByte(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedByte(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public byte getUngroupedPrevByte(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevByte(shiftState.getPrevMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public char getUngroupedChar(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedChar(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public char getUngroupedPrevChar(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevChar(shiftState.getPrevMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public short getUngroupedShort(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedShort(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public short getUngroupedPrevShort(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevShort(shiftState.getPrevMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public int getUngroupedInt(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedInt(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public int getUngroupedPrevInt(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevInt(shiftState.getPrevMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public long getUngroupedLong(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedLong(shiftState.getMasked(groupRowKey),
                offsetInGroup);
    }

    @Override
    public long getUngroupedPrevLong(long groupRowKey, int offsetInGroup) {
        return ((UngroupableColumnSource) innerSource).getUngroupedPrevLong(shiftState.getPrevMasked(groupRowKey),
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
        return new BitMaskingColumnSource<>(shiftState, innerSource.reinterpret(alternateDataType));
    }

    private static class FillContext implements ColumnSource.FillContext {

        private final Shareable shareable;
        private final ColumnSource.FillContext innerFillContext;

        private FillContext(final BitMaskingColumnSource<?> cs, final int chunkCapacity,
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
                rowSequence.forAllRowKeys((final long rowKey) -> {
                    final long innerIndexKey =
                            usePrev ? shiftState.getPrevMasked(rowKey) : shiftState.getMasked(rowKey);
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
        // TODO (CPW): an alternative to doing a sort and permute is to record the list of "backwards" transitions,
        // each region of the column source will go back to the start. We can record that and know how many sub-chunks
        // need to be filled. We could even get fancy and mark if they are all identical (modulo offset in the first
        // and length last chunk), in which case we could do a single fill; then just copy it around as necessary
        final long sz = rowSequence.size();
        if (sz <= 0) {
            destination.setSize(0);
            return;
        }

        final int intSz = rowSequence.intSize();
        if (usePrev && shiftState.rightEmptyPrev()) {
            destination.setSize(intSz);
            destination.fillWithNullValue(0, intSz);
            return;
        } else if (!usePrev && shiftState.rightEmpty()) {
            destination.setSize(intSz);
            destination.fillWithNullValue(0, intSz);
            return;
        }

        final FillContext effectiveContext = (FillContext) context;
        effectiveContext.shareable.ensureMaskedKeysInitialized(shiftState, usePrev, rowSequence);
        final WritableLongChunk<RowKeys> maskedKeys = effectiveContext.shareable.maskedKeys;

        if (FillUnordered.providesFillUnordered(innerSource)) {
            // noinspection unchecked
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
