package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import io.deephaven.db.v2.CrossJoinShiftState;
import static io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import static io.deephaven.db.v2.sources.chunk.Attributes.Values;
import static io.deephaven.util.QueryConstants.*;

public class BitMaskingColumnSource<T> extends AbstractColumnSource<T>
    implements UngroupableColumnSource {

    private final CrossJoinShiftState shiftState;
    private final ColumnSource<T> innerSource;

    public BitMaskingColumnSource(final CrossJoinShiftState shiftState,
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
    public T get(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.get(shiftState.getMasked(index));
    }

    @Override
    public Boolean getBoolean(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getBoolean(shiftState.getMasked(index));
    }

    @Override
    public byte getByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }
        return innerSource.getByte(shiftState.getMasked(index));
    }

    @Override
    public char getChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        return innerSource.getChar(shiftState.getMasked(index));
    }

    @Override
    public double getDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getDouble(shiftState.getMasked(index));
    }

    @Override
    public float getFloat(long index) {
        if (index < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getFloat(shiftState.getMasked(index));
    }

    @Override
    public int getInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        return innerSource.getInt(shiftState.getMasked(index));
    }

    @Override
    public long getLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        return innerSource.getLong(shiftState.getMasked(index));
    }

    @Override
    public short getShort(long index) {
        if (index < 0) {
            return NULL_SHORT;
        }
        return innerSource.getShort(shiftState.getMasked(index));
    }

    @Override
    public T getPrev(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getPrev(shiftState.getPrevMasked(index));
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        if (index < 0) {
            return null;
        }
        return innerSource.getPrevBoolean(shiftState.getPrevMasked(index));
    }

    @Override
    public byte getPrevByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }
        return innerSource.getPrevByte(shiftState.getPrevMasked(index));
    }

    @Override
    public char getPrevChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        return innerSource.getPrevChar(shiftState.getPrevMasked(index));
    }

    @Override
    public double getPrevDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        return innerSource.getPrevDouble(shiftState.getPrevMasked(index));
    }

    @Override
    public float getPrevFloat(long index) {
        if (index < 0) {
            return NULL_FLOAT;
        }
        return innerSource.getPrevFloat(shiftState.getPrevMasked(index));
    }

    @Override
    public int getPrevInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        return innerSource.getPrevInt(shiftState.getPrevMasked(index));
    }

    @Override
    public long getPrevLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        return innerSource.getPrevLong(shiftState.getPrevMasked(index));
    }

    @Override
    public short getPrevShort(long index) {
        if (index < 0) {
            return NULL_SHORT;
        }
        return innerSource.getPrevShort(shiftState.getPrevMasked(index));
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
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedSize(shiftState.getMasked(columnIndex));
    }

    @Override
    public long getUngroupedPrevSize(long columnIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevSize(shiftState.getPrevMasked(columnIndex));
    }

    @Override
    public T getUngrouped(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource)
            .getUngrouped(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public T getUngroupedPrev(long columnIndex, int arrayIndex) {
        // noinspection unchecked
        return (T) ((UngroupableColumnSource) innerSource)
            .getUngroupedPrev(shiftState.getPrevMasked(columnIndex), arrayIndex);
    }

    @Override
    public Boolean getUngroupedBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedBoolean(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public Boolean getUngroupedPrevBoolean(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevBoolean(shiftState.getPrevMasked(columnIndex), arrayIndex);
    }

    @Override
    public double getUngroupedDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedDouble(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public double getUngroupedPrevDouble(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevDouble(shiftState.getPrevMasked(columnIndex), arrayIndex);
    }

    @Override
    public float getUngroupedFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedFloat(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public float getUngroupedPrevFloat(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevFloat(shiftState.getPrevMasked(columnIndex), arrayIndex);
    }

    @Override
    public byte getUngroupedByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedByte(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public byte getUngroupedPrevByte(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevByte(shiftState.getPrevMasked(columnIndex), arrayIndex);
    }

    @Override
    public char getUngroupedChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedChar(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public char getUngroupedPrevChar(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevChar(shiftState.getPrevMasked(columnIndex), arrayIndex);
    }

    @Override
    public short getUngroupedShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedShort(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public short getUngroupedPrevShort(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevShort(shiftState.getPrevMasked(columnIndex), arrayIndex);
    }

    @Override
    public int getUngroupedInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedInt(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public int getUngroupedPrevInt(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevInt(shiftState.getPrevMasked(columnIndex), arrayIndex);
    }

    @Override
    public long getUngroupedLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedLong(shiftState.getMasked(columnIndex), arrayIndex);
    }

    @Override
    public long getUngroupedPrevLong(long columnIndex, int arrayIndex) {
        return ((UngroupableColumnSource) innerSource)
            .getUngroupedPrevLong(shiftState.getPrevMasked(columnIndex), arrayIndex);
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

    private class ReinterpretToOriginal<ALTERNATE_DATA_TYPE>
        extends BitMaskingColumnSource<ALTERNATE_DATA_TYPE> {
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
            if (cs.innerSource instanceof FillUnordered) {
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

        private static final class SharingKey
            extends SharedContext.ExactReferenceSharingKey<Shareable> {

            private SharingKey(@NotNull final CrossJoinShiftState crossJoinShiftState) {
                super(crossJoinShiftState);
            }
        }

        private static final class Shareable extends SharedContext {

            private final boolean shared;

            private final WritableLongChunk<KeyIndices> maskedKeys;

            private boolean maskedKeysReusable;

            private Shareable(final boolean shared, final int chunkCapacity) {
                this.shared = shared;
                maskedKeys = WritableLongChunk.makeWritableChunk(chunkCapacity);
            }

            private void ensureMaskedKeysInitialized(@NotNull final CrossJoinShiftState shiftState,
                final boolean usePrev, @NotNull final OrderedKeys orderedKeys) {
                if (maskedKeysReusable) {
                    return;
                }
                if (!shared) {
                    reset();
                }

                maskedKeys.setSize(0);
                orderedKeys.forAllLongs((final long indexKey) -> {
                    final long innerIndexKey = usePrev ? shiftState.getPrevMasked(indexKey)
                        : shiftState.getMasked(indexKey);
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
        @NotNull final OrderedKeys orderedKeys) {
        doFillChunk(context, destination, orderedKeys, false);
    }

    @Override
    public void fillPrevChunk(@NotNull final ColumnSource.FillContext context,
        @NotNull final WritableChunk<? super Values> destination,
        @NotNull final OrderedKeys orderedKeys) {
        doFillChunk(context, destination, orderedKeys, true);
    }

    private void doFillChunk(@NotNull final ColumnSource.FillContext context,
        @NotNull final WritableChunk<? super Values> destination,
        @NotNull final OrderedKeys orderedKeys,
        boolean usePrev) {
        // TODO (nate): revisit and decide if it is worth generating all right-side indexes,
        // sorting, compacting,
        // and then permuting back. (Note: fillChunk takes orderedKeys which are unique.)
        final long sz = orderedKeys.size();
        if (sz <= 0) {
            destination.setSize(0);
            return;
        }

        final FillContext effectiveContext = (FillContext) context;
        effectiveContext.shareable.ensureMaskedKeysInitialized(shiftState, usePrev, orderedKeys);
        final WritableLongChunk<KeyIndices> maskedKeys = effectiveContext.shareable.maskedKeys;

        if (innerSource instanceof FillUnordered) {
            final FillUnordered cs = (FillUnordered) innerSource;
            if (usePrev) {
                cs.fillPrevChunkUnordered(effectiveContext.innerFillContext, destination,
                    maskedKeys);
            } else {
                cs.fillChunkUnordered(effectiveContext.innerFillContext, destination, maskedKeys);
            }
        } else {
            // TODO: Apply the same approach as in RORCS
            final ChunkFiller filler = destination.getChunkFiller();
            if (usePrev) {
                filler.fillPrevByIndices(innerSource, maskedKeys, destination);
            } else {
                filler.fillByIndices(innerSource, maskedKeys, destination);
            }
        }

        destination.setSize((int) sz);
    }
}
