package io.deephaven.engine.table.impl.sources.deltaaware;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.chunk.*;

public class ChunkAdapter<T> {
    public static <T> ChunkAdapter<T> create(Class type, final ChunkSink baseline,
            final ChunkSink delta) {
        // noinspection unchecked
        return type == Boolean.class ? (ChunkAdapter<T>) new BooleanChunkAdapter(baseline, delta)
                : new ChunkAdapter<>(baseline, delta);
    }

    /**
     * A copy of DeltaAwareColumnSource.baseline, kept here for convenience
     */
    private final ChunkSink baseline;
    /**
     * A copy of DeltaAwareColumnSource.delta, kept here for convenience, and updated when the corresponding delta
     * changes.
     */
    private ChunkSink delta;
    /**
     * A context suitable for getting data from baseline.
     */
    private final ChunkSource.FillContext baselineContext;
    /**
     * A context suitable for getting data from delta. Updated when the corresponding delta changes.
     */
    private ChunkSource.FillContext deltaContext;
    /**
     * A context suitable for getting filling the delta from a context. Updated when the corresponding delta changes.
     */
    private ChunkSink.FillFromContext deltaFillFromContext;
    /**
     * A custom RowSequence implementation optimized to store a single key, and to be resettable.
     */
    private final SoleKey soleKey;
    /**
     * The allocated chunk (of the proper target type) that we use as a staging area for our values.
     */
    private final WritableChunk<Values> baseChunk;

    ChunkAdapter(final ChunkSink baseline, final ChunkSink delta) {
        this.baseline = baseline;
        this.delta = delta;
        this.baselineContext = baseline.makeFillContext(1);
        this.deltaContext = baseline == delta ? baselineContext : delta.makeFillContext(1);
        this.deltaFillFromContext = delta.makeFillFromContext(1);
        this.soleKey = new SoleKey(-1);
        baseChunk = baseline.getChunkType().makeWritableChunk(1);
    }

    T get(final long index, final long deltaIndex) {
        beginGet(index, deltaIndex);
        return baseChunk.<T>asObjectChunk().get(0);
    }

    Boolean getBoolean(final long index, final long deltaIndex) {
        return (Boolean) get(index, deltaIndex);
    }

    byte getByte(final long index, final long deltaIndex) {
        beginGet(index, deltaIndex);
        return baseChunk.asByteChunk().get(0);
    }

    final char getChar(final long index, final long deltaIndex) {
        beginGet(index, deltaIndex);
        return baseChunk.asCharChunk().get(0);
    }

    final double getDouble(final long index, final long deltaIndex) {
        beginGet(index, deltaIndex);
        return baseChunk.asDoubleChunk().get(0);
    }

    final float getFloat(final long index, final long deltaIndex) {
        beginGet(index, deltaIndex);
        return baseChunk.asFloatChunk().get(0);
    }

    final int getInt(final long index, final long deltaIndex) {
        beginGet(index, deltaIndex);
        return baseChunk.asIntChunk().get(0);
    }

    final long getLong(final long index, final long deltaIndex) {
        beginGet(index, deltaIndex);
        return baseChunk.asLongChunk().get(0);
    }

    final short getShort(final long index, final long deltaIndex) {
        beginGet(index, deltaIndex);
        return baseChunk.asShortChunk().get(0);
    }

    final void set(final long index, final T value) {
        baseChunk.asWritableObjectChunk().set(0, value);
        finishSet(index);
    }

    void set(final long index, final byte value) {
        baseChunk.asWritableByteChunk().set(0, value);
        finishSet(index);
    }

    final void set(final long index, final char value) {
        baseChunk.asWritableCharChunk().set(0, value);
        finishSet(index);
    }

    final void set(final long index, final double value) {
        baseChunk.asWritableDoubleChunk().set(0, value);
        finishSet(index);
    }

    final void set(final long index, final float value) {
        baseChunk.asWritableFloatChunk().set(0, value);
        finishSet(index);
    }

    final void set(final long index, final int value) {
        baseChunk.asWritableIntChunk().set(0, value);
        finishSet(index);
    }

    final void set(final long index, final long value) {
        baseChunk.asWritableLongChunk().set(0, value);
        finishSet(index);
    }

    final void set(final long index, final short value) {
        baseChunk.asWritableShortChunk().set(0, value);
        finishSet(index);
    }

    private void beginGet(final long index, final long deltaIndex) {
        final long whichIndex;
        final ChunkSource whichSrc;
        final ChunkSource.FillContext whichContext;
        if (deltaIndex < 0) {
            whichIndex = index;
            whichSrc = baseline;
            whichContext = baselineContext;
        } else {
            whichIndex = deltaIndex;
            whichSrc = delta;
            whichContext = deltaContext;
        }
        soleKey.setKey(whichIndex);
        whichSrc.fillChunk(whichContext, baseChunk, soleKey);
    }

    private void finishSet(final long index) {
        soleKey.setKey(index);
        delta.fillFromChunk(deltaFillFromContext, baseChunk, soleKey);
    }

    private static class BooleanChunkAdapter extends ChunkAdapter<Boolean> {
        BooleanChunkAdapter(ChunkSink baseline, ChunkSink delta) {
            super(baseline, delta);
        }

        final byte getByte(final long index, final long deltaIndex) {
            return BooleanUtils.booleanAsByte(getBoolean(index, deltaIndex));
        }

        final void set(final long index, final byte value) {
            set(index, BooleanUtils.byteAsBoolean(value));
        }
    }
}
