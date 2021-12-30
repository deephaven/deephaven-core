/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFiller and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.chunkfillers;

import io.deephaven.engine.table.ElementSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import io.deephaven.chunk.attributes.Values;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.apache.commons.lang3.mutable.MutableInt;

public final class BooleanChunkFiller implements ChunkFiller {
    public static final BooleanChunkFiller INSTANCE = new BooleanChunkFiller();

    @Override
    public final void fillByRanges(final ElementSource src, final RowSequence keys, final WritableChunk<? super Values> dest) {
        final WritableBooleanChunk<? super Values> typedDest = dest.asWritableBooleanChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeyRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.getBoolean(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final RowSequence keys, final WritableChunk<? super Values> dest) {
        final WritableBooleanChunk<? super Values> typedDest = dest.asWritableBooleanChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeys(v -> {
            typedDest.set(destPos.intValue(), src.getBoolean(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final LongChunk<? extends RowKeys> chunk, final WritableChunk<? super Values> dest) {
        final WritableBooleanChunk<? super Values> typedDest = dest.asWritableBooleanChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getBoolean(chunk.get(i)));
        }
    }

    @Override
    public final void fillPrevByRanges(final ElementSource src, final RowSequence keys, final WritableChunk<? super Values> dest) {
        final WritableBooleanChunk<? super Values> typedDest = dest.asWritableBooleanChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeyRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.getPrevBoolean(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final RowSequence keys, final WritableChunk<? super Values> dest) {
        final WritableBooleanChunk<? super Values> typedDest = dest.asWritableBooleanChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeys(v -> {
            typedDest.set(destPos.intValue(), src.getPrevBoolean(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final LongChunk<? extends RowKeys> chunk, final WritableChunk<? super Values> dest) {
        final WritableBooleanChunk<? super Values> typedDest = dest.asWritableBooleanChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getPrevBoolean(chunk.get(i)));
        }
    }

    @Override
    public void fillFromSingleValue(ElementSource src, long srcKey, WritableColumnSource dest, RowSequence destKeys) {
        final boolean value = src.getBoolean(srcKey);
        destKeys.forAllRowKeys(destKey -> dest.set(destKey, value));
    }
}
