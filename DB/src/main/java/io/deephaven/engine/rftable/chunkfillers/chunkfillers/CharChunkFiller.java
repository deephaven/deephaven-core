package io.deephaven.engine.rftable.chunkfillers.chunkfillers;

import io.deephaven.engine.table.ElementSource;
import static io.deephaven.engine.chunk.Attributes.RowKeys;
import static io.deephaven.engine.chunk.Attributes.Values;

import io.deephaven.engine.table.WritableSource;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableCharChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.apache.commons.lang3.mutable.MutableInt;

public final class CharChunkFiller implements ChunkFiller {
    public static final CharChunkFiller INSTANCE = new CharChunkFiller();

    @Override
    public final void fillByRanges(final ElementSource src, final RowSequence keys, final WritableChunk<? super Values> dest) {
        final WritableCharChunk<? super Values> typedDest = dest.asWritableCharChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeyRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.getChar(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final RowSequence keys, final WritableChunk<? super Values> dest) {
        final WritableCharChunk<? super Values> typedDest = dest.asWritableCharChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeys(v -> {
            typedDest.set(destPos.intValue(), src.getChar(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final LongChunk<? extends RowKeys> chunk, final WritableChunk<? super Values> dest) {
        final WritableCharChunk<? super Values> typedDest = dest.asWritableCharChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getChar(chunk.get(i)));
        }
    }

    @Override
    public final void fillPrevByRanges(final ElementSource src, final RowSequence keys, final WritableChunk<? super Values> dest) {
        final WritableCharChunk<? super Values> typedDest = dest.asWritableCharChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeyRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.getPrevChar(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final RowSequence keys, final WritableChunk<? super Values> dest) {
        final WritableCharChunk<? super Values> typedDest = dest.asWritableCharChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeys(v -> {
            typedDest.set(destPos.intValue(), src.getPrevChar(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final LongChunk<? extends RowKeys> chunk, final WritableChunk<? super Values> dest) {
        final WritableCharChunk<? super Values> typedDest = dest.asWritableCharChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getPrevChar(chunk.get(i)));
        }
    }

    @Override
    public void fillFromSingleValue(ElementSource src, long srcKey, WritableSource dest, RowSequence destKeys) {
        final char value = src.getChar(srcKey);
        destKeys.forAllRowKeys(destKey -> dest.set(destKey, value));
    }
}
