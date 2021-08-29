/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFiller and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.structures.chunk.util.chunkfillers;

import io.deephaven.engine.v2.sources.ElementSource;
import static io.deephaven.engine.structures.chunk.Attributes.KeyIndices;
import static io.deephaven.engine.structures.chunk.Attributes.Values;

import io.deephaven.engine.v2.sources.WritableSource;
import io.deephaven.engine.structures.chunk.LongChunk;
import io.deephaven.engine.structures.chunk.WritableByteChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.engine.v2.utils.OrderedKeys;
import org.apache.commons.lang3.mutable.MutableInt;

public final class ByteChunkFiller implements ChunkFiller {
    public static final ByteChunkFiller INSTANCE = new ByteChunkFiller();

    @Override
    public final void fillByRanges(final ElementSource src, final OrderedKeys keys, final WritableChunk<? super Values> dest) {
        final WritableByteChunk<? super Values> typedDest = dest.asWritableByteChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.getByte(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final OrderedKeys keys, final WritableChunk<? super Values> dest) {
        final WritableByteChunk<? super Values> typedDest = dest.asWritableByteChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongs(v -> {
            typedDest.set(destPos.intValue(), src.getByte(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final LongChunk<? extends KeyIndices> chunk, final WritableChunk<? super Values> dest) {
        final WritableByteChunk<? super Values> typedDest = dest.asWritableByteChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getByte(chunk.get(i)));
        }
    }

    @Override
    public final void fillPrevByRanges(final ElementSource src, final OrderedKeys keys, final WritableChunk<? super Values> dest) {
        final WritableByteChunk<? super Values> typedDest = dest.asWritableByteChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.getPrevByte(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final OrderedKeys keys, final WritableChunk<? super Values> dest) {
        final WritableByteChunk<? super Values> typedDest = dest.asWritableByteChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongs(v -> {
            typedDest.set(destPos.intValue(), src.getPrevByte(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final LongChunk<? extends KeyIndices> chunk, final WritableChunk<? super Values> dest) {
        final WritableByteChunk<? super Values> typedDest = dest.asWritableByteChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getPrevByte(chunk.get(i)));
        }
    }

    @Override
    public void fillFromSingleValue(ElementSource src, long srcKey, WritableSource dest, OrderedKeys destKeys) {
        final byte value = src.getByte(srcKey);
        destKeys.forAllLongs(destKey -> dest.set(destKey, value));
    }
}
