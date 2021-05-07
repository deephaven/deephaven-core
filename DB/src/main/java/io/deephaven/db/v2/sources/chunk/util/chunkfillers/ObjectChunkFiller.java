/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkFiller and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunk.util.chunkfillers;

import io.deephaven.db.v2.sources.ElementSource;
import static io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import static io.deephaven.db.v2.sources.chunk.Attributes.Values;

import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.apache.commons.lang3.mutable.MutableInt;

public final class ObjectChunkFiller implements ChunkFiller {
    public static final ObjectChunkFiller INSTANCE = new ObjectChunkFiller();

    @Override
    public final void fillByRanges(final ElementSource src, final OrderedKeys keys, final WritableChunk<? super Values> dest) {
        final WritableObjectChunk<Object, ? super Values> typedDest = dest.asWritableObjectChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.get(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final OrderedKeys keys, final WritableChunk<? super Values> dest) {
        final WritableObjectChunk<Object, ? super Values> typedDest = dest.asWritableObjectChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongs(v -> {
            typedDest.set(destPos.intValue(), src.get(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final LongChunk<? extends KeyIndices> chunk, final WritableChunk<? super Values> dest) {
        final WritableObjectChunk<Object, ? super Values> typedDest = dest.asWritableObjectChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.get(chunk.get(i)));
        }
    }

    @Override
    public final void fillPrevByRanges(final ElementSource src, final OrderedKeys keys, final WritableChunk<? super Values> dest) {
        final WritableObjectChunk<Object, ? super Values> typedDest = dest.asWritableObjectChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.intValue(), src.getPrev(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final OrderedKeys keys, final WritableChunk<? super Values> dest) {
        final WritableObjectChunk<Object, ? super Values> typedDest = dest.asWritableObjectChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllLongs(v -> {
            typedDest.set(destPos.intValue(), src.getPrev(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.intValue());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final LongChunk<? extends KeyIndices> chunk, final WritableChunk<? super Values> dest) {
        final WritableObjectChunk<Object, ? super Values> typedDest = dest.asWritableObjectChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getPrev(chunk.get(i)));
        }
    }

    @Override
    public void fillFromSingleValue(ElementSource src, long srcKey, WritableSource dest, OrderedKeys destKeys) {
        final Object value = src.get(srcKey);
        destKeys.forAllLongs(destKey -> dest.set(destKey, value));
    }
}
