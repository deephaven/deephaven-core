//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkFiller and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.chunkfillers;

import io.deephaven.engine.table.ElementSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import io.deephaven.chunk.attributes.Values;

import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.mutable.MutableInt;

public final class DoubleChunkFiller implements ChunkFiller {
    public static final DoubleChunkFiller INSTANCE = new DoubleChunkFiller();

    @Override
    public final void fillByRanges(final ElementSource src, final RowSequence keys,
            final WritableChunk<? super Values> dest) {
        final WritableDoubleChunk<? super Values> typedDest = dest.asWritableDoubleChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeyRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.get(), src.getDouble(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.get());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final RowSequence keys,
            final WritableChunk<? super Values> dest) {
        final WritableDoubleChunk<? super Values> typedDest = dest.asWritableDoubleChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeys(v -> {
            typedDest.set(destPos.get(), src.getDouble(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.get());
    }

    @Override
    public final void fillByIndices(final ElementSource src, final LongChunk<? extends RowKeys> chunk,
            final WritableChunk<? super Values> dest) {
        final WritableDoubleChunk<? super Values> typedDest = dest.asWritableDoubleChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getDouble(chunk.get(i)));
        }
    }

    @Override
    public final void fillPrevByRanges(final ElementSource src, final RowSequence keys,
            final WritableChunk<? super Values> dest) {
        final WritableDoubleChunk<? super Values> typedDest = dest.asWritableDoubleChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeyRanges((start, end) -> {
            for (long v = start; v <= end; ++v) {
                typedDest.set(destPos.get(), src.getPrevDouble(v));
                destPos.increment();
            }
        });
        typedDest.setSize(destPos.get());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final RowSequence keys,
            final WritableChunk<? super Values> dest) {
        final WritableDoubleChunk<? super Values> typedDest = dest.asWritableDoubleChunk();
        final MutableInt destPos = new MutableInt(0);
        keys.forAllRowKeys(v -> {
            typedDest.set(destPos.get(), src.getPrevDouble(v));
            destPos.increment();
        });
        typedDest.setSize(destPos.get());
    }

    @Override
    public final void fillPrevByIndices(final ElementSource src, final LongChunk<? extends RowKeys> chunk,
            final WritableChunk<? super Values> dest) {
        final WritableDoubleChunk<? super Values> typedDest = dest.asWritableDoubleChunk();
        final int sz = chunk.size();
        // Calling setSize early provides a more informative exception if the destination chunk
        // does not have enough capacity.
        typedDest.setSize(sz);
        for (int i = 0; i < sz; ++i) {
            typedDest.set(i, src.getPrevDouble(chunk.get(i)));
        }
    }

    @Override
    public void fillFromSingleValue(ElementSource src, long srcKey, WritableColumnSource dest, RowSequence destKeys) {
        final double value = src.getDouble(srcKey);
        destKeys.forAllRowKeys(destKey -> dest.set(destKey, value));
    }
}
