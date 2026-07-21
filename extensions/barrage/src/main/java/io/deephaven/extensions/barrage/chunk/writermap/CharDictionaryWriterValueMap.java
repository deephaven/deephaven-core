//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.writermap;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;
import it.unimi.dsi.fastutil.chars.Char2IntOpenHashMap;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class CharDictionaryWriterValueMap implements DictionaryWriterValueMap {

    private final Char2IntOpenHashMap valueToIndex = new Char2IntOpenHashMap();
    private final CharArrayList values = new CharArrayList();

    CharDictionaryWriterValueMap() {
        valueToIndex.defaultReturnValue(-1);
    }

    private int getOrAdd(final char value) {
        // region canonicalization
        final char key = value;
        // endregion canonicalization
        final int existing = valueToIndex.putIfAbsent(key, values.size());
        if (existing != -1) {
            return existing;
        }
        values.add(key);
        return values.size() - 1;
    }

    @Override
    public void fillIndexChunk(
            @NotNull final Chunk<Values> source,
            @Nullable final RowSet subset,
            final boolean useDeephavenNulls,
            @NotNull final WritableIntChunk<Values> out) {
        final CharChunk<Values> src = source.asCharChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (useDeephavenNulls) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, getOrAdd(src.get(i)));
                }
            } else {
                // NULL_INT written for null rows becomes a cleared bit in the validity bitmap when the
                // downstream index writer serializes this chunk; it is never sent as a real dictionary index.
                for (int i = 0; i < size; ++i) {
                    final char v = src.get(i);
                    out.set(outPos++, v == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : getOrAdd(v));
                }
            }
        } else {
            if (useDeephavenNulls) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, getOrAdd(src.get((int) it.nextLong())));
                    }
                }
            } else {
                // NULL_INT written for null rows becomes a cleared bit in the validity bitmap when the
                // downstream index writer serializes this chunk; it is never sent as a real dictionary index.
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final char v = src.get((int) it.nextLong());
                        out.set(outPos++, v == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : getOrAdd(v));
                    }
                }
            }
        }
    }

    @Override
    @NotNull
    public WritableChunk<Values> buildChunk(final int fromInclusive, final int toExclusive) {
        final int numValues = toExclusive - fromInclusive;
        final WritableCharChunk<Values> out = WritableCharChunk.makeWritableChunk(numValues);
        out.copyFromTypedArray(values.elements(), fromInclusive, 0, numValues);
        out.setSize(numValues);
        return out;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public void reset() {
        valueToIndex.clear();
        values.clear();
    }
}
