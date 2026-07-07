//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Object-keyed {@link DictionaryWriterValueMap}. Null values are never dictionary-encoded; they always map to
 * {@code QueryConstants.NULL_INT} regardless of {@code useDeephavenNulls}.
 */
final class ObjectDictionaryWriterValueMap implements DictionaryWriterValueMap {

    private final Object2IntOpenHashMap<Object> valueToIndex = new Object2IntOpenHashMap<>();
    private final ObjectArrayList<Object> values = new ObjectArrayList<>();

    ObjectDictionaryWriterValueMap() {
        valueToIndex.defaultReturnValue(-1);
    }

    private int getOrAdd(final Object value) {
        final int existing = valueToIndex.putIfAbsent(value, values.size());
        if (existing != -1) {
            return existing;
        }
        values.add(value);
        return values.size() - 1;
    }

    @Override
    public void fillIndexChunk(
            @NotNull final Chunk<Values> source,
            @Nullable final RowSet subset,
            final boolean useDeephavenNulls,
            @NotNull final WritableIntChunk<Values> out) {
        final ObjectChunk<Object, Values> src = source.asObjectChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            for (int i = 0; i < size; ++i) {
                final Object v = src.get(i);
                out.set(outPos++, v == null ? QueryConstants.NULL_INT : getOrAdd(v));
            }
        } else {
            try (final RowSet.Iterator it = subset.iterator()) {
                while (it.hasNext()) {
                    final Object v = src.get((int) it.nextLong());
                    out.set(outPos++, v == null ? QueryConstants.NULL_INT : getOrAdd(v));
                }
            }
        }
    }

    @Override
    @NotNull
    public WritableChunk<Values> buildChunk(final int fromInclusive, final int toExclusive) {
        final int numValues = toExclusive - fromInclusive;
        final WritableObjectChunk<Object, Values> out = WritableObjectChunk.makeWritableChunk(numValues);
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
