//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharDictionaryWriterIndexKernel and run "./gradlew replicateBarrageUtils" to regenerate
//
// @formatter:off
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.QueryConstants;

final class FloatDictionaryWriterIndexKernel implements DictionaryWriterIndexKernel {
    static final FloatDictionaryWriterIndexKernel INSTANCE = new FloatDictionaryWriterIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final FloatChunk<Values> src = source.asFloatChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForFloat(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final float v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_INT : state.indexForFloat(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForFloat(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final float v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_FLOAT ? QueryConstants.NULL_INT : state.indexForFloat(v));
                    }
                }
            }
        }
    }
}
