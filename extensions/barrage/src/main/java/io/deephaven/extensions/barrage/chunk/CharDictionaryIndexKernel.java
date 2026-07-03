//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.QueryConstants;

final class CharDictionaryIndexKernel implements DictionaryIndexKernel {
    static final CharDictionaryIndexKernel INSTANCE = new CharDictionaryIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final CharChunk<Values> src = source.asCharChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            if (options.useDeephavenNulls()) {
                for (int i = 0; i < size; ++i) {
                    out.set(outPos++, state.indexForChar(src.get(i)));
                }
            } else {
                for (int i = 0; i < size; ++i) {
                    final char v = src.get(i);
                    out.set(outPos++,
                            v == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : state.indexForChar(v));
                }
            }
        } else {
            if (options.useDeephavenNulls()) {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        out.set(outPos++, state.indexForChar(src.get((int) it.nextLong())));
                    }
                }
            } else {
                try (final RowSet.Iterator it = subset.iterator()) {
                    while (it.hasNext()) {
                        final char v = src.get((int) it.nextLong());
                        out.set(outPos++,
                                v == QueryConstants.NULL_CHAR ? QueryConstants.NULL_INT : state.indexForChar(v));
                    }
                }
            }
        }
    }
}
