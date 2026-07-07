//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.QueryConstants;

final class ObjectDictionaryWriterIndexKernel implements DictionaryWriterIndexKernel {
    static final ObjectDictionaryWriterIndexKernel INSTANCE = new ObjectDictionaryWriterIndexKernel();

    @Override
    public void fillIndexChunk(final Chunk<Values> source, final RowSet subset, final BarrageOptions options,
            final DictionaryWriterState state, final WritableIntChunk<Values> out) {
        final ObjectChunk<Object, Values> src = source.asObjectChunk();
        int outPos = 0;
        if (subset == null) {
            final int size = src.size();
            for (int i = 0; i < size; ++i) {
                final Object v = src.get(i);
                out.set(outPos++, v == null ? QueryConstants.NULL_INT : state.indexForObject(v));
            }
        } else {
            try (final RowSet.Iterator it = subset.iterator()) {
                while (it.hasNext()) {
                    final Object v = src.get((int) it.nextLong());
                    out.set(outPos++, v == null ? QueryConstants.NULL_INT : state.indexForObject(v));
                }
            }
        }
    }
}
