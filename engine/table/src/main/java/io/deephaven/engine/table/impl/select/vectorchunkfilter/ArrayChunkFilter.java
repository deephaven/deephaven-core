//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.vectorchunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.util.SafeCloseable;

abstract class ArrayChunkFilter implements ChunkFilter, SafeCloseable {
    final VectorComponentFilterWrapper vectorComponentFilterWrapper;
    final WritableIntChunk<ChunkPositions> srcPos;
    final WritableBooleanChunk<Values> matched;
    final int chunkSize;

    public ArrayChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        this.vectorComponentFilterWrapper = vectorComponentFilterWrapper;
        this.chunkSize = chunkSize;
        srcPos = WritableIntChunk.makeWritableChunk(chunkSize);
        matched = WritableBooleanChunk.makeWritableChunk(chunkSize);
    }

    long flushMatches(final LongChunk<OrderedRowKeys> keys, final WritableLongChunk<OrderedRowKeys> results,
            final int pos, long lastMatch, final WritableChunk<? extends Values> temporaryValues) {
        temporaryValues.setSize(pos);
        matched.setSize(pos);
        vectorComponentFilterWrapper.chunkFilter.filter(temporaryValues, matched);
        temporaryValues.setSize(chunkSize);
        for (int mm = 0; mm < pos; ++mm) {
            final int thisPos = srcPos.get(mm);
            if (thisPos != lastMatch && matched.get(mm)) {
                results.add(keys.get(thisPos));
                lastMatch = thisPos;
            }
        }
        return lastMatch;
    }

    @Override
    public int filter(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int filterAnd(final Chunk<? extends Values> values, final WritableBooleanChunk<Values> results) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        srcPos.close();
        matched.close();
    }
}
