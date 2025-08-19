//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.vectorchunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfChar;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.vector.CharVector;

class CharArrayChunkFilter extends VectorChunkFilter {
    final WritableCharChunk<? extends Values> temporaryValues;

    CharArrayChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        super(vectorComponentFilterWrapper, chunkSize);
        temporaryValues = WritableCharChunk.makeWritableChunk(chunkSize);
    }

    @Override
    public void filter(final Chunk<? extends Values> values, final LongChunk<OrderedRowKeys> keys,
            final WritableLongChunk<OrderedRowKeys> results) {
        final ObjectChunk<char [], ? extends Values> objectChunk = values.asObjectChunk();
        results.setSize(0);

        temporaryValues.setSize(chunkSize);
        srcPos.setSize(chunkSize);
        int fillPos = 0;

        long lastMatch = RowSet.NULL_ROW_KEY;

        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            final char [] array = objectChunk.get(indexOfVector);
            for (int ii = 0; ii < array.length; ++ii) {
                final char element = array[ii];
                srcPos.set(fillPos, indexOfVector);
                temporaryValues.set(fillPos++, element);
                if (fillPos == chunkSize) {
                    lastMatch = flushMatches(keys, results, fillPos, lastMatch, temporaryValues);
                    fillPos = 0;
                    if (lastMatch == indexOfVector) {
                        break;
                    }
                }
            }
        }
        flushMatches(keys, results, fillPos, lastMatch, temporaryValues);
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
        temporaryValues.close();
        super.close();
    }
}
