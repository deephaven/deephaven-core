//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharArrayChunkFilter and run "./gradlew replicateVectorChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.vectorchunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfInt;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.vector.IntVector;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * A wrapper that extracts elements from a int [], returning true for the array if any elements are matched by the
 * wrapped chunk filter.
 */
class IntArrayChunkFilter extends VectorChunkFilter {
    final WritableIntChunk<? extends Values> temporaryValues;

    IntArrayChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        super(vectorComponentFilterWrapper, chunkSize);
        temporaryValues = WritableIntChunk.makeWritableChunk(chunkSize);
    }

    @Override
    void doFilter(final Chunk<? extends Values> values,
            final IntPredicate applyFilter,
            final IntConsumer matchConsumer) {
        final ObjectChunk<int[], ? extends Values> objectChunk = values.asObjectChunk();

        temporaryValues.setSize(chunkSize);
        srcPos.setSize(chunkSize);
        int fillPos = 0;

        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            final int[] array = objectChunk.get(indexOfVector);
            for (int ii = 0; ii < array.length; ++ii) {
                final int element = array[ii];
                srcPos.set(fillPos, indexOfVector);
                temporaryValues.set(fillPos++, element);
                if (fillPos == chunkSize) {
                    final int lastMatch = flushMatches(matchConsumer, fillPos, temporaryValues);
                    fillPos = 0;
                    if (lastMatch == indexOfVector) {
                        break;
                    }
                }
            }
        }
        flushMatches(matchConsumer, fillPos, temporaryValues);
    }

    @Override
    public void close() {
        temporaryValues.close();
        super.close();
    }
}
