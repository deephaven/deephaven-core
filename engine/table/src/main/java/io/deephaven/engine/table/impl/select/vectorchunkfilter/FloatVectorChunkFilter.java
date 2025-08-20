//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharVectorChunkFilter and run "./gradlew replicateVectorChunkFilters" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.vectorchunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfFloat;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.FloatVector;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

class FloatVectorChunkFilter extends VectorChunkFilter {
    final WritableFloatChunk<? extends Values> temporaryValues;

    FloatVectorChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        super(vectorComponentFilterWrapper, chunkSize);
        temporaryValues = WritableFloatChunk.makeWritableChunk(chunkSize);
    }


    @Override
    void doFilter(final Chunk<? extends Values> values,
                          final IntPredicate applyFilter,
                          final IntConsumer matchConsumer) {
            final ObjectChunk<FloatVector, ? extends Values> objectChunk = values.asObjectChunk();

        temporaryValues.setSize(chunkSize);
        srcPos.setSize(chunkSize);
        int fillPos = 0;

        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final FloatVector vector = objectChunk.get(indexOfVector);
            try (final ValueIteratorOfFloat vi = vector.iterator()) {
                while (vi.hasNext()) {
                    final float element = vi.next();
                    srcPos.set(fillPos, indexOfVector);
                    temporaryValues.set(fillPos++, element);
                    if (fillPos == chunkSize) {
                        final long lastMatch = flushMatches(matchConsumer, fillPos, temporaryValues);
                        fillPos = 0;
                        if (lastMatch == indexOfVector) {
                            break;
                        }
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
