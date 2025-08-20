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
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfByte;
import io.deephaven.vector.ByteVector;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * A wrapper that extracts elements from a ByteVector, returning true for the ByteVector if any elements are matched by
 * the wrapped chunk filter.
 */
class ByteVectorChunkFilter extends VectorChunkFilter {
    final WritableByteChunk<? extends Values> temporaryValues;

    ByteVectorChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        super(vectorComponentFilterWrapper, chunkSize);
        temporaryValues = WritableByteChunk.makeWritableChunk(chunkSize);
    }


    @Override
    void doFilter(final Chunk<? extends Values> values,
            final IntPredicate applyFilter,
            final IntConsumer matchConsumer) {
        final ObjectChunk<ByteVector, ? extends Values> objectChunk = values.asObjectChunk();

        temporaryValues.setSize(chunkSize);
        srcPos.setSize(chunkSize);
        int fillPos = 0;

        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final ByteVector vector = objectChunk.get(indexOfVector);
            try (final ValueIteratorOfByte vi = vector.iterator()) {
                while (vi.hasNext()) {
                    final byte element = vi.next();
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
        }
        flushMatches(matchConsumer, fillPos, temporaryValues);
    }


    @Override
    public void close() {
        temporaryValues.close();
        super.close();
    }
}
