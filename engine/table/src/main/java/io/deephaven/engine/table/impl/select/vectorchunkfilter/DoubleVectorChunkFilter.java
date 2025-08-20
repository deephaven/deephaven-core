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
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfDouble;
import io.deephaven.engine.table.impl.chunkfilter.DoubleChunkFilter;
import io.deephaven.vector.DoubleVector;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * A wrapper that extracts elements from a DoubleVector, returning true for the DoubleVector if any elements are matched by
 * the wrapped chunk filter.
 */
class DoubleVectorChunkFilter extends VectorChunkFilter {
    final WritableDoubleChunk<? extends Values> temporaryValues;

    DoubleVectorChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        super(vectorComponentFilterWrapper, chunkSize);
        temporaryValues = WritableDoubleChunk.makeWritableChunk(chunkSize);
    }


    @Override
    void doFilter(final Chunk<? extends Values> values,
            final IntPredicate applyFilter,
            final IntConsumer matchConsumer) {
        final ObjectChunk<DoubleVector, ? extends Values> objectChunk = values.asObjectChunk();

        if (vectorComponentFilterWrapper.chunkFilter instanceof DoubleChunkFilter) {
            final DoubleChunkFilter elementFilter = (DoubleChunkFilter) vectorComponentFilterWrapper.chunkFilter;

            applySingleElements(applyFilter, matchConsumer, objectChunk, elementFilter);
            return;
        }

        applyChunks(applyFilter, matchConsumer, objectChunk);
    }

    private void applyChunks(final IntPredicate applyFilter,
            final IntConsumer matchConsumer,
            final ObjectChunk<DoubleVector, ? extends Values> objectChunk) {
        temporaryValues.setSize(chunkSize);
        srcPos.setSize(chunkSize);
        int fillPos = 0;

        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final DoubleVector vector = objectChunk.get(indexOfVector);
            try (final ValueIteratorOfDouble vi = vector.iterator()) {
                while (vi.hasNext()) {
                    final double element = vi.next();
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

    private void applySingleElements(final IntPredicate applyFilter,
            final IntConsumer matchConsumer,
            final ObjectChunk<DoubleVector, ? extends Values> objectChunk,
            final DoubleChunkFilter elementFilter) {
        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final DoubleVector vector = objectChunk.get(indexOfVector);
            try (final ValueIteratorOfDouble vi = vector.iterator()) {
                while (vi.hasNext()) {
                    final double element = vi.next();
                    if (elementFilter.matches(element)) {
                        matchConsumer.accept(indexOfVector);
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        temporaryValues.close();
        super.close();
    }
}
