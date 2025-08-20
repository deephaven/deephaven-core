//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.vectorchunkfilter;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfChar;
import io.deephaven.engine.table.impl.chunkfilter.CharChunkFilter;
import io.deephaven.vector.CharVector;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * A wrapper that extracts elements from a CharVector, returning true for the CharVector if any elements are matched by
 * the wrapped chunk filter.
 */
class CharVectorChunkFilter extends VectorChunkFilter {
    final WritableCharChunk<? extends Values> temporaryValues;

    CharVectorChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        super(vectorComponentFilterWrapper, chunkSize);
        temporaryValues = WritableCharChunk.makeWritableChunk(chunkSize);
    }


    @Override
    void doFilter(final Chunk<? extends Values> values,
            final IntPredicate applyFilter,
            final IntConsumer matchConsumer) {
        final ObjectChunk<CharVector, ? extends Values> objectChunk = values.asObjectChunk();

        if (vectorComponentFilterWrapper.chunkFilter instanceof CharChunkFilter) {
            final CharChunkFilter elementFilter = (CharChunkFilter) vectorComponentFilterWrapper.chunkFilter;

            applySingleElements(applyFilter, matchConsumer, objectChunk, elementFilter);
            return;
        }

        applyChunks(applyFilter, matchConsumer, objectChunk);
    }

    private void applyChunks(final IntPredicate applyFilter,
            final IntConsumer matchConsumer,
            final ObjectChunk<CharVector, ? extends Values> objectChunk) {
        temporaryValues.setSize(chunkSize);
        srcPos.setSize(chunkSize);
        int fillPos = 0;

        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final CharVector vector = objectChunk.get(indexOfVector);
            try (final ValueIteratorOfChar vi = vector.iterator()) {
                while (vi.hasNext()) {
                    final char element = vi.next();
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
            final ObjectChunk<CharVector, ? extends Values> objectChunk,
            final CharChunkFilter elementFilter) {
        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final CharVector vector = objectChunk.get(indexOfVector);
            try (final ValueIteratorOfChar vi = vector.iterator()) {
                while (vi.hasNext()) {
                    final char element = vi.next();
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
