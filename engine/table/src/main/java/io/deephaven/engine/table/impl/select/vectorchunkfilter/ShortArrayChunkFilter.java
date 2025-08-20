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
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfShort;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.ShortChunkFilter;
import io.deephaven.vector.ShortVector;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * A wrapper that extracts elements from a short [], returning true for the array if any elements are matched by the
 * wrapped chunk filter.
 */
class ShortArrayChunkFilter extends VectorChunkFilter {
    final WritableShortChunk<? extends Values> temporaryValues;

    ShortArrayChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        super(vectorComponentFilterWrapper, chunkSize);
        temporaryValues = WritableShortChunk.makeWritableChunk(chunkSize);
    }

    @Override
    void doFilter(final Chunk<? extends Values> values,
            final IntPredicate applyFilter,
            final IntConsumer matchConsumer) {
        final ObjectChunk<short[], ? extends Values> objectChunk = values.asObjectChunk();

        if (vectorComponentFilterWrapper.chunkFilter instanceof ShortChunkFilter) {
            final ShortChunkFilter elementFilter = (ShortChunkFilter) vectorComponentFilterWrapper.chunkFilter;

            applySingleElements(applyFilter, matchConsumer, objectChunk, elementFilter);
            return;
        }

        applyChunks(applyFilter, matchConsumer, objectChunk);
    }

    private void applyChunks(IntPredicate applyFilter, IntConsumer matchConsumer,
            ObjectChunk<short[], ? extends Values> objectChunk) {
        temporaryValues.setSize(chunkSize);
        srcPos.setSize(chunkSize);
        int fillPos = 0;

        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final short[] array = objectChunk.get(indexOfVector);
            for (int ii = 0; ii < array.length; ++ii) {
                final short element = array[ii];
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

    private static void applySingleElements(IntPredicate applyFilter, IntConsumer matchConsumer,
            ObjectChunk<short[], ? extends Values> objectChunk, ShortChunkFilter elementFilter) {
        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final short[] array = objectChunk.get(indexOfVector);
            for (int ii = 0; ii < array.length; ++ii) {
                final short element = array[ii];
                if (elementFilter.matches(element)) {
                    matchConsumer.accept(indexOfVector);
                    break;
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
