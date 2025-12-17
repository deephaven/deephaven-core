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
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfByte;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.impl.chunkfilter.ByteChunkFilter;
import io.deephaven.vector.ByteVector;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * A wrapper that extracts elements from a byte [], returning true for the array if any elements are matched by the
 * wrapped chunk filter.
 */
class ByteArrayChunkFilter extends VectorChunkFilter {
    final WritableByteChunk<? extends Values> temporaryValues;

    ByteArrayChunkFilter(final VectorComponentFilterWrapper vectorComponentFilterWrapper, final int chunkSize) {
        super(vectorComponentFilterWrapper, chunkSize);
        temporaryValues = WritableByteChunk.makeWritableChunk(chunkSize);
    }

    @Override
    void doFilter(final Chunk<? extends Values> values,
            final IntPredicate applyFilter,
            final IntConsumer matchConsumer) {
        final ObjectChunk<byte[], ? extends Values> objectChunk = values.asObjectChunk();

        if (vectorComponentFilterWrapper.chunkFilter instanceof ByteChunkFilter) {
            final ByteChunkFilter elementFilter = (ByteChunkFilter) vectorComponentFilterWrapper.chunkFilter;

            applySingleElements(applyFilter, matchConsumer, objectChunk, elementFilter);
            return;
        }

        applyChunks(applyFilter, matchConsumer, objectChunk);
    }

    private void applyChunks(IntPredicate applyFilter, IntConsumer matchConsumer,
            ObjectChunk<byte[], ? extends Values> objectChunk) {
        temporaryValues.setSize(chunkSize);
        srcPos.setSize(chunkSize);
        int fillPos = 0;

        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final byte[] array = objectChunk.get(indexOfVector);
            for (int ii = 0; ii < array.length; ++ii) {
                final byte element = array[ii];
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
            ObjectChunk<byte[], ? extends Values> objectChunk, ByteChunkFilter elementFilter) {
        for (int indexOfVector = 0; indexOfVector < objectChunk.size(); ++indexOfVector) {
            if (!applyFilter.test(indexOfVector)) {
                continue;
            }
            final byte[] array = objectChunk.get(indexOfVector);
            for (int ii = 0; ii < array.length; ++ii) {
                final byte element = array[ii];
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
