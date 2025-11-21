//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharCompactKernel and run "./gradlew replicateHashing" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.util.compact;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;


public class ObjectCompactKernel implements CompactKernel {
    static ObjectCompactKernel INSTANCE = new ObjectCompactKernel();

    private ObjectCompactKernel() {} // use the instance

    /**
     * Compact the values in values by retaining only the positions where retainValues is true.
     *
     * @param values the input and output chunk of values
     * @param retainValues a chunk parallel to values, a value is retained in the output iff retainedValues is true
     */
    public static <T> void compact(WritableObjectChunk<T, ? extends Any> values, BooleanChunk<Any> retainValues) {
        int writePosition = 0;
        for (int ii = 0; ii < retainValues.size(); ++ii) {
            if (retainValues.get(ii)) {
                values.set(writePosition++, values.get(ii));
            }
        }
        values.setSize(writePosition);
    }

    @Override
    public void compact(WritableChunk<? extends Any> values, BooleanChunk<Any> retainValues) {
        compact(values.asWritableObjectChunk(), retainValues);
    }


    @Override
    public void compactAndCount(
            WritableChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts,
            boolean countNull,
            boolean countNaN) {
        compactAndCount(valueChunk.asWritableObjectChunk(), counts, countNull, countNaN);
    }

    @Override
    public void compactAndCount(
            WritableChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts,
            IntChunk<ChunkPositions> startPositions,
            WritableIntChunk<ChunkLengths> lengths,
            boolean countNull,
            boolean countNaN) {
        compactAndCount(valueChunk.asWritableObjectChunk(), counts, startPositions, lengths, countNull, countNaN);
    }

    public static <T> void compactAndCount(
            WritableObjectChunk<T, ? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts) {
        compactAndCount(valueChunk, counts, false, false);
    }

    public static <T> void compactAndCount(
            WritableObjectChunk<T, ? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts,
            boolean countNullNaN) {
        compactAndCount(valueChunk, counts, countNullNaN, countNullNaN);
    }

    public static <T> void compactAndCount(
            WritableObjectChunk<T, ? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts,
            boolean countNull,
            boolean countNaN) {
        final int newSize = compactAndCount(valueChunk, counts, 0, valueChunk.size(), countNull, countNaN);
        valueChunk.setSize(newSize);
        counts.setSize(newSize);
    }

    public static <T> void compactAndCount(
            WritableObjectChunk<T, ? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts,
            IntChunk<ChunkPositions> startPositions,
            WritableIntChunk<ChunkLengths> lengths,
            boolean countNullNaN) {
        compactAndCount(valueChunk, counts, startPositions, lengths, countNullNaN, countNullNaN);
    }

    public static <T> void compactAndCount(
            WritableObjectChunk<T, ? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts,
            IntChunk<ChunkPositions> startPositions,
            WritableIntChunk<ChunkLengths> lengths,
            boolean countNull,
            boolean countNaN) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int newSize =
                    compactAndCount(valueChunk, counts, startPositions.get(ii), lengths.get(ii), countNull, countNaN);
            lengths.set(ii, newSize);
        }
    }

    public static <T> int compactAndCount(
            WritableObjectChunk<T, ? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts,
            final int start,
            final int length,
            boolean countNullNaN) {
        return compactAndCount(valueChunk, counts, start, length, countNullNaN, countNullNaN);
    }

    public static <T> int compactAndCount(
            WritableObjectChunk<T, ? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts,
            final int start,
            final int length,
            boolean countNull,
            boolean countNaN) {
        int wpos = -1;
        // region compactAndCount
        valueChunk.sort(start, length);
        Object lastValue = null;
        int currentCount = -1;
        final int end = start + length;
        for (int rpos = start; rpos < end; ++rpos) {
            final T nextValue = valueChunk.get(rpos);
            if (!countNull && nextValue == null) {
                continue;
            }
            // region maybeCountNaN
            // endregion maybeCountNaN
            if (wpos == -1 || !ObjectComparisons.eq(nextValue, lastValue)) {
                valueChunk.set(++wpos + start, nextValue);
                counts.set(wpos + start, currentCount = 1);
                lastValue = nextValue;
                continue;
            }
            counts.set(wpos + start, ++currentCount);
        }
        // endregion compactAndCount
        return wpos + 1;
    }
}
