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
import io.deephaven.util.compare.ShortComparisons;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

public class ShortCompactKernel implements CompactKernel {
    static ShortCompactKernel INSTANCE = new ShortCompactKernel();

    private ShortCompactKernel() {} // use the instance

    /**
     * Compact the values in values by retaining only the positions where retainValues is true.
     *
     * @param values the input and output chunk of values
     * @param retainValues a chunk parallel to values, a value is retained in the output iff retainedValues is true
     */
    public static void compact(WritableShortChunk<? extends Any> values, BooleanChunk<Any> retainValues) {
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
        compact(values.asWritableShortChunk(), retainValues);
    }


    @Override
    public void compactAndCount(WritableChunk<? extends Values> valueChunk, WritableIntChunk<ChunkLengths> counts,
            boolean countNullAndNan) {
        compactAndCount(valueChunk.asWritableShortChunk(), counts, countNullAndNan);
    }

    @Override
    public void compactAndCount(WritableChunk<? extends Values> valueChunk, WritableIntChunk<ChunkLengths> counts,
            IntChunk<ChunkPositions> startPositions, WritableIntChunk<ChunkLengths> lengths, boolean countNullAndNan) {
        compactAndCount(valueChunk.asWritableShortChunk(), counts, startPositions, lengths, countNullAndNan);
    }

    public static void compactAndCount(WritableShortChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts) {
        compactAndCount(valueChunk, counts, false);
    }

    public static void compactAndCount(WritableShortChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts, boolean countNull) {
        final int newSize = compactAndCount(valueChunk, counts, 0, valueChunk.size(), countNull);
        valueChunk.setSize(newSize);
        counts.setSize(newSize);
    }

    public static void compactAndCount(WritableShortChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts, IntChunk<ChunkPositions> startPositions,
            WritableIntChunk<ChunkLengths> lengths, boolean countNullAndNan) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int newSize =
                    compactAndCount(valueChunk, counts, startPositions.get(ii), lengths.get(ii), countNullAndNan);
            lengths.set(ii, newSize);
        }
    }

    public static int compactAndCount(WritableShortChunk<? extends Values> valueChunk,
            WritableIntChunk<ChunkLengths> counts, final int start, final int length, boolean countNullAndNan) {
        int wpos = -1;
        // region compactAndCount
        if (countNullAndNan) {
            valueChunk.sort(start, length);
        } else {
            valueChunk.sortUnsafe(start, length);
        }
        short lastValue = NULL_SHORT;
        int currentCount = -1;
        final int end = start + length;
        for (int rpos = start; rpos < end; ++rpos) {
            final short nextValue = valueChunk.get(rpos);
            if (!countNullAndNan && isNullOrNan(nextValue)) {
                continue;
            }
            if (wpos == -1 || !ShortComparisons.eq(nextValue, lastValue)) {
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

    private static boolean isNullOrNan(short value) {
        // region isNullOrNan
        return value == NULL_SHORT;
        // endregion isNullOrNan
    }
}
