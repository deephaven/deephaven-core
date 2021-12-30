/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharCompactKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.util.compact;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;

import static io.deephaven.util.QueryConstants.NULL_FLOAT;

public class FloatCompactKernel implements CompactKernel {
    static FloatCompactKernel INSTANCE = new FloatCompactKernel();
    private FloatCompactKernel() {} // use the instance

    /**
     * Compact the values in values by retaining only the positions where retainValues is true.
     *
     * @param values the input and output chunk of values
     * @param retainValues a chunk parallel to values, a value is retained in the output iff retainedValues is true
     */
     public static void compact(WritableFloatChunk<? extends Any> values, BooleanChunk<Any> retainValues) {
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
        compact(values.asWritableFloatChunk(), retainValues);
    }


    @Override
    public void compactAndCount(WritableChunk<? extends Values> valueChunk, WritableIntChunk<ChunkLengths> counts, boolean countNull) {
        compactAndCount(valueChunk.asWritableFloatChunk(), counts, countNull);
    }

    @Override
    public void compactAndCount(WritableChunk<? extends Values> valueChunk, WritableIntChunk<ChunkLengths> counts, IntChunk<ChunkPositions> startPositions, WritableIntChunk<ChunkLengths> lengths, boolean countNull) {
        compactAndCount(valueChunk.asWritableFloatChunk(), counts, startPositions, lengths, countNull);
    }

    public static void compactAndCount(WritableFloatChunk<? extends Values> valueChunk, WritableIntChunk<ChunkLengths> counts) {
         compactAndCount(valueChunk, counts, false);
    }

    public static void compactAndCount(WritableFloatChunk<? extends Values> valueChunk, WritableIntChunk<ChunkLengths> counts, boolean countNull) {
        final int newSize = compactAndCount(valueChunk, counts, 0, valueChunk.size(), countNull);
        valueChunk.setSize(newSize);
        counts.setSize(newSize);
    }

    public static void compactAndCount(WritableFloatChunk<? extends Values> valueChunk, WritableIntChunk<ChunkLengths> counts, IntChunk<ChunkPositions> startPositions, WritableIntChunk<ChunkLengths> lengths, boolean countNull) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int newSize = compactAndCount(valueChunk, counts, startPositions.get(ii), lengths.get(ii), countNull);
            lengths.set(ii, newSize);
        }
    }

    public static int compactAndCount(WritableFloatChunk<? extends Values> valueChunk, WritableIntChunk<ChunkLengths> counts, final int start, final int length, boolean countNull) {
        int wpos = -1;
        // region compactAndCount
        valueChunk.sort(start, length);
        float lastValue = NULL_FLOAT;
        int currentCount = -1;
        final int end = start + length;
        for (int rpos = start; rpos < end; ++rpos) {
            final float nextValue = valueChunk.get(rpos);
            if (!countNull && shouldIgnore(nextValue)) {
                continue;
            }
            if (wpos == -1 || !FloatComparisons.eq(nextValue, lastValue)) {
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

    private static boolean shouldIgnore(float value) {
         // region shouldIgnore
        return value == NULL_FLOAT || Float.isNaN(value);
        // endregion shouldIgnore
    }
}
