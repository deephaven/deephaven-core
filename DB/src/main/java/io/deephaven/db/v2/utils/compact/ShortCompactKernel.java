/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharCompactKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.utils.compact;

import io.deephaven.db.util.DhShortComparisons;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;

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
    public void compactAndCount(WritableChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, boolean countNull) {
        compactAndCount(valueChunk.asWritableShortChunk(), counts, countNull);
    }

    @Override
    public void compactAndCount(WritableChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, IntChunk<Attributes.ChunkPositions> startPositions, WritableIntChunk<Attributes.ChunkLengths> lengths, boolean countNull) {
        compactAndCount(valueChunk.asWritableShortChunk(), counts, startPositions, lengths, countNull);
    }

    public static void compactAndCount(WritableShortChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts) {
         compactAndCount(valueChunk, counts, false);
    }

    public static void compactAndCount(WritableShortChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, boolean countNull) {
        final int newSize = compactAndCount(valueChunk, counts, 0, valueChunk.size(), countNull);
        valueChunk.setSize(newSize);
        counts.setSize(newSize);
    }

    public static void compactAndCount(WritableShortChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, IntChunk<Attributes.ChunkPositions> startPositions, WritableIntChunk<Attributes.ChunkLengths> lengths, boolean countNull) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int newSize = compactAndCount(valueChunk, counts, startPositions.get(ii), lengths.get(ii), countNull);
            lengths.set(ii, newSize);
        }
    }

    public static int compactAndCount(WritableShortChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, final int start, final int length, boolean countNull) {
        int wpos = -1;
        // region compactAndCount
        valueChunk.sort(start, length);
        short lastValue = NULL_SHORT;
        int currentCount = -1;
        final int end = start + length;
        for (int rpos = start; rpos < end; ++rpos) {
            final short nextValue = valueChunk.get(rpos);
            if (!countNull && shouldIgnore(nextValue)) {
                continue;
            }
            if (wpos == -1 || !DhShortComparisons.eq(nextValue, lastValue)) {
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

    private static boolean shouldIgnore(short value) {
         // region shouldIgnore
        return value == NULL_SHORT;
        // endregion shouldIgnore
    }
}
