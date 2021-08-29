/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharCompactKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.utils.compact;

import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.Any;


public class BooleanCompactKernel implements CompactKernel {
    static BooleanCompactKernel INSTANCE = new BooleanCompactKernel();
    private BooleanCompactKernel() {} // use the instance

    /**
     * Compact the values in values by retaining only the positions where retainValues is true.
     *
     * @param values the input and output chunk of values
     * @param retainValues a chunk parallel to values, a value is retained in the output iff retainedValues is true
     */
     public static void compact(WritableBooleanChunk<? extends Any> values, BooleanChunk<Any> retainValues) {
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
        compact(values.asWritableBooleanChunk(), retainValues);
    }


    @Override
    public void compactAndCount(WritableChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, boolean countNull) {
        compactAndCount(valueChunk.asWritableBooleanChunk(), counts, countNull);
    }

    @Override
    public void compactAndCount(WritableChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, IntChunk<Attributes.ChunkPositions> startPositions, WritableIntChunk<Attributes.ChunkLengths> lengths, boolean countNull) {
        compactAndCount(valueChunk.asWritableBooleanChunk(), counts, startPositions, lengths, countNull);
    }

    public static void compactAndCount(WritableBooleanChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts) {
         compactAndCount(valueChunk, counts, false);
    }

    public static void compactAndCount(WritableBooleanChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, boolean countNull) {
        final int newSize = compactAndCount(valueChunk, counts, 0, valueChunk.size(), countNull);
        valueChunk.setSize(newSize);
        counts.setSize(newSize);
    }

    public static void compactAndCount(WritableBooleanChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, IntChunk<Attributes.ChunkPositions> startPositions, WritableIntChunk<Attributes.ChunkLengths> lengths, boolean countNull) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int newSize = compactAndCount(valueChunk, counts, startPositions.get(ii), lengths.get(ii), countNull);
            lengths.set(ii, newSize);
        }
    }

    public static int compactAndCount(WritableBooleanChunk<? extends Attributes.Values> valueChunk, WritableIntChunk<Attributes.ChunkLengths> counts, final int start, final int length, boolean countNull) {
        int wpos = -1;
        // region compactAndCount
        int trueValues = 0;        int falseValues = 0;        final int end = start + length;        for (int rpos = start; rpos < end; ++rpos) {            final boolean nextValue = valueChunk.get(rpos);            if (nextValue) {                trueValues++;            }            else {                falseValues++;            }        }
        if (trueValues > 0) {
            valueChunk.set(++wpos, true);
            counts.set(wpos, trueValues);
        }
        if (falseValues > 0) {
            valueChunk.set(++wpos, false);
            counts.set(wpos, falseValues);
        }
        // endregion compactAndCount
        return wpos + 1;
    }

    private static boolean shouldIgnore(boolean value) {
         // region shouldIgnore
        return false;
        // endregion shouldIgnore
    }
}
