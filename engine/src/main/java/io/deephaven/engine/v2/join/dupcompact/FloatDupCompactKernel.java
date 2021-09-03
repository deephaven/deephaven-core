/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharDupCompactKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.join.dupcompact;

import io.deephaven.engine.util.DhFloatComparisons;

import io.deephaven.engine.structures.chunk.*;
import io.deephaven.engine.structures.chunk.Attributes.Any;

public class FloatDupCompactKernel implements DupCompactKernel {
    static final FloatDupCompactKernel INSTANCE = new FloatDupCompactKernel();

    private FloatDupCompactKernel() {} // use through the instance

    @Override
    public int compactDuplicates(WritableChunk<? extends Any> chunkToCompact, WritableLongChunk<Attributes.KeyIndices> keyIndices) {
        return compactDuplicates(chunkToCompact.asWritableFloatChunk(), keyIndices);
    }

    private static int compactDuplicates(WritableFloatChunk<? extends Any> chunkToCompact, WritableLongChunk<Attributes.KeyIndices> keyIndices) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        float last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final float current = chunkToCompact.get(rpos);
            if (!leq(last, current)) {
                return rpos;
            }
            last = current;

            while (rpos < inputSize - 1 && eq(current, chunkToCompact.get(rpos + 1))) {
                rpos++;
            }
            chunkToCompact.set(wpos, current);
            keyIndices.set(wpos, keyIndices.get(rpos));
            rpos++;
            wpos++;
        }
        chunkToCompact.setSize(wpos);
        keyIndices.setSize(wpos);

        return -1;
    }

    // region comparison functions
    private static int doComparison(float lhs, float rhs) {
        return DhFloatComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(float lhs, float rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(float lhs, float rhs) {
        // region equality function
        return DhFloatComparisons.eq(lhs, rhs);
        // endregion equality function
    }
}
