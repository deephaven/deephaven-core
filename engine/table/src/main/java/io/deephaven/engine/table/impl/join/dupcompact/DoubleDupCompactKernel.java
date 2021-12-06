/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharDupCompactKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.join.dupcompact;

import io.deephaven.util.compare.DoubleComparisons;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

public class DoubleDupCompactKernel implements DupCompactKernel {
    static final DoubleDupCompactKernel INSTANCE = new DoubleDupCompactKernel();

    private DoubleDupCompactKernel() {} // use through the instance

    @Override
    public int compactDuplicates(WritableChunk<? extends Any> chunkToCompact, WritableLongChunk<RowKeys> keyIndices) {
        return compactDuplicates(chunkToCompact.asWritableDoubleChunk(), keyIndices);
    }

    private static int compactDuplicates(WritableDoubleChunk<? extends Any> chunkToCompact, WritableLongChunk<RowKeys> keyIndices) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        double last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final double current = chunkToCompact.get(rpos);
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
    private static int doComparison(double lhs, double rhs) {
        return DoubleComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(double lhs, double rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(double lhs, double rhs) {
        // region equality function
        return DoubleComparisons.eq(lhs, rhs);
        // endregion equality function
    }
}
