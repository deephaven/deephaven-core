/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharDupCompactKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.join.dupcompact;

import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

public class NullAwareCharDupCompactKernel implements DupCompactKernel {
    static final NullAwareCharDupCompactKernel INSTANCE = new NullAwareCharDupCompactKernel();

    private NullAwareCharDupCompactKernel() {} // use through the instance

    @Override
    public int compactDuplicates(WritableChunk<? extends Any> chunkToCompact, WritableLongChunk<RowKeys> keyIndices) {
        return compactDuplicates(chunkToCompact.asWritableCharChunk(), keyIndices);
    }

    private static int compactDuplicates(WritableCharChunk<? extends Any> chunkToCompact, WritableLongChunk<RowKeys> keyIndices) {
        final int inputSize = chunkToCompact.size();
        if (inputSize == 0) {
            return -1;
        }

        int wpos = 0;
        int rpos = 0;

        char last = chunkToCompact.get(0);

        while (rpos < inputSize) {
            final char current = chunkToCompact.get(rpos);
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
    private static int doComparison(char lhs, char rhs) {
        return CharComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(char lhs, char rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(char lhs, char rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
}
