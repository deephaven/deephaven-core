/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortCheck and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sortcheck;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.ByteComparisons;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;

public class ByteReverseSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new ByteReverseSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asByteChunk());
    }

    private int sortCheck(ByteChunk<? extends Values> valuesToCheck) {
        if (valuesToCheck.size() == 0) {
            return -1;
        }
        byte last = valuesToCheck.get(0);
        for (int ii = 1; ii < valuesToCheck.size(); ++ii) {
            final byte current = valuesToCheck.get(ii);
            if (!leq(last, current)) {
                return ii - 1;
            }
            last = current;
        }
        return -1;
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static int doComparison(byte lhs, byte rhs) {
        return -1 * ByteComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(byte lhs, byte rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}
