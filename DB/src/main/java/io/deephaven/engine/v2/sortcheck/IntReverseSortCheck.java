/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortCheck and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sortcheck;

import io.deephaven.util.compare.IntComparisons;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.IntChunk;
import io.deephaven.engine.chunk.Chunk;

public class IntReverseSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new IntReverseSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Attributes.Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asIntChunk());
    }

    private int sortCheck(IntChunk<? extends Attributes.Values> valuesToCheck) {
        if (valuesToCheck.size() == 0) {
            return -1;
        }
        int last = valuesToCheck.get(0);
        for (int ii = 1; ii < valuesToCheck.size(); ++ii) {
            final int current = valuesToCheck.get(ii);
            if (!leq(last, current)) {
                return ii - 1;
            }
            last = current;
        }
        return -1;
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static int doComparison(int lhs, int rhs) {
        return -1 * IntComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(int lhs, int rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}
