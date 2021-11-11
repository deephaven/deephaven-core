/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortCheck and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sortcheck;

import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.DoubleChunk;
import io.deephaven.engine.chunk.Chunk;

public class DoubleReverseSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new DoubleReverseSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Attributes.Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asDoubleChunk());
    }

    private int sortCheck(DoubleChunk<? extends Attributes.Values> valuesToCheck) {
        if (valuesToCheck.size() == 0) {
            return -1;
        }
        double last = valuesToCheck.get(0);
        for (int ii = 1; ii < valuesToCheck.size(); ++ii) {
            final double current = valuesToCheck.get(ii);
            if (!leq(last, current)) {
                return ii - 1;
            }
            last = current;
        }
        return -1;
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static int doComparison(double lhs, double rhs) {
        return -1 * DoubleComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(double lhs, double rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}
