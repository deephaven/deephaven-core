/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortCheck and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sortcheck;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;

public class FloatReverseSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new FloatReverseSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asFloatChunk());
    }

    private int sortCheck(FloatChunk<? extends Values> valuesToCheck) {
        if (valuesToCheck.size() == 0) {
            return -1;
        }
        float last = valuesToCheck.get(0);
        for (int ii = 1; ii < valuesToCheck.size(); ++ii) {
            final float current = valuesToCheck.get(ii);
            if (!leq(last, current)) {
                return ii - 1;
            }
            last = current;
        }
        return -1;
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static int doComparison(float lhs, float rhs) {
        return -1 * FloatComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(float lhs, float rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}
