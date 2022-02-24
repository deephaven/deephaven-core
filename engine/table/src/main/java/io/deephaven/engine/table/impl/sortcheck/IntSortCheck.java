/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortCheck and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sortcheck;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.IntComparisons;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;

public class IntSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new IntSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asIntChunk());
    }

    private int sortCheck(IntChunk<? extends Values> valuesToCheck) {
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
    private static int doComparison(int lhs, int rhs) {
        return IntComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(int lhs, int rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}
