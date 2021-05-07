/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortCheck and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sortcheck;

import io.deephaven.db.util.DhFloatComparisons;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.FloatChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;

public class FloatSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new FloatSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Attributes.Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asFloatChunk());
    }

    private int sortCheck(FloatChunk<? extends Attributes.Values> valuesToCheck) {
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
    private static int doComparison(float lhs, float rhs) {
        return DhFloatComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(float lhs, float rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}
