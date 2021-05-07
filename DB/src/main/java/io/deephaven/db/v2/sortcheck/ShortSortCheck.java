/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSortCheck and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sortcheck;

import io.deephaven.db.util.DhShortComparisons;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ShortChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;

public class ShortSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new ShortSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Attributes.Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asShortChunk());
    }

    private int sortCheck(ShortChunk<? extends Attributes.Values> valuesToCheck) {
        if (valuesToCheck.size() == 0) {
            return -1;
        }
        short last = valuesToCheck.get(0);
        for (int ii = 1; ii < valuesToCheck.size(); ++ii) {
            final short current = valuesToCheck.get(ii);
            if (!leq(last, current)) {
                return ii - 1;
            }
            last = current;
        }
        return -1;
    }

    // region comparison functions
    private static int doComparison(short lhs, short rhs) {
        return DhShortComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(short lhs, short rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}
