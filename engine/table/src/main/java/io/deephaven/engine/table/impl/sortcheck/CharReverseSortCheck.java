//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharReverseSortCheck and run "./gradlew replicateSortCheck" to regenerate
//
// @formatter:off

package io.deephaven.engine.table.impl.sortcheck;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;

public class CharReverseSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new CharReverseSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asCharChunk());
    }

    private int sortCheck(CharChunk<? extends Values> valuesToCheck) {
        if (valuesToCheck.size() == 0) {
            return -1;
        }
        char last = valuesToCheck.get(0);
        for (int ii = 1; ii < valuesToCheck.size(); ++ii) {
            final char current = valuesToCheck.get(ii);
            if (!leq(last, current)) {
                return ii - 1;
            }
            last = current;
        }
        return -1;
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static boolean leq(char lhs, char rhs) {
        return CharComparisons.geq(lhs, rhs);
    }
    // endregion comparison functions
}
