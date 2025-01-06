//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSortCheck and run "./gradlew replicateSortCheck" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sortcheck;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;

public class DoubleSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new DoubleSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asDoubleChunk());
    }

    private int sortCheck(DoubleChunk<? extends Values> valuesToCheck) {
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
    private static boolean leq(double lhs, double rhs) {
        return DoubleComparisons.leq(lhs, rhs);
    }
    // endregion comparison functions
}
