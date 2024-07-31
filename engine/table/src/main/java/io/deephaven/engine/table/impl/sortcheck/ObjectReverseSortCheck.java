//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ObjectReverseSortCheck and run "./gradlew replicateSortCheck" to regenerate
//
// @formatter:off

package io.deephaven.engine.table.impl.sortcheck;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.Chunk;

public class ObjectReverseSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new ObjectReverseSortCheck();

    @Override
    public int sortCheck(Chunk<? extends Values> valuesToCheck) {
        return sortCheck(valuesToCheck.asObjectChunk());
    }

    private int sortCheck(ObjectChunk<Object, ? extends Values> valuesToCheck) {
        if (valuesToCheck.size() == 0) {
            return -1;
        }
        Object last = valuesToCheck.get(0);
        for (int ii = 1; ii < valuesToCheck.size(); ++ii) {
            final Object current = valuesToCheck.get(ii);
            if (!leq(last, current)) {
                return ii - 1;
            }
            last = current;
        }
        return -1;
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static boolean leq(Object lhs, Object rhs) {
        return ObjectComparisons.geq(lhs, rhs);
    }
    // endregion comparison functions
}
