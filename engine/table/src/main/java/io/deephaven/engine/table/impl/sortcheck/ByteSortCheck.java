//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSortCheck and run "./gradlew replicateSortCheck" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sortcheck;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.ByteComparisons;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;

public class ByteSortCheck implements SortCheck {
    static final SortCheck INSTANCE = new ByteSortCheck();

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
    private static int doComparison(byte lhs, byte rhs) {
        return ByteComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean leq(byte lhs, byte rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}
