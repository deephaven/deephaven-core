//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LongNoExactReverseStampKernel and run "./gradlew replicateStampKernel" to regenerate
//
// @formatter:off

package io.deephaven.engine.table.impl.join.stamp;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;


public class LongNoExactReverseStampKernel implements StampKernel {
    static final LongNoExactReverseStampKernel INSTANCE = new LongNoExactReverseStampKernel();

    private LongNoExactReverseStampKernel() {} // static use only

    @Override
    public void computeRedirections(Chunk<Values> leftStamps, Chunk<Values> rightStamps,
            LongChunk<RowKeys> rightKeyIndices, WritableLongChunk<RowKeys> leftRedirections) {
        computeRedirections(leftStamps.asLongChunk(), rightStamps.asLongChunk(), rightKeyIndices, leftRedirections);
    }

    static private void computeRedirections(LongChunk<Values> leftStamps, LongChunk<Values> rightStamps,
            LongChunk<RowKeys> rightKeyIndices, WritableLongChunk<RowKeys> leftRedirections) {
        final int leftSize = leftStamps.size();
        final int rightSize = rightStamps.size();
        if (rightSize == 0) {
            leftRedirections.fillWithValue(0, leftSize, RowSequence.NULL_ROW_KEY);
            leftRedirections.setSize(leftSize);
            return;
        }

        int rightLowIdx = 0;
        long rightLowValue = rightStamps.get(0);

        final int maxRightIdx = rightSize - 1;

        for (int li = 0; li < leftSize;) {
            final long leftValue = leftStamps.get(li);
            if (leq(leftValue, rightLowValue)) {
                leftRedirections.set(li++, RowSequence.NULL_ROW_KEY);
                continue;
            }

            int rightHighIdx = rightSize;

            while (rightLowIdx < rightHighIdx) {
                final int rightMidIdx = ((rightHighIdx - rightLowIdx) / 2) + rightLowIdx;
                final long rightMidValue = rightStamps.get(rightMidIdx);
                if (lt(rightMidValue, leftValue)) {
                    rightLowIdx = rightMidIdx;
                    rightLowValue = rightMidValue;
                    if (rightLowIdx == rightHighIdx - 1) {
                        break;
                    }
                } else {
                    rightHighIdx = rightMidIdx;
                }
            }

            final long redirectionKey = rightKeyIndices.get(rightLowIdx);
            if (rightLowIdx == maxRightIdx) {
                leftRedirections.fillWithValue(li, leftSize - li, redirectionKey);
                return;
            } else {
                leftRedirections.set(li++, redirectionKey);
                final long nextRightValue = rightStamps.get(rightLowIdx + 1);
                while (li < leftSize && lt(leftStamps.get(li), nextRightValue)) {
                    leftRedirections.set(li++, redirectionKey);
                }
            }
        }
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static int doComparison(long lhs, long rhs) {
        return -1 * Long.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean lt(long lhs, long rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(long lhs, long rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(long lhs, long rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
}
