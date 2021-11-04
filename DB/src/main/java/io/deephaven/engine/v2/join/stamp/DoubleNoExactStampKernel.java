/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharNoExactStampKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.join.stamp;

import io.deephaven.engine.util.DhDoubleComparisons;

import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.utils.RowSet;


public class DoubleNoExactStampKernel implements StampKernel {
    static final DoubleNoExactStampKernel INSTANCE = new DoubleNoExactStampKernel();
    private DoubleNoExactStampKernel() {} // static use only

    @Override
    public void computeRedirections(Chunk<Values> leftStamps, Chunk<Values> rightStamps, LongChunk<Attributes.RowKeys> rightKeyIndices, WritableLongChunk<Attributes.RowKeys> leftRedirections) {
        computeRedirections(leftStamps.asDoubleChunk(), rightStamps.asDoubleChunk(), rightKeyIndices, leftRedirections);
    }

    static private void computeRedirections(DoubleChunk<Values> leftStamps, DoubleChunk<Values> rightStamps, LongChunk<RowKeys> rightKeyIndices, WritableLongChunk<Attributes.RowKeys> leftRedirections) {
        final int leftSize = leftStamps.size();
        final int rightSize = rightStamps.size();
        if (rightSize == 0) {
            leftRedirections.fillWithValue(0, leftSize, RowSet.NULL_ROW_KEY);
            leftRedirections.setSize(leftSize);
            return;
        }

        int rightLowIdx = 0;
        double rightLowValue = rightStamps.get(0);

        final int maxRightIdx = rightSize - 1;

        for (int li = 0; li < leftSize; ) {
            final double leftValue = leftStamps.get(li);
            if (leq(leftValue, rightLowValue)) {
                leftRedirections.set(li++, RowSet.NULL_ROW_KEY);
                continue;
            }

            int rightHighIdx = rightSize;

            while (rightLowIdx < rightHighIdx) {
                final int rightMidIdx = ((rightHighIdx - rightLowIdx) / 2) + rightLowIdx;
                final double rightMidValue = rightStamps.get(rightMidIdx);
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
                final double nextRightValue = rightStamps.get(rightLowIdx + 1);
                while (li < leftSize && lt(leftStamps.get(li), nextRightValue)) {
                    leftRedirections.set(li++, redirectionKey);
                }
            }
        }
    }
    // region comparison functions
    private static int doComparison(double lhs, double rhs) {
        return DhDoubleComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean lt(double lhs, double rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(double lhs, double rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(double lhs, double rhs) {
        // region equality function
        return DhDoubleComparisons.eq(lhs, rhs);
        // endregion equality function
    }
}
