/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharNoExactStampKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.join.stamp;

import io.deephaven.engine.chunk.*;
import io.deephaven.engine.chunk.Attributes.RowKeys;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.v2.utils.RowSet;


public class ShortNoExactStampKernel implements StampKernel {
    static final ShortNoExactStampKernel INSTANCE = new ShortNoExactStampKernel();
    private ShortNoExactStampKernel() {} // static use only

    @Override
    public void computeRedirections(Chunk<Values> leftStamps, Chunk<Values> rightStamps, LongChunk<Attributes.RowKeys> rightKeyIndices, WritableLongChunk<Attributes.RowKeys> leftRedirections) {
        computeRedirections(leftStamps.asShortChunk(), rightStamps.asShortChunk(), rightKeyIndices, leftRedirections);
    }

    static private void computeRedirections(ShortChunk<Values> leftStamps, ShortChunk<Values> rightStamps, LongChunk<RowKeys> rightKeyIndices, WritableLongChunk<Attributes.RowKeys> leftRedirections) {
        final int leftSize = leftStamps.size();
        final int rightSize = rightStamps.size();
        if (rightSize == 0) {
            leftRedirections.fillWithValue(0, leftSize, RowSet.NULL_ROW_KEY);
            leftRedirections.setSize(leftSize);
            return;
        }

        int rightLowIdx = 0;
        short rightLowValue = rightStamps.get(0);

        final int maxRightIdx = rightSize - 1;

        for (int li = 0; li < leftSize; ) {
            final short leftValue = leftStamps.get(li);
            if (leq(leftValue, rightLowValue)) {
                leftRedirections.set(li++, RowSet.NULL_ROW_KEY);
                continue;
            }

            int rightHighIdx = rightSize;

            while (rightLowIdx < rightHighIdx) {
                final int rightMidIdx = ((rightHighIdx - rightLowIdx) / 2) + rightLowIdx;
                final short rightMidValue = rightStamps.get(rightMidIdx);
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
                final short nextRightValue = rightStamps.get(rightLowIdx + 1);
                while (li < leftSize && lt(leftStamps.get(li), nextRightValue)) {
                    leftRedirections.set(li++, redirectionKey);
                }
            }
        }
    }
    // region comparison functions
    private static int doComparison(short lhs, short rhs) {
        return Short.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean lt(short lhs, short rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(short lhs, short rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(short lhs, short rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
}
