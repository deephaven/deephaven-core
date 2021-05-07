/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharStampKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.join.stamp;

import io.deephaven.db.util.DhFloatComparisons;

import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.FloatChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.Index;


public class FloatReverseStampKernel implements StampKernel {
    static final FloatReverseStampKernel INSTANCE = new FloatReverseStampKernel();
    private FloatReverseStampKernel() {} // static use only

    @Override
    public void computeRedirections(Chunk<Values> leftStamps, Chunk<Values> rightStamps, LongChunk<KeyIndices> rightKeyIndices, WritableLongChunk<KeyIndices> leftRedirections) {
        computeRedirections(leftStamps.asFloatChunk(), rightStamps.asFloatChunk(), rightKeyIndices, leftRedirections);
    }

    static private void computeRedirections(FloatChunk<Values> leftStamps, FloatChunk<Values> rightStamps, LongChunk<KeyIndices> rightKeyIndices, WritableLongChunk<KeyIndices> leftRedirections) {
        final int leftSize = leftStamps.size();
        final int rightSize = rightStamps.size();
        if (rightSize == 0) {
            leftRedirections.fillWithValue(0, leftSize, Index.NULL_KEY);
            leftRedirections.setSize(leftSize);
            return;
        }

        int rightLowIdx = 0;
        float rightLowValue = rightStamps.get(0);

        final int maxRightIdx = rightSize - 1;

        for (int li = 0; li < leftSize; ) {
            final float leftValue = leftStamps.get(li);
            if (lt(leftValue, rightLowValue)) {
                leftRedirections.set(li++, Index.NULL_KEY);
                continue;
            }
            else if (eq(leftValue, rightLowValue)) {
                leftRedirections.set(li++, rightKeyIndices.get(rightLowIdx));
                continue;
            }

            int rightHighIdx = rightSize;

            while (rightLowIdx < rightHighIdx) {
                final int rightMidIdx = ((rightHighIdx - rightLowIdx) / 2) + rightLowIdx;
                final float rightMidValue = rightStamps.get(rightMidIdx);
                if (leq(rightMidValue, leftValue)) {
                    rightLowIdx = rightMidIdx;
                    rightLowValue = rightMidValue;
                    if (rightLowIdx == rightHighIdx - 1 || eq(rightLowValue, leftValue)) {
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
                final float nextRightValue = rightStamps.get(rightLowIdx + 1);
                while (li < leftSize && lt(leftStamps.get(li), nextRightValue)) {
                    leftRedirections.set(li++, redirectionKey);
                }
            }
        }
    }
    // region comparison functions
    private static int doComparison(float lhs, float rhs) {
        return -1 * DhFloatComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean lt(float lhs, float rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(float lhs, float rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(float lhs, float rhs) {
        // region equality function
        return DhFloatComparisons.eq(lhs, rhs);
        // endregion equality function
    }
}
