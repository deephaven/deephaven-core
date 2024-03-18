//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharStampKernel and run "./gradlew replicateStampKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.join.stamp;

import java.util.Objects;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;


public class ObjectStampKernel implements StampKernel {
    static final ObjectStampKernel INSTANCE = new ObjectStampKernel();

    private ObjectStampKernel() {} // static use only

    @Override
    public void computeRedirections(Chunk<Values> leftStamps, Chunk<Values> rightStamps,
            LongChunk<RowKeys> rightKeyIndices, WritableLongChunk<RowKeys> leftRedirections) {
        computeRedirections(leftStamps.asObjectChunk(), rightStamps.asObjectChunk(), rightKeyIndices, leftRedirections);
    }

    static private void computeRedirections(ObjectChunk<Object, Values> leftStamps, ObjectChunk<Object, Values> rightStamps,
            LongChunk<RowKeys> rightKeyIndices, WritableLongChunk<RowKeys> leftRedirections) {
        final int leftSize = leftStamps.size();
        final int rightSize = rightStamps.size();
        if (rightSize == 0) {
            leftRedirections.fillWithValue(0, leftSize, RowSequence.NULL_ROW_KEY);
            leftRedirections.setSize(leftSize);
            return;
        }

        int rightLowIdx = 0;
        Object rightLowValue = rightStamps.get(0);

        final int maxRightIdx = rightSize - 1;

        for (int li = 0; li < leftSize;) {
            final Object leftValue = leftStamps.get(li);
            if (lt(leftValue, rightLowValue)) {
                leftRedirections.set(li++, RowSequence.NULL_ROW_KEY);
                continue;
            } else if (eq(leftValue, rightLowValue)) {
                leftRedirections.set(li++, rightKeyIndices.get(rightLowIdx));
                continue;
            }

            int rightHighIdx = rightSize;

            while (rightLowIdx < rightHighIdx) {
                final int rightMidIdx = ((rightHighIdx - rightLowIdx) / 2) + rightLowIdx;
                final Object rightMidValue = rightStamps.get(rightMidIdx);
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
                final Object nextRightValue = rightStamps.get(rightLowIdx + 1);
                while (li < leftSize && lt(leftStamps.get(li), nextRightValue)) {
                    leftRedirections.set(li++, redirectionKey);
                }
            }
        }
    }

    // region comparison functions
    // ascending comparison
    private static int doComparison(Object lhs, Object rhs) {
       if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return -1;
        }
        if (rhs == null) {
            return 1;
        }
        //noinspection unchecked,rawtypes
        return ((Comparable)lhs).compareTo(rhs);
    }

    // endregion comparison functions

    private static boolean lt(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) <= 0;
    }

    private static boolean eq(Object lhs, Object rhs) {
        // region equality function
        return Objects.equals(lhs, rhs);
        // endregion equality function
    }
}
