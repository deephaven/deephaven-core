/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkSsaStamp and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.ssa;

import java.util.Objects;

import java.util.Objects;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;

/**
 * Stamp kernel for when the left hand side is a sorted chunk and the right hand side is a ticking SegmentedSortedArray.
 */
public class ObjectReverseChunkSsaStamp implements ChunkSsaStamp {
    static ObjectReverseChunkSsaStamp INSTANCE = new ObjectReverseChunkSsaStamp();

    private ObjectReverseChunkSsaStamp() {} // use the instance

    @Override
    public void processEntry(Chunk<Values> leftStampValues, Chunk<RowKeys> leftStampKeys, SegmentedSortedArray ssa, WritableLongChunk<RowKeys> rightKeysForLeft, boolean disallowExactMatch) {
        processEntry(leftStampValues.asObjectChunk(), leftStampKeys, (ObjectReverseSegmentedSortedArray)ssa, rightKeysForLeft, disallowExactMatch);
    }

    private static void processEntry(ObjectChunk<Object, Values> leftStampValues, Chunk<RowKeys> leftStampKeys, ObjectReverseSegmentedSortedArray ssa, WritableLongChunk<RowKeys> rightKeysForLeft, boolean disallowExactMatch) {
        final int leftSize = leftStampKeys.size();
        final long rightSize = ssa.size();
        if (rightSize == 0) {
            rightKeysForLeft.fillWithValue(0, leftSize, RowSequence.NULL_ROW_KEY);
            rightKeysForLeft.setSize(leftSize);
            return;
        }

        final ObjectReverseSegmentedSortedArray.Iterator ssaIt = ssa.iterator(disallowExactMatch, true);

        for (int li = 0; li < leftSize; ) {
            final Object leftValue = leftStampValues.get(li);
            final int comparison = doComparison(leftValue, ssaIt.getValue());
            if (disallowExactMatch ? comparison <= 0 : comparison < 0) {
                rightKeysForLeft.set(li++, RowSequence.NULL_ROW_KEY);
                continue;
            }
            else if (comparison == 0) {
                rightKeysForLeft.set(li++, ssaIt.getKey());
                continue;
            }

            ssaIt.advanceToLast(leftValue);

            final long redirectionKey = ssaIt.getKey();
            if (!ssaIt.hasNext()) {
                rightKeysForLeft.fillWithValue(li, leftSize - li, redirectionKey);
                return;
            } else {
                rightKeysForLeft.set(li++, redirectionKey);
                final Object nextRightValue = ssaIt.nextValue();
                while (li < leftSize && (disallowExactMatch ? leq(leftStampValues.get(li), nextRightValue) :  lt(leftStampValues.get(li), nextRightValue))) {
                    rightKeysForLeft.set(li++, redirectionKey);
                }
            }
        }
    }

    @Override
    public void processRemovals(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, WritableLongChunk<RowKeys> priorRedirections, WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        processRemovals(leftStampValues.asObjectChunk(), leftStampKeys, rightStampChunk.asObjectChunk(), rightKeys, priorRedirections, rowRedirection, modifiedBuilder, disallowExactMatch);
    }

    private static void processRemovals(ObjectChunk<Object, Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, ObjectChunk<Object, ? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, WritableLongChunk<RowKeys> nextRedirections, WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        // When removing a row, record the stamp, redirection key, and prior redirection key.  Binary search
        // in the left for the removed key to find the smallest value geq the removed right.  Update all rows
        // with the removed redirection to the previous key.

        int leftLowIdx = 0;

        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final Object rightStampValue = rightStampChunk.get(ii);
            final long rightStampKey = rightKeys.get(ii);
            final long newRightStampKey = nextRedirections.get(ii);

            leftLowIdx = findFirstResponsiveLeft(leftLowIdx, leftStampValues, disallowExactMatch, rightStampValue);

            while (leftLowIdx < leftStampKeys.size()) {
                final long leftKey = leftStampKeys.get(leftLowIdx);
                final long leftRedirectionKey = rowRedirection.get(leftKey);
                if (leftRedirectionKey == rightStampKey) {
                    modifiedBuilder.addKey(leftKey);
                    if (newRightStampKey == RowSequence.NULL_ROW_KEY) {
                        rowRedirection.removeVoid(leftKey);
                    } else {
                        rowRedirection.putVoid(leftKey, newRightStampKey);
                    }
                    leftLowIdx++;
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void processInsertion(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, Chunk<Values> nextRightValue, WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        processInsertion(leftStampValues.asObjectChunk(), leftStampKeys, rightStampChunk.asObjectChunk(), rightKeys, nextRightValue.asObjectChunk(), rowRedirection, modifiedBuilder, endsWithLastValue, disallowExactMatch);
    }

    private static void processInsertion(ObjectChunk<Object, Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, ObjectChunk<Object, ? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, ObjectChunk<Object, Values> nextRightValue, WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        // We've already filtered out duplicate right stamps by the time we get here, which means that the rightStampChunk
        // contains only values that are the last in any given run; and thus are possible matches.

        // We binary search in the left for the first value >=, everything up until the next extant right value (contained
        // in the nextRightValue chunk) should be re-stamped with our value

        int leftLowIdx = 0;

        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final Object rightStampValue = rightStampChunk.get(ii);

            leftLowIdx = findFirstResponsiveLeft(leftLowIdx, leftStampValues, disallowExactMatch, rightStampValue);

            final long rightStampKey = rightKeys.get(ii);

            if (ii == rightStampChunk.size() - 1 && endsWithLastValue) {
                while (leftLowIdx < leftStampKeys.size()) {
                    final long leftKey = leftStampKeys.get(leftLowIdx);
                    rowRedirection.putVoid(leftKey, rightStampKey);
                    modifiedBuilder.addKey(leftKey);
                    leftLowIdx++;
                }
            } else {
                final Object nextRight = nextRightValue.get(ii);
                while (leftLowIdx < leftStampKeys.size()) {
                    final Object leftValue = leftStampValues.get(leftLowIdx);
                    if (disallowExactMatch ? leq(leftValue, nextRight) : lt(leftValue, nextRight)) {
                        final long leftKey = leftStampKeys.get(leftLowIdx);
                        rowRedirection.putVoid(leftKey, rightStampKey);
                        modifiedBuilder.addKey(leftKey);
                        leftLowIdx++;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public int findModified(int first, Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, RowRedirection rowRedirection, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        return findModified(first, leftStampValues.asObjectChunk(), leftStampKeys, rowRedirection, rightStampChunk.asObjectChunk(), rightStampIndices, modifiedBuilder, disallowExactMatch);
    }

    private static int findModified(int leftLowIdx, ObjectChunk<Object, Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, RowRedirection rowRedirection, ObjectChunk<Object, ? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final Object rightStampValue = rightStampChunk.get(ii);

            // now find the lowest left value leq (lt) than rightStampValue
            leftLowIdx = findFirstResponsiveLeft(leftLowIdx, leftStampValues, disallowExactMatch, rightStampValue);

            final long rightStampKey = rightStampIndices.get(ii);
            int checkIdx = leftLowIdx;
            while (checkIdx < leftStampValues.size() && rowRedirection.get(leftStampKeys.get(checkIdx)) == rightStampKey) {
                modifiedBuilder.addKey(leftStampKeys.get(checkIdx));
                checkIdx++;
            }
        }

        return leftLowIdx;
    }

    @Override
    public void applyShift(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta, WritableRowRedirection rowRedirection, boolean disallowExactMatch) {
        applyShift(leftStampValues.asObjectChunk(), leftStampKeys, rightStampChunk.asObjectChunk(), rightStampKeys, shiftDelta, rowRedirection, disallowExactMatch);
    }

    private void applyShift(ObjectChunk<Object, Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, ObjectChunk<Object, ? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta, WritableRowRedirection rowRedirection, boolean disallowExactMatch) {
        int leftLowIdx = 0;
        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final Object rightStampValue = rightStampChunk.get(ii);

            // now find the lowest left value leq (lt) than rightStampValue
            leftLowIdx = findFirstResponsiveLeft(leftLowIdx, leftStampValues, disallowExactMatch, rightStampValue);

            final long rightStampKey = rightStampKeys.get(ii);
            int checkIdx = leftLowIdx;
            while (checkIdx < leftStampValues.size() && rowRedirection.get(leftStampKeys.get(checkIdx)) == rightStampKey) {
                rowRedirection.putVoid(leftStampKeys.get(checkIdx), rightStampKey + shiftDelta);
                checkIdx++;
            }
        }
    }

    private static int findFirstResponsiveLeft(int leftLowIdx, ObjectChunk<Object, Values> leftStampValues, boolean disallowExactMatch, Object rightStampValue) {
        int leftHighIdx = leftStampValues.size();

        while (leftLowIdx < leftHighIdx) {
            final int leftMidIdx = (leftHighIdx + leftLowIdx) >>> 1;
            final Object leftMidValue = leftStampValues.get(leftMidIdx);

            final int comparison = doComparison(leftMidValue, rightStampValue);
            final boolean moveLow = disallowExactMatch ? comparison <= 0 : comparison < 0;
            if (moveLow) {
                leftLowIdx = leftMidIdx + 1;
            } else {
                leftHighIdx = leftMidIdx;
            }
        }
        return leftLowIdx;
    }

    // region comparison functions
    // descending comparison
    private static int doComparison(Object lhs, Object rhs) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return 1;
        }
        if (rhs == null) {
            return -1;
        }
        //noinspection unchecked,rawtypes
        return ((Comparable)rhs).compareTo(lhs);
    }
    // endregion comparison functions

    private static boolean lt(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(Object lhs, Object rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}

