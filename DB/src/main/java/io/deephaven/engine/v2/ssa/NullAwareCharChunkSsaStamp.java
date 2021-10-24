/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkSsaStamp and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.ssa;

import io.deephaven.engine.util.DhCharComparisons;

import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.utils.RowSetBuilder;
import io.deephaven.engine.v2.utils.TrackingMutableRowSet;
import io.deephaven.engine.v2.utils.RedirectionIndex;

/**
 * Stamp kernel for when the left hand side is a sorted chunk and the right hand side is a ticking SegmentedSortedArray.
 */
public class NullAwareCharChunkSsaStamp implements ChunkSsaStamp {
    static NullAwareCharChunkSsaStamp INSTANCE = new NullAwareCharChunkSsaStamp();

    private NullAwareCharChunkSsaStamp() {} // use the instance

    @Override
    public void processEntry(Chunk<Values> leftStampValues, Chunk<RowKeys> leftStampKeys, SegmentedSortedArray ssa, WritableLongChunk<RowKeys> rightKeysForLeft, boolean disallowExactMatch) {
        processEntry(leftStampValues.asCharChunk(), leftStampKeys, (NullAwareCharSegmentedSortedArray)ssa, rightKeysForLeft, disallowExactMatch);
    }

    private static void processEntry(CharChunk<Values> leftStampValues, Chunk<RowKeys> leftStampKeys, NullAwareCharSegmentedSortedArray ssa, WritableLongChunk<Attributes.RowKeys> rightKeysForLeft, boolean disallowExactMatch) {
        final int leftSize = leftStampKeys.size();
        final long rightSize = ssa.size();
        if (rightSize == 0) {
            rightKeysForLeft.fillWithValue(0, leftSize, TrackingMutableRowSet.NULL_ROW_KEY);
            rightKeysForLeft.setSize(leftSize);
            return;
        }

        final NullAwareCharSegmentedSortedArray.Iterator ssaIt = ssa.iterator(disallowExactMatch, true);

        for (int li = 0; li < leftSize; ) {
            final char leftValue = leftStampValues.get(li);
            final int comparison = doComparison(leftValue, ssaIt.getValue());
            if (disallowExactMatch ? comparison <= 0 : comparison < 0) {
                rightKeysForLeft.set(li++, TrackingMutableRowSet.NULL_ROW_KEY);
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
                final char nextRightValue = ssaIt.nextValue();
                while (li < leftSize && (disallowExactMatch ? leq(leftStampValues.get(li), nextRightValue) :  lt(leftStampValues.get(li), nextRightValue))) {
                    rightKeysForLeft.set(li++, redirectionKey);
                }
            }
        }
    }

    @Override
    public void processRemovals(Chunk<Values> leftStampValues, LongChunk<Attributes.RowKeys> leftStampKeys, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, WritableLongChunk<RowKeys> priorRedirections, RedirectionIndex redirectionIndex, RowSetBuilder modifiedBuilder, boolean disallowExactMatch) {
        processRemovals(leftStampValues.asCharChunk(), leftStampKeys, rightStampChunk.asCharChunk(), rightKeys, priorRedirections, redirectionIndex, modifiedBuilder, disallowExactMatch);
    }

    private static void processRemovals(CharChunk<Values> leftStampValues, LongChunk<Attributes.RowKeys> leftStampKeys, CharChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, WritableLongChunk<RowKeys> nextRedirections, RedirectionIndex redirectionIndex, RowSetBuilder modifiedBuilder, boolean disallowExactMatch) {
        // When removing a row, record the stamp, redirection key, and prior redirection key.  Binary search
        // in the left for the removed key to find the smallest value geq the removed right.  Update all rows
        // with the removed redirection to the previous key.

        int leftLowIdx = 0;

        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final char rightStampValue = rightStampChunk.get(ii);
            final long rightStampKey = rightKeys.get(ii);
            final long newRightStampKey = nextRedirections.get(ii);

            leftLowIdx = findFirstResponsiveLeft(leftLowIdx, leftStampValues, disallowExactMatch, rightStampValue);

            while (leftLowIdx < leftStampKeys.size()) {
                final long leftKey = leftStampKeys.get(leftLowIdx);
                final long leftRedirectionKey = redirectionIndex.get(leftKey);
                if (leftRedirectionKey == rightStampKey) {
                    modifiedBuilder.addKey(leftKey);
                    if (newRightStampKey == TrackingMutableRowSet.NULL_ROW_KEY) {
                        redirectionIndex.removeVoid(leftKey);
                    } else {
                        redirectionIndex.putVoid(leftKey, newRightStampKey);
                    }
                    leftLowIdx++;
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void processInsertion(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, Chunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys, Chunk<Values> nextRightValue, RedirectionIndex redirectionIndex, RowSetBuilder modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        processInsertion(leftStampValues.asCharChunk(), leftStampKeys, rightStampChunk.asCharChunk(), rightKeys, nextRightValue.asCharChunk(), redirectionIndex, modifiedBuilder, endsWithLastValue, disallowExactMatch);
    }

    private static void processInsertion(CharChunk<Values> leftStampValues, LongChunk<Attributes.RowKeys> leftStampKeys, CharChunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys, CharChunk<Values> nextRightValue, RedirectionIndex redirectionIndex, RowSetBuilder modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        // We've already filtered out duplicate right stamps by the time we get here, which means that the rightStampChunk
        // contains only values that are the last in any given run; and thus are possible matches.

        // We binary search in the left for the first value >=, everything up until the next extant right value (contained
        // in the nextRightValue chunk) should be re-stamped with our value

        int leftLowIdx = 0;

        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final char rightStampValue = rightStampChunk.get(ii);

            leftLowIdx = findFirstResponsiveLeft(leftLowIdx, leftStampValues, disallowExactMatch, rightStampValue);

            final long rightStampKey = rightKeys.get(ii);

            if (ii == rightStampChunk.size() - 1 && endsWithLastValue) {
                while (leftLowIdx < leftStampKeys.size()) {
                    final long leftKey = leftStampKeys.get(leftLowIdx);
                    redirectionIndex.putVoid(leftKey, rightStampKey);
                    modifiedBuilder.addKey(leftKey);
                    leftLowIdx++;
                }
            } else {
                final char nextRight = nextRightValue.get(ii);
                while (leftLowIdx < leftStampKeys.size()) {
                    final char leftValue = leftStampValues.get(leftLowIdx);
                    if (disallowExactMatch ? leq(leftValue, nextRight) : lt(leftValue, nextRight)) {
                        final long leftKey = leftStampKeys.get(leftLowIdx);
                        redirectionIndex.putVoid(leftKey, rightStampKey);
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
    public int findModified(int first, Chunk<Values> leftStampValues, LongChunk<Attributes.RowKeys> leftStampKeys, RedirectionIndex redirectionIndex, Chunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightStampIndices, RowSetBuilder modifiedBuilder, boolean disallowExactMatch) {
        return findModified(first, leftStampValues.asCharChunk(), leftStampKeys, redirectionIndex, rightStampChunk.asCharChunk(), rightStampIndices, modifiedBuilder, disallowExactMatch);
    }

    private static int findModified(int leftLowIdx, CharChunk<Values> leftStampValues, LongChunk<Attributes.RowKeys> leftStampKeys, RedirectionIndex redirectionIndex, CharChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampIndices, RowSetBuilder modifiedBuilder, boolean disallowExactMatch) {
        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final char rightStampValue = rightStampChunk.get(ii);

            // now find the lowest left value leq (lt) than rightStampValue
            leftLowIdx = findFirstResponsiveLeft(leftLowIdx, leftStampValues, disallowExactMatch, rightStampValue);

            final long rightStampKey = rightStampIndices.get(ii);
            int checkIdx = leftLowIdx;
            while (checkIdx < leftStampValues.size() && redirectionIndex.get(leftStampKeys.get(checkIdx)) == rightStampKey) {
                modifiedBuilder.addKey(leftStampKeys.get(checkIdx));
                checkIdx++;
            }
        }

        return leftLowIdx;
    }

    @Override
    public void applyShift(Chunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        applyShift(leftStampValues.asCharChunk(), leftStampKeys, rightStampChunk.asCharChunk(), rightStampKeys, shiftDelta, redirectionIndex, disallowExactMatch);
    }

    private void applyShift(CharChunk<Values> leftStampValues, LongChunk<RowKeys> leftStampKeys, CharChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        int leftLowIdx = 0;
        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final char rightStampValue = rightStampChunk.get(ii);

            // now find the lowest left value leq (lt) than rightStampValue
            leftLowIdx = findFirstResponsiveLeft(leftLowIdx, leftStampValues, disallowExactMatch, rightStampValue);

            final long rightStampKey = rightStampKeys.get(ii);
            int checkIdx = leftLowIdx;
            while (checkIdx < leftStampValues.size() && redirectionIndex.get(leftStampKeys.get(checkIdx)) == rightStampKey) {
                redirectionIndex.putVoid(leftStampKeys.get(checkIdx), rightStampKey + shiftDelta);
                checkIdx++;
            }
        }
    }

    private static int findFirstResponsiveLeft(int leftLowIdx, CharChunk<Values> leftStampValues, boolean disallowExactMatch, char rightStampValue) {
        int leftHighIdx = leftStampValues.size();

        while (leftLowIdx < leftHighIdx) {
            final int leftMidIdx = (leftHighIdx + leftLowIdx) >>> 1;
            final char leftMidValue = leftStampValues.get(leftMidIdx);

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
    private static int doComparison(char lhs, char rhs) {
        return DhCharComparisons.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean lt(char lhs, char rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(char lhs, char rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}

