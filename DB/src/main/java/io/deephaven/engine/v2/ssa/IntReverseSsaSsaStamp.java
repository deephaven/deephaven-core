/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsaSsaStamp and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.ssa;

import io.deephaven.engine.v2.sources.chunk.*;
import io.deephaven.engine.v2.sources.chunk.Attributes.RowKeys;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.sized.SizedLongChunk;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import io.deephaven.engine.v2.utils.RedirectionIndex;

/**
 * Stamp kernel for when the left hand side is a sorted chunk and the right hand side is a ticking SegmentedSortedArray.
 */
public class IntReverseSsaSsaStamp implements SsaSsaStamp {
    static IntReverseSsaSsaStamp INSTANCE = new IntReverseSsaSsaStamp();

    private IntReverseSsaSsaStamp() {} // use the instance

    @Override
    public void processEntry(SegmentedSortedArray leftSsa, SegmentedSortedArray rightSsa, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        processEntry((IntReverseSegmentedSortedArray)leftSsa, (IntReverseSegmentedSortedArray)rightSsa, redirectionIndex, disallowExactMatch);
    }

    private static void processEntry(IntReverseSegmentedSortedArray leftSsa, IntReverseSegmentedSortedArray rightSsa, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        final long rightSize = rightSsa.size();
        if (rightSize == 0) {
            fillWithNull(redirectionIndex, leftSsa.iterator(disallowExactMatch, false));
            return;
        }

        final IntReverseSegmentedSortedArray.Iterator rightIt = rightSsa.iterator(disallowExactMatch, true);
        final IntReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        while (leftIt.hasNext()) {
            leftIt.next();
            final int leftValue = leftIt.getValue();
            final int comparison = doComparison(leftValue, rightIt.getValue());
            if (disallowExactMatch ? comparison <= 0 : comparison < 0) {
                redirectionIndex.removeVoid(leftIt.getKey());
                continue;
            }
            else if (comparison == 0) {
                redirectionIndex.putVoid(leftIt.getKey(), rightIt.getKey());
                continue;
            }

            rightIt.advanceToLast(leftValue);

            final long redirectionKey = rightIt.getKey();
            if (!rightIt.hasNext()) {
                redirectionIndex.put(leftIt.getKey(), redirectionKey);
                fillWithValue(redirectionIndex, leftIt, redirectionKey);
                return;
            } else {
                redirectionIndex.putVoid(leftIt.getKey(), redirectionKey);
                final int nextRightValue = rightIt.nextValue();
                while (leftIt.hasNext() && (disallowExactMatch ? leq(leftIt.nextValue(), nextRightValue) :  lt(leftIt.nextValue(), nextRightValue))) {
                    leftIt.next();
                    redirectionIndex.put(leftIt.getKey(), redirectionKey);
                }
            }
        }
    }

    private static void fillWithNull(RedirectionIndex redirectionIndex, IntReverseSegmentedSortedArray.Iterator leftIt) {
        while (leftIt.hasNext()) {
            leftIt.next();
            redirectionIndex.removeVoid(leftIt.getKey());
        }
    }

    private static void fillWithValue(RedirectionIndex redirectionIndex, IntReverseSegmentedSortedArray.Iterator leftIt, long rightKey) {
        while (leftIt.hasNext()) {
            leftIt.next();
            redirectionIndex.putVoid(leftIt.getKey(), rightKey);
        }
    }

    @Override
    public void processRemovals(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys, WritableLongChunk<RowKeys> priorRedirections, RedirectionIndex redirectionIndex, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        processRemovals((IntReverseSegmentedSortedArray)leftSsa, rightStampChunk.asIntChunk(), rightKeys, priorRedirections, redirectionIndex, modifiedBuilder, disallowExactMatch);
    }

    static private void processRemovals(IntReverseSegmentedSortedArray leftSsa, IntChunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys, WritableLongChunk<RowKeys> nextRedirections, RedirectionIndex redirectionIndex, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        // When removing a row, record the stamp, redirection key, and prior redirection key.  Binary search
        // in the left for the removed key to find the smallest value geq the removed right.  Update all rows
        // with the removed redirection to the previous key.

        final IntReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final int rightStampValue = rightStampChunk.get(ii);
                final long rightStampKey = rightKeys.get(ii);
                final long newRightStampKey = nextRedirections.get(ii);

                leftIt.advanceToBeforeFirst(rightStampValue);

                while (leftIt.hasNext()) {
                    final long leftKey = leftIt.nextKey();
                    final long leftRedirectionKey = redirectionIndex.get(leftKey);
                    if (leftRedirectionKey == rightStampKey) {
                        if (mks == capacity) {
                            capacity *= 2;
                            modifiedKeys.ensureCapacityPreserve(capacity).setSize(capacity);
                        }
                        modifiedKeys.get().set(mks++, leftKey);
                        if (newRightStampKey == RowSet.NULL_ROW_KEY) {
                            redirectionIndex.removeVoid(leftKey);
                        } else {
                            redirectionIndex.putVoid(leftKey, newRightStampKey);
                        }
                        leftIt.next();
                    } else {
                        break;
                    }
                }
            }

            if (mks > 0) {
                modifiedKeys.get().setSize(mks);
                modifiedKeys.get().sort();
                modifiedBuilder.addOrderedRowKeysChunk(WritableLongChunk.downcast(modifiedKeys.get()));
            }
        }
    }

    @Override
    public void processInsertion(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, Chunk<Values> nextRightValue, RedirectionIndex redirectionIndex, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        processInsertion((IntReverseSegmentedSortedArray)leftSsa, rightStampChunk.asIntChunk(), rightKeys, nextRightValue.asIntChunk(), redirectionIndex, modifiedBuilder, endsWithLastValue, disallowExactMatch);
    }

    static private void processInsertion(IntReverseSegmentedSortedArray leftSsa, IntChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, IntChunk<Values> nextRightValue, RedirectionIndex redirectionIndex, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        // We've already filtered out duplicate right stamps by the time we get here, which means that the rightStampChunk
        // contains only values that are the last in any given run; and thus are possible matches.

        // We binary search in the left for the first value >=, everything up until the next extant right value (contained
        // in the nextRightValue chunk) should be re-stamped with our value

        final IntReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final int rightStampValue = rightStampChunk.get(ii);

                leftIt.advanceToBeforeFirst(rightStampValue);

                final long rightStampKey = rightKeys.get(ii);

                if (ii == rightStampChunk.size() - 1 && endsWithLastValue) {
                    while (leftIt.hasNext()) {
                        leftIt.next();
                        final long leftKey = leftIt.getKey();
                        redirectionIndex.putVoid(leftKey, rightStampKey);
                        if (mks == capacity) {
                            capacity *= 2;
                            modifiedKeys.ensureCapacityPreserve(capacity).setSize(capacity);
                        }
                        modifiedKeys.get().set(mks++, leftKey);
                    }
                } else {
                    final int nextRight = nextRightValue.get(ii);
                    while (leftIt.hasNext()) {
                        final int leftValue = leftIt.nextValue();
                        if (disallowExactMatch ? leq(leftValue, nextRight) : lt(leftValue, nextRight)) {
                            final long leftKey = leftIt.nextKey();
                            redirectionIndex.putVoid(leftKey, rightStampKey);
                            if (mks == capacity) {
                                capacity *= 2;
                                modifiedKeys.ensureCapacityPreserve(capacity).setSize(capacity);
                            }
                            modifiedKeys.get().set(mks++, leftKey);
                            leftIt.next();
                        } else {
                            break;
                        }
                    }
                }
            }
            if (mks > 0) {
                modifiedKeys.get().setSize(mks);
                modifiedKeys.get().sort();
                modifiedBuilder.addOrderedRowKeysChunk(WritableLongChunk.downcast(modifiedKeys.get()));
            }
        }
    }

    @Override
    public void findModified(SegmentedSortedArray leftSsa, RedirectionIndex redirectionIndex, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        findModified((IntReverseSegmentedSortedArray)leftSsa, redirectionIndex, rightStampChunk.asIntChunk(), rightStampIndices, modifiedBuilder, disallowExactMatch);
    }

    private static void findModified(IntReverseSegmentedSortedArray leftSsa, RedirectionIndex redirectionIndex, IntChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        final IntReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final int rightStampValue = rightStampChunk.get(ii);

                // now find the lowest left value leq (lt) than rightStampValue
                leftIt.advanceToBeforeFirst(rightStampValue);

                final long rightStampKey = rightStampIndices.get(ii);
                while (leftIt.hasNext() && redirectionIndex.get(leftIt.nextKey()) == rightStampKey) {
                    leftIt.next();

                    if (mks == capacity) {
                        capacity *= 2;
                        modifiedKeys.ensureCapacityPreserve(capacity).setSize(capacity);
                    }
                    modifiedKeys.get().set(mks++, leftIt.getKey());
                }
            }

            if (mks > 0) {
                modifiedKeys.get().setSize(mks);
                modifiedKeys.get().sort();
                modifiedBuilder.addOrderedRowKeysChunk(WritableLongChunk.downcast(modifiedKeys.get()));
            }
        }
    }

    @Override
    public void applyShift(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        applyShift((IntReverseSegmentedSortedArray)leftSsa, rightStampChunk.asIntChunk(), rightStampKeys, shiftDelta, redirectionIndex, disallowExactMatch);
    }

    private void applyShift(IntReverseSegmentedSortedArray leftSsa, IntChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        final IntReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final int rightStampValue = rightStampChunk.get(ii);

            leftIt.advanceToBeforeFirst(rightStampValue);

            final long rightStampKey = rightStampKeys.get(ii);
            while (leftIt.hasNext() && redirectionIndex.get(leftIt.nextKey()) == rightStampKey) {
                leftIt.next();
                redirectionIndex.putVoid(leftIt.getKey(), rightStampKey + shiftDelta);
            }
        }
    }

    // region comparison functions
    // note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)
    private static int doComparison(int lhs, int rhs) {
        return -1 * Integer.compare(lhs, rhs);
    }
    // endregion comparison functions

    private static boolean lt(int lhs, int rhs) {
        return doComparison(lhs, rhs) < 0;
    }

    private static boolean leq(int lhs, int rhs) {
        return doComparison(lhs, rhs) <= 0;
    }
}

