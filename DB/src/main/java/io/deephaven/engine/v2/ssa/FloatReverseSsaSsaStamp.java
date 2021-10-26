/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsaSsaStamp and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.ssa;

import io.deephaven.engine.util.DhFloatComparisons;

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
public class FloatReverseSsaSsaStamp implements SsaSsaStamp {
    static FloatReverseSsaSsaStamp INSTANCE = new FloatReverseSsaSsaStamp();

    private FloatReverseSsaSsaStamp() {} // use the instance

    @Override
    public void processEntry(SegmentedSortedArray leftSsa, SegmentedSortedArray rightSsa, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        processEntry((FloatReverseSegmentedSortedArray)leftSsa, (FloatReverseSegmentedSortedArray)rightSsa, redirectionIndex, disallowExactMatch);
    }

    private static void processEntry(FloatReverseSegmentedSortedArray leftSsa, FloatReverseSegmentedSortedArray rightSsa, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        final long rightSize = rightSsa.size();
        if (rightSize == 0) {
            fillWithNull(redirectionIndex, leftSsa.iterator(disallowExactMatch, false));
            return;
        }

        final FloatReverseSegmentedSortedArray.Iterator rightIt = rightSsa.iterator(disallowExactMatch, true);
        final FloatReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        while (leftIt.hasNext()) {
            leftIt.next();
            final float leftValue = leftIt.getValue();
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
                final float nextRightValue = rightIt.nextValue();
                while (leftIt.hasNext() && (disallowExactMatch ? leq(leftIt.nextValue(), nextRightValue) :  lt(leftIt.nextValue(), nextRightValue))) {
                    leftIt.next();
                    redirectionIndex.put(leftIt.getKey(), redirectionKey);
                }
            }
        }
    }

    private static void fillWithNull(RedirectionIndex redirectionIndex, FloatReverseSegmentedSortedArray.Iterator leftIt) {
        while (leftIt.hasNext()) {
            leftIt.next();
            redirectionIndex.removeVoid(leftIt.getKey());
        }
    }

    private static void fillWithValue(RedirectionIndex redirectionIndex, FloatReverseSegmentedSortedArray.Iterator leftIt, long rightKey) {
        while (leftIt.hasNext()) {
            leftIt.next();
            redirectionIndex.putVoid(leftIt.getKey(), rightKey);
        }
    }

    @Override
    public void processRemovals(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys, WritableLongChunk<Attributes.RowKeys> priorRedirections, RedirectionIndex redirectionIndex, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        processRemovals((FloatReverseSegmentedSortedArray)leftSsa, rightStampChunk.asFloatChunk(), rightKeys, priorRedirections, redirectionIndex, modifiedBuilder, disallowExactMatch);
    }

    static private void processRemovals(FloatReverseSegmentedSortedArray leftSsa, FloatChunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys, WritableLongChunk<Attributes.RowKeys> nextRedirections, RedirectionIndex redirectionIndex, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        // When removing a row, record the stamp, redirection key, and prior redirection key.  Binary search
        // in the left for the removed key to find the smallest value geq the removed right.  Update all rows
        // with the removed redirection to the previous key.

        final FloatReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<Attributes.RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final float rightStampValue = rightStampChunk.get(ii);
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
    public void processInsertion(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys, Chunk<Values> nextRightValue, RedirectionIndex redirectionIndex, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        processInsertion((FloatReverseSegmentedSortedArray)leftSsa, rightStampChunk.asFloatChunk(), rightKeys, nextRightValue.asFloatChunk(), redirectionIndex, modifiedBuilder, endsWithLastValue, disallowExactMatch);
    }

    static private void processInsertion(FloatReverseSegmentedSortedArray leftSsa, FloatChunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightKeys, FloatChunk<Values> nextRightValue, RedirectionIndex redirectionIndex, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        // We've already filtered out duplicate right stamps by the time we get here, which means that the rightStampChunk
        // contains only values that are the last in any given run; and thus are possible matches.

        // We binary search in the left for the first value >=, everything up until the next extant right value (contained
        // in the nextRightValue chunk) should be re-stamped with our value

        final FloatReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final float rightStampValue = rightStampChunk.get(ii);

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
                    final float nextRight = nextRightValue.get(ii);
                    while (leftIt.hasNext()) {
                        final float leftValue = leftIt.nextValue();
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
        findModified((FloatReverseSegmentedSortedArray)leftSsa, redirectionIndex, rightStampChunk.asFloatChunk(), rightStampIndices, modifiedBuilder, disallowExactMatch);
    }

    private static void findModified(FloatReverseSegmentedSortedArray leftSsa, RedirectionIndex redirectionIndex, FloatChunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        final FloatReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final float rightStampValue = rightStampChunk.get(ii);

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
        applyShift((FloatReverseSegmentedSortedArray)leftSsa, rightStampChunk.asFloatChunk(), rightStampKeys, shiftDelta, redirectionIndex, disallowExactMatch);
    }

    private void applyShift(FloatReverseSegmentedSortedArray leftSsa, FloatChunk<? extends Values> rightStampChunk, LongChunk<Attributes.RowKeys> rightStampKeys, long shiftDelta, RedirectionIndex redirectionIndex, boolean disallowExactMatch) {
        final FloatReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final float rightStampValue = rightStampChunk.get(ii);

            leftIt.advanceToBeforeFirst(rightStampValue);

            final long rightStampKey = rightStampKeys.get(ii);
            while (leftIt.hasNext() && redirectionIndex.get(leftIt.nextKey()) == rightStampKey) {
                leftIt.next();
                redirectionIndex.putVoid(leftIt.getKey(), rightStampKey + shiftDelta);
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
}

