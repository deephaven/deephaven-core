/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsaSsaStamp and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.rowset.RowSetBuilderRandom;

/**
 * Stamp kernel for when the left hand side is a sorted chunk and the right hand side is a ticking SegmentedSortedArray.
 */
public class LongReverseSsaSsaStamp implements SsaSsaStamp {
    static LongReverseSsaSsaStamp INSTANCE = new LongReverseSsaSsaStamp();

    private LongReverseSsaSsaStamp() {} // use the instance

    @Override
    public void processEntry(SegmentedSortedArray leftSsa, SegmentedSortedArray rightSsa, WritableRowRedirection rowRedirection, boolean disallowExactMatch) {
        processEntry((LongReverseSegmentedSortedArray)leftSsa, (LongReverseSegmentedSortedArray)rightSsa, rowRedirection, disallowExactMatch);
    }

    private static void processEntry(LongReverseSegmentedSortedArray leftSsa, LongReverseSegmentedSortedArray rightSsa, WritableRowRedirection rowRedirection, boolean disallowExactMatch) {
        final long rightSize = rightSsa.size();
        if (rightSize == 0) {
            fillWithNull(rowRedirection, leftSsa.iterator(disallowExactMatch, false));
            return;
        }

        final LongReverseSegmentedSortedArray.Iterator rightIt = rightSsa.iterator(disallowExactMatch, true);
        final LongReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        while (leftIt.hasNext()) {
            leftIt.next();
            final long leftValue = leftIt.getValue();
            final int comparison = doComparison(leftValue, rightIt.getValue());
            if (disallowExactMatch ? comparison <= 0 : comparison < 0) {
                rowRedirection.removeVoid(leftIt.getKey());
                continue;
            }
            else if (comparison == 0) {
                rowRedirection.putVoid(leftIt.getKey(), rightIt.getKey());
                continue;
            }

            rightIt.advanceToLast(leftValue);

            final long redirectionKey = rightIt.getKey();
            if (!rightIt.hasNext()) {
                rowRedirection.put(leftIt.getKey(), redirectionKey);
                fillWithValue(rowRedirection, leftIt, redirectionKey);
                return;
            } else {
                rowRedirection.putVoid(leftIt.getKey(), redirectionKey);
                final long nextRightValue = rightIt.nextValue();
                while (leftIt.hasNext() && (disallowExactMatch ? leq(leftIt.nextValue(), nextRightValue) :  lt(leftIt.nextValue(), nextRightValue))) {
                    leftIt.next();
                    rowRedirection.put(leftIt.getKey(), redirectionKey);
                }
            }
        }
    }

    private static void fillWithNull(WritableRowRedirection rowRedirection, LongReverseSegmentedSortedArray.Iterator leftIt) {
        while (leftIt.hasNext()) {
            leftIt.next();
            rowRedirection.removeVoid(leftIt.getKey());
        }
    }

    private static void fillWithValue(WritableRowRedirection rowRedirection, LongReverseSegmentedSortedArray.Iterator leftIt, long rightKey) {
        while (leftIt.hasNext()) {
            leftIt.next();
            rowRedirection.putVoid(leftIt.getKey(), rightKey);
        }
    }

    @Override
    public void processRemovals(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, WritableLongChunk<RowKeys> priorRedirections, WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        processRemovals((LongReverseSegmentedSortedArray)leftSsa, rightStampChunk.asLongChunk(), rightKeys, priorRedirections, rowRedirection, modifiedBuilder, disallowExactMatch);
    }

    static private void processRemovals(LongReverseSegmentedSortedArray leftSsa, LongChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, WritableLongChunk<RowKeys> nextRedirections, WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        // When removing a row, record the stamp, redirection key, and prior redirection key.  Binary search
        // in the left for the removed key to find the smallest value geq the removed right.  Update all rows
        // with the removed redirection to the previous key.

        final LongReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final long rightStampValue = rightStampChunk.get(ii);
                final long rightStampKey = rightKeys.get(ii);
                final long newRightStampKey = nextRedirections.get(ii);

                leftIt.advanceToBeforeFirst(rightStampValue);

                while (leftIt.hasNext()) {
                    final long leftKey = leftIt.nextKey();
                    final long leftRedirectionKey = rowRedirection.get(leftKey);
                    if (leftRedirectionKey == rightStampKey) {
                        if (mks == capacity) {
                            capacity *= 2;
                            modifiedKeys.ensureCapacityPreserve(capacity).setSize(capacity);
                        }
                        modifiedKeys.get().set(mks++, leftKey);
                        if (newRightStampKey == RowSequence.NULL_ROW_KEY) {
                            rowRedirection.removeVoid(leftKey);
                        } else {
                            rowRedirection.putVoid(leftKey, newRightStampKey);
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
    public void processInsertion(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, Chunk<Values> nextRightValue, WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        processInsertion((LongReverseSegmentedSortedArray)leftSsa, rightStampChunk.asLongChunk(), rightKeys, nextRightValue.asLongChunk(), rowRedirection, modifiedBuilder, endsWithLastValue, disallowExactMatch);
    }

    static private void processInsertion(LongReverseSegmentedSortedArray leftSsa, LongChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightKeys, LongChunk<Values> nextRightValue, WritableRowRedirection rowRedirection, RowSetBuilderRandom modifiedBuilder, boolean endsWithLastValue, boolean disallowExactMatch) {
        // We've already filtered out duplicate right stamps by the time we get here, which means that the rightStampChunk
        // contains only values that are the last in any given run; and thus are possible matches.

        // We binary search in the left for the first value >=, everything up until the next extant right value (contained
        // in the nextRightValue chunk) should be re-stamped with our value

        final LongReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final long rightStampValue = rightStampChunk.get(ii);

                leftIt.advanceToBeforeFirst(rightStampValue);

                final long rightStampKey = rightKeys.get(ii);

                if (ii == rightStampChunk.size() - 1 && endsWithLastValue) {
                    while (leftIt.hasNext()) {
                        leftIt.next();
                        final long leftKey = leftIt.getKey();
                        rowRedirection.putVoid(leftKey, rightStampKey);
                        if (mks == capacity) {
                            capacity *= 2;
                            modifiedKeys.ensureCapacityPreserve(capacity).setSize(capacity);
                        }
                        modifiedKeys.get().set(mks++, leftKey);
                    }
                } else {
                    final long nextRight = nextRightValue.get(ii);
                    while (leftIt.hasNext()) {
                        final long leftValue = leftIt.nextValue();
                        if (disallowExactMatch ? leq(leftValue, nextRight) : lt(leftValue, nextRight)) {
                            final long leftKey = leftIt.nextKey();
                            rowRedirection.putVoid(leftKey, rightStampKey);
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
    public void findModified(SegmentedSortedArray leftSsa, RowRedirection rowRedirection, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        findModified((LongReverseSegmentedSortedArray)leftSsa, rowRedirection, rightStampChunk.asLongChunk(), rightStampIndices, modifiedBuilder, disallowExactMatch);
    }

    private static void findModified(LongReverseSegmentedSortedArray leftSsa, RowRedirection rowRedirection, LongChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampIndices, RowSetBuilderRandom modifiedBuilder, boolean disallowExactMatch) {
        final LongReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        try (final SizedLongChunk<RowKeys> modifiedKeys = new SizedLongChunk<>()) {
            int capacity = rightStampChunk.size();
            modifiedKeys.ensureCapacity(capacity).setSize(capacity);
            int mks = 0;

            for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
                final long rightStampValue = rightStampChunk.get(ii);

                // now find the lowest left value leq (lt) than rightStampValue
                leftIt.advanceToBeforeFirst(rightStampValue);

                final long rightStampKey = rightStampIndices.get(ii);
                while (leftIt.hasNext() && rowRedirection.get(leftIt.nextKey()) == rightStampKey) {
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
    public void applyShift(SegmentedSortedArray leftSsa, Chunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta, WritableRowRedirection rowRedirection, boolean disallowExactMatch) {
        applyShift((LongReverseSegmentedSortedArray)leftSsa, rightStampChunk.asLongChunk(), rightStampKeys, shiftDelta, rowRedirection, disallowExactMatch);
    }

    private void applyShift(LongReverseSegmentedSortedArray leftSsa, LongChunk<? extends Values> rightStampChunk, LongChunk<RowKeys> rightStampKeys, long shiftDelta, WritableRowRedirection rowRedirection, boolean disallowExactMatch) {
        final LongReverseSegmentedSortedArray.Iterator leftIt = leftSsa.iterator(disallowExactMatch, false);

        for (int ii = 0; ii < rightStampChunk.size(); ++ii) {
            final long rightStampValue = rightStampChunk.get(ii);

            leftIt.advanceToBeforeFirst(rightStampValue);

            final long rightStampKey = rightStampKeys.get(ii);
            while (leftIt.hasNext() && rowRedirection.get(leftIt.nextKey()) == rightStampKey) {
                leftIt.next();
                rowRedirection.putVoid(leftIt.getKey(), rightStampKey + shiftDelta);
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
}

