package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.util.*;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

abstract class StaticNaturalJoinStateManager {
    static final int CHUNK_SIZE = 4096;
    static final long DUPLICATE_RIGHT_VALUE = -2;
    static final long NO_RIGHT_ENTRY_VALUE = RowSequence.NULL_ROW_KEY;

    final ColumnSource<?>[] keySourcesForErrorMessages;

    StaticNaturalJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        this.keySourcesForErrorMessages = keySourcesForErrorMessages;
    }

    @SuppressWarnings("WeakerAccess")
    public void checkExactMatch(boolean exactMatch, long leftKeyIndex, long rightSide) {
        if (exactMatch && rightSide == NO_RIGHT_ENTRY_VALUE) {
            throw new RuntimeException("Tables don't have one-to-one mapping - no mappings for key " + extractKeyStringFromSourceTable(leftKeyIndex) + ".");
        }
    }

    // produce a pretty key for error messages
    private String extractKeyStringFromSourceTable(long leftKey) {
        if (keySourcesForErrorMessages.length == 1) {
            return Objects.toString(keySourcesForErrorMessages[0].get(leftKey));
        }
        return "[" + Arrays.stream(keySourcesForErrorMessages).map(ls -> Objects.toString(ls.get(leftKey))).collect(Collectors.joining(", ")) + "]";
    }

    WritableRowRedirection buildRowRedirection(QueryTable leftTable, boolean exactMatch, LongUnaryOperator rightSideFromSlot, JoinControl.RedirectionType redirectionType) {
        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat()) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];
                for (int ii = 0; ii < innerIndex.length; ++ii) {
                    final long rightSide = rightSideFromSlot.applyAsLong(ii);
                    checkExactMatch(exactMatch, leftTable.getRowSet().get(ii), rightSide);
                    innerIndex[ii] = rightSide;
                }
                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();

                long leftPosition = 0;
                for (final RowSet.Iterator it = leftTable.getRowSet().iterator(); it.hasNext(); ) {
                    final long next = it.nextLong();
                    final long rightSide = rightSideFromSlot.applyAsLong(leftPosition++);
                    checkExactMatch(exactMatch, leftTable.getRowSet().get(next), rightSide);
                    if (rightSide != NO_RIGHT_ENTRY_VALUE) {
                        sparseRedirections.set(next, rightSide);
                    }
                }
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection = WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());

                long leftPosition = 0;
                for (final RowSet.Iterator it = leftTable.getRowSet().iterator(); it.hasNext(); ) {
                    final long next = it.nextLong();
                    final long rightSide = rightSideFromSlot.applyAsLong(leftPosition++);
                    checkExactMatch(exactMatch, leftTable.getRowSet().get(next), rightSide);
                    if (rightSide != NO_RIGHT_ENTRY_VALUE) {
                        rowRedirection.put(next, rightSide);
                    }
                }

                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    abstract void decorateLeftSide(RowSet leftRowSet, ColumnSource<?> [] leftSources, final LongArraySource leftRedirections);
}
