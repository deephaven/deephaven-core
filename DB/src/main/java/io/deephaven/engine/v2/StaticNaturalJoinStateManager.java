package io.deephaven.engine.v2;

import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.LongArraySource;
import io.deephaven.engine.v2.sources.LongSparseArraySource;
import io.deephaven.engine.v2.utils.*;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

abstract class StaticNaturalJoinStateManager {
    static final int CHUNK_SIZE = 4096;
    static final long DUPLICATE_RIGHT_VALUE = -2;
    static final long NO_RIGHT_ENTRY_VALUE = TrackingMutableRowSet.NULL_ROW_KEY;

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

    RedirectionIndex buildRedirectionIndex(QueryTable leftTable, boolean exactMatch, LongUnaryOperator rightSideFromSlot, JoinControl.RedirectionType redirectionType) {
        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat()) {
                    throw new IllegalStateException("Left table is not flat for contiguous redirection rowSet build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];
                for (int ii = 0; ii < innerIndex.length; ++ii) {
                    final long rightSide = rightSideFromSlot.applyAsLong(ii);
                    checkExactMatch(exactMatch, leftTable.getIndex().get(ii), rightSide);
                    innerIndex[ii] = rightSide;
                }
                return new ContiguousRedirectionIndexImpl(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();

                long leftPosition = 0;
                for (final TrackingMutableRowSet.Iterator it = leftTable.getIndex().iterator(); it.hasNext(); ) {
                    final long next = it.nextLong();
                    final long rightSide = rightSideFromSlot.applyAsLong(leftPosition++);
                    checkExactMatch(exactMatch, leftTable.getIndex().get(next), rightSide);
                    if (rightSide != NO_RIGHT_ENTRY_VALUE) {
                        sparseRedirections.set(next, rightSide);
                    }
                }
                return new LongColumnSourceRedirectionIndex(sparseRedirections);
            }
            case Hash: {
                final RedirectionIndex redirectionIndex = RedirectionIndexLockFreeImpl.FACTORY.createRedirectionIndex(leftTable.intSize());

                long leftPosition = 0;
                for (final TrackingMutableRowSet.Iterator it = leftTable.getIndex().iterator(); it.hasNext(); ) {
                    final long next = it.nextLong();
                    final long rightSide = rightSideFromSlot.applyAsLong(leftPosition++);
                    checkExactMatch(exactMatch, leftTable.getIndex().get(next), rightSide);
                    if (rightSide != NO_RIGHT_ENTRY_VALUE) {
                        redirectionIndex.put(next, rightSide);
                    }
                }

                return redirectionIndex;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    abstract void decorateLeftSide(TrackingMutableRowSet leftRowSet, ColumnSource<?> [] leftSources, final LongArraySource leftRedirections);
}
