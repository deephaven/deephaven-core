import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.impl.RowSetFactory;
import io.deephaven.util.SafeCloseablePair;

/**
 * Utilities for working with {@link RowSetShiftData}.
 */
public class RowSetShiftUtils {
    /**
     * Apply all shifts to the provided rowSet. Moves rowSet from pre-shift keyspace to post-shift keyspace.
     *
     * @param shiftData The {@link RowSetShiftData}
     * @param rowSet The {@link RowSet} to shift
     * @return {@code rowSet}
     */
    public static WritableRowSet apply(RowSetShiftData shiftData, final WritableRowSet rowSet) {
        final RowSetBuilderSequential toRemove = RowSetFactory.builderSequential();
        final RowSetBuilderSequential toInsert = RowSetFactory.builderSequential();
        try (final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator()) {
            for (int idx = 0; idx < shiftData.size(); ++idx) {
                final long beginRange = shiftData.getBeginRange(idx);
                final long endRange = shiftData.getEndRange(idx);
                final long shiftDelta = shiftData.getShiftDelta(idx);

                if (!rsIt.advance(beginRange)) {
                    break;
                }

                toRemove.appendRange(beginRange, endRange);
                rsIt.getNextRowSequenceThrough(endRange)
                        .forAllRowKeyRanges((s, e) -> toInsert.appendRange(s + shiftDelta, e + shiftDelta));
            }
        }

        try (final RowSet remove = toRemove.build();
             final RowSet insert = toInsert.build()) {
            rowSet.remove(remove);
            rowSet.insert(insert);
        }
        return rowSet;
    }

    /**
     * Unapply all shifts to the provided rowSet. Moves rowSet from post-shift keyspace to pre-shift keyspace.
     *
     * @param shiftData The {@link RowSetShiftData}
     * @param rowSet The {@link RowSet} to shift
     * @return {@code rowSet}
     */
    public static RowSet unapply(RowSetShiftData shiftData, final WritableRowSet rowSet) {
        final RowSetBuilderSequential toRemove = RowSetFactory.builderSequential();
        final RowSetBuilderSequential toInsert = RowSetFactory.builderSequential();
        try (final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator()) {
            for (int idx = 0; idx < shiftData.size(); ++idx) {
                final long beginRange = shiftData.getBeginRange(idx);
                final long endRange = shiftData.getEndRange(idx);
                final long shiftDelta = shiftData.getShiftDelta(idx);

                if (!rsIt.advance(beginRange + shiftDelta)) {
                    break;
                }

                toRemove.appendRange(beginRange + shiftDelta, endRange + shiftDelta);
                rsIt.getNextRowSequenceThrough(endRange + shiftDelta)
                        .forAllRowKeyRanges((s, e) -> toInsert.appendRange(s - shiftDelta, e - shiftDelta));
            }
        }

        try (final RowSet remove = toRemove.build();
                final RowSet insert = toInsert.build()) {
            rowSet.remove(remove);
            rowSet.insert(insert);
        }
        return rowSet;
    }

    /**
     * This method creates two parallel TrackingWritableRowSet structures that contain postShiftIndex keys affected by
     * shifts. The two Indexes have the same size. An element at position k in the first rowSet is the pre-shift key for
     * the same row whose post-shift key is at position k in the second rowSet.
     *
     * @param shiftData The {@link RowSetShiftData}
     * @param postShiftIndex The rowSet of keys that were shifted in post-shift keyspace. It should not contain rows
     *        that did not exist prior to the shift.
     * @return A SafeCloseablePair of preShiftedKeys and postShiftedKeys that intersect this RowSetShiftData with
     *         postShiftIndex.
     */
    public static SafeCloseablePair<RowSet, RowSet> extractParallelShiftedRowsFromPostShiftIndex(
            RowSetShiftData shiftData, final RowSet postShiftIndex) {
        if (shiftData.empty()) {
            return SafeCloseablePair.of(RowSetFactory.empty(),
                    RowSetFactory.empty());
        }

        final RowSetBuilderSequential preShiftBuilder = RowSetFactory.builderSequential();
        final RowSetBuilderSequential postShiftBuilder = RowSetFactory.builderSequential();

        try (final RowSequence.Iterator rsIt = postShiftIndex.getRowSequenceIterator()) {
            for (int idx = 0; idx < shiftData.size(); ++idx) {
                final long beginRange = shiftData.getBeginRange(idx);
                final long endRange = shiftData.getEndRange(idx);
                final long shiftDelta = shiftData.getShiftDelta(idx);

                if (!rsIt.advance(beginRange + shiftDelta)) {
                    break;
                }

                rsIt.getNextRowSequenceThrough(endRange + shiftDelta).forAllRowKeyRanges((s, e) -> {
                    preShiftBuilder.appendRange(s - shiftDelta, e - shiftDelta);
                    postShiftBuilder.appendRange(s, e);
                });
            }
        }

        final SafeCloseablePair<RowSet, RowSet> retVal =
                SafeCloseablePair.of(preShiftBuilder.build(), postShiftBuilder.build());
        Assert.eq(retVal.first.size(), "retVal.first.size()", retVal.second.size(), "retVal.second.size()");
        return retVal;
    }
}
