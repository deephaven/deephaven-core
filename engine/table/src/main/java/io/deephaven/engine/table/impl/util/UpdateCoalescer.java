//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;

import java.util.function.BiConsumer;

/**
 * Helper utility for coalescing multiple {@link TableUpdateImpl updates}.
 */
public class UpdateCoalescer {

    public final WritableRowSet added;
    public final WritableRowSet removed;
    public final WritableRowSet modified;

    public RowSetShiftData shifted;
    public ModifiedColumnSet modifiedColumnSet;

    // This is a RowSet that represents which keys still exist in prevSpace for the agg update. It is necessary to
    // keep to ensure we make the correct selections when shift destinations overlap.
    private final WritableRowSet rowSet;

    public UpdateCoalescer(final RowSet rowSet, final TableUpdate update) {
        this.rowSet = rowSet.copy();
        this.rowSet.remove(update.removed());

        this.added = update.added().copy();
        this.removed = update.removed().copy();
        this.modified = update.modified().copy();
        this.shifted = update.shifted();

        if (modified.isEmpty()) {
            // just to be certain
            this.modifiedColumnSet = ModifiedColumnSet.EMPTY;
        } else {
            this.modifiedColumnSet = update.modifiedColumnSet().copy();
        }
    }

    public TableUpdate coalesce() {
        return new TableUpdateImpl(added, removed, modified, shifted, modifiedColumnSet);
    }

    public UpdateCoalescer update(final TableUpdate update) {
        // Remove update.remove from our coalesced post-shift added/modified.
        try (final SafeCloseableList closer = new SafeCloseableList()) {
            final RowSet addedAndRemoved = closer.add(added.extract(update.removed()));

            if (update.removed().isNonempty()) {
                modified.remove(update.removed());
            }

            // Aggregate update.remove in coalesced pre-shift removed.
            try (final WritableRowSet myRemoved = update.removed().minus(addedAndRemoved)) {
                shifted.unapply(myRemoved);
                removed.insert(myRemoved);
                rowSet.remove(myRemoved);
            }

            // Apply new shifts to our post-shift added/modified.
            if (update.shifted().nonempty()) {
                update.shifted().apply(added);
                update.shifted().apply(modified);

                updateShifts(update.shifted());
            }

            // We can't modify rows that did not exist previously.
            try (final RowSet myModified = update.modified().minus(added)) {
                updateModified(update.modifiedColumnSet(), myModified);
            }

            // Note: adding removed identical indices is allowed.
            added.insert(update.added());
        }

        return this;
    }

    private void updateModified(final ModifiedColumnSet myMCS, final RowSet myModified) {
        if (myModified.isEmpty()) {
            return;
        }

        modified.insert(myModified);
        if (modifiedColumnSet.empty()) {
            modifiedColumnSet = myMCS.copy();
        } else {
            modifiedColumnSet.setAll(myMCS);
        }
    }

    private void updateShifts(final RowSetShiftData myShifts) {
        if (shifted.empty()) {
            shifted = myShifts;
            return;
        }

        final RowSet.SearchIterator indexIter = rowSet.searchIterator();
        final RowSetShiftData.Builder newShifts = new RowSetShiftData.Builder();

        // Appends shifts to our builder from watermarkKey to supplied key adding extra delta if needed.
        final MutableInt outerIdx = new MutableInt(0);
        final MutableLong watermarkKey = new MutableLong(0);
        final BiConsumer<Long, Long> fixShiftIfOverlap = (end, ttlDelta) -> {
            long minBegin = newShifts.getMinimumValidBeginForNextDelta(ttlDelta);
            if (ttlDelta < 0) {
                final RowSet.SearchIterator revIter = rowSet.reverseIterator();
                if (revIter.advance(watermarkKey.get() - 1)
                        && revIter.currentValue() > newShifts.lastShiftEnd()) {
                    minBegin = Math.max(minBegin, revIter.currentValue() + 1 - ttlDelta);
                }
            }

            if (end < watermarkKey.get() || minBegin < watermarkKey.get()) {
                return;
            }

            // this means the previous shift overlaps this shift; let's figure out who wins
            final long contestBegin = watermarkKey.get();
            final boolean currentValid = indexIter.advance(contestBegin);
            if (currentValid && indexIter.currentValue() < minBegin && indexIter.currentValue() <= end) {
                newShifts.limitPreviousShiftFor(indexIter.currentValue(), ttlDelta);
                watermarkKey.set(indexIter.currentValue());
            } else {
                watermarkKey.set(Math.min(end + 1, minBegin));
            }
        };

        final BiConsumer<Long, Long> consumeUntilWithExtraDelta = (endRange, extraDelta) -> {
            while (outerIdx.get() < shifted.size() && watermarkKey.get() <= endRange) {
                final long outerBegin =
                        Math.max(watermarkKey.get(), shifted.getBeginRange(outerIdx.get()));
                final long outerEnd = shifted.getEndRange(outerIdx.get());
                final long outerDelta = shifted.getShiftDelta(outerIdx.get());

                // Shift before the outer shift.
                final long headerEnd = Math.min(endRange, outerBegin - 1 + (outerDelta < 0 ? outerDelta : 0));
                if (watermarkKey.get() <= headerEnd && extraDelta != 0) {
                    fixShiftIfOverlap.accept(headerEnd, extraDelta);
                    newShifts.shiftRange(watermarkKey.get(), headerEnd, extraDelta);
                }
                final long maxWatermark =
                        endRange == Long.MAX_VALUE ? outerBegin : Math.min(endRange + 1, outerBegin);
                watermarkKey.set(Math.max(watermarkKey.get(), maxWatermark));

                // Does endRange occur before this outerIdx shift? If so pop-out we need to change extraDelta.
                if (watermarkKey.get() > endRange) {
                    return;
                }

                final long myEnd = Math.min(outerEnd, endRange);
                final long ttlDelta = outerDelta + extraDelta;
                fixShiftIfOverlap.accept(myEnd, ttlDelta);

                newShifts.shiftRange(watermarkKey.get(), myEnd, ttlDelta);
                watermarkKey.set(myEnd + 1);

                // Is this shift completely used up? If so, let's move on to the next!
                if (myEnd == outerEnd) {
                    outerIdx.increment();
                }
            }

            if (outerIdx.get() == shifted.size() && watermarkKey.get() <= endRange && extraDelta != 0) {
                fixShiftIfOverlap.accept(endRange, extraDelta);
                newShifts.shiftRange(watermarkKey.get(), endRange, extraDelta);
            }
            watermarkKey.set(endRange + 1);
        };

        final ShiftInversionHelper inverter = new ShiftInversionHelper(shifted);

        for (int si = 0; si < myShifts.size(); ++si) {
            final long beginKey = inverter.mapToPrevKeyspace(myShifts.getBeginRange(si), false);
            final long endKey = inverter.mapToPrevKeyspace(myShifts.getEndRange(si), true);
            if (endKey < beginKey) {
                continue;
            }

            consumeUntilWithExtraDelta.accept(beginKey - 1, 0L);
            consumeUntilWithExtraDelta.accept(endKey, myShifts.getShiftDelta(si));
        }
        consumeUntilWithExtraDelta.accept(Long.MAX_VALUE, 0L);

        shifted = newShifts.build();
    }
}
