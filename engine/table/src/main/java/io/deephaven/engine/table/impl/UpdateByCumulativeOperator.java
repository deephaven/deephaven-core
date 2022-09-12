package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.engine.table.impl.updateby.internal.BaseCharUpdateByOperator;
import io.deephaven.engine.table.impl.updateby.internal.BaseWindowedByteUpdateByOperator;
import io.deephaven.tablelogger.Row;
import org.jetbrains.annotations.NotNull;

public abstract class UpdateByCumulativeOperator implements UpdateByOperator {

    public abstract class UpdateCumulativeContext implements UpdateContext {
        protected LongSegmentedSortedArray timestampSsa;

        protected RowSetBuilderSequential modifiedBuilder;
        protected RowSet newModified;

        // store the current subset of rows that need computation
        protected RowSet affectedRows;

        public LongSegmentedSortedArray getTimestampSsa() {
            return timestampSsa;
        }

        /**
         * Find the smallest valued key that participated in the upstream {@link TableUpdate}.
         *
         * @param added the added rows
         * @param modified the modified rows
         * @param removed the removed rows
         * @param shifted the shifted rows
         *
         * @return the smallest key that participated in any part of the update.
         */
        long smallestAffectedKey(@NotNull final RowSet added,
                                        @NotNull final RowSet modified,
                                        @NotNull final RowSet removed,
                                        @NotNull final RowSetShiftData shifted,
                                        @NotNull final RowSet affectedIndex) {

            long smallestModifiedKey = Long.MAX_VALUE;
            if (removed.isNonempty()) {
                smallestModifiedKey = removed.firstRowKey();
            }

            if (added.isNonempty()) {
                smallestModifiedKey = Math.min(smallestModifiedKey, added.firstRowKey());
            }

            if (modified.isNonempty()) {
                smallestModifiedKey = Math.min(smallestModifiedKey, modified.firstRowKey());
            }

            if (shifted.nonempty()) {
                final long firstModKey = modified.isEmpty() ? Long.MAX_VALUE : modified.firstRowKey();
                boolean modShiftFound = !modified.isEmpty();
                boolean affectedFound = false;
                try (final RowSequence.Iterator it = affectedIndex.getRowSequenceIterator()) {
                    for (int shiftIdx = 0; shiftIdx < shifted.size() && (!modShiftFound || !affectedFound); shiftIdx++) {
                        final long shiftStart = shifted.getBeginRange(shiftIdx);
                        final long shiftEnd = shifted.getEndRange(shiftIdx);
                        final long shiftDelta = shifted.getShiftDelta(shiftIdx);

                        if (!affectedFound) {
                            if (it.advance(shiftStart + shiftDelta)) {
                                final long maybeAffectedKey = it.peekNextKey();
                                if (maybeAffectedKey <= shiftEnd + shiftDelta) {
                                    affectedFound = true;
                                    final long keyToCompare =
                                            shiftDelta > 0 ? maybeAffectedKey - shiftDelta : maybeAffectedKey;
                                    smallestModifiedKey = Math.min(smallestModifiedKey, keyToCompare);
                                }
                            } else {
                                affectedFound = true;
                            }
                        }

                        if (!modShiftFound) {
                            if (firstModKey <= (shiftEnd + shiftDelta)) {
                                modShiftFound = true;
                                // If the first modified key is in the range we should include it
                                if (firstModKey >= (shiftStart + shiftDelta)) {
                                    smallestModifiedKey = Math.min(smallestModifiedKey, firstModKey - shiftDelta);
                                } else {
                                    // Otherwise it's not included in any shifts, and since shifts can't reorder rows
                                    // it is the smallest possible value and we've already accounted for it above.
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            return smallestModifiedKey;
        }

        public RowSet determineAffectedRows(@NotNull final TableUpdate upstream, @NotNull final TrackingRowSet source, final boolean initialStep) {
            Assert.assertion(affectedRows==null, "affectedRows should be null when determineAffectedRows() is called");
            if (initialStep) {
                affectedRows = source.copy();
                return affectedRows;
            }

            long smallestModifiedKey = smallestAffectedKey(upstream.added(), upstream.modified(), upstream.removed(), upstream.shifted(), source);

            affectedRows = smallestModifiedKey == Long.MAX_VALUE
                    ? RowSetFactory.empty()
                    : source.subSetByKeyRange(smallestModifiedKey, source.lastRowKey());
            return affectedRows;
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        public RowSet getInfluencerRows() {
            return affectedRows;
        }

        public RowSetBuilderSequential getModifiedBuilder() {
            if(modifiedBuilder == null) {
                modifiedBuilder = RowSetFactory.builderSequential();
            }
            return modifiedBuilder;
        }

        @Override
        public void close() {
            try (final RowSet ignored = affectedRows;
                 final RowSet ignored2 = newModified) {
            }
        }
    }

    @Override
    public void initializeFor(@NotNull final UpdateContext context,
                              @NotNull final RowSet updateRowSet) {
    }

    @Override
    public void finishFor(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext)context;
        ctx.newModified = ctx.getModifiedBuilder().build();
    }

    @NotNull
    final public RowSet getAdditionalModifications(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext)context;
        return ctx.newModified;
    }

    @Override
    final public boolean anyModified(@NotNull final UpdateContext context) {
        UpdateCumulativeContext ctx = (UpdateCumulativeContext)context;
        return ctx.newModified != null && ctx.newModified.isNonempty();
    }

    /*** nearly all cumulative operators will not reference a timestamp column, exceptions are Ema */
    @Override
    public String getTimestampColumnName() {
        return null;
    }

    /*** cumulative operators do not need keys */
    @Override
    public boolean requiresKeys() {
        return false;
    }

    /*** cumulative operators do not need position data */
    @Override
    public boolean requiresPositions() {
        return false;
    }

    /*** cumulative operators always need values */
    @Override
    public boolean requiresValues(@NotNull final UpdateContext context) {
        return true;
    }
}
