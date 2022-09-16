package io.deephaven.engine.table.impl.updateby;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.UpdateByOperator;
import io.deephaven.engine.table.impl.UpdateByWindowedOperator;
import io.deephaven.engine.table.impl.ssa.LongSegmentedSortedArray;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;

import static io.deephaven.util.QueryConstants.NULL_LONG;

public class UpdateByWindow {
    protected final boolean windowed;
    @Nullable
    protected final String timestampColumnName;
    protected final long prevUnits;
    protected final long fwdUnits;

    protected ArrayList<UpdateByOperator> operators;

    public class UpdateByWindowContext implements SafeCloseable {
        /** the rows affected by this update */
        protected RowSet affectedRows;
        protected RowSet influencerRows;

        // for use with a ticking window
        protected RowSet affectedRowPositions;
        protected RowSet influencerPositions;

        // keep track of what rows were modified (we'll use a single set for all operators sharing a window)
        protected RowSetBuilderSequential modifiedBuilder;
        protected RowSet newModified;

        /** the column source providing the timestamp data for this window */ 
        @Nullable
        protected ColumnSource<?> timestampColumnSource;

        /** the timestamp SSA providing fast lookup for time windows */
        @Nullable
        protected LongSegmentedSortedArray timestampSsa;

        /** An array of boolean denoting which operators are affected by the current update. */
        final boolean[] opAffected;

        /** An array of context objects for each underlying operator */
        final UpdateByOperator.UpdateContext[] opContext;
        
        public UpdateByWindowContext(final ColumnSource<?> timestampColumnSource, final LongSegmentedSortedArray timestampSsa) {
            this.timestampColumnSource = timestampColumnSource;
            this.timestampSsa = timestampSsa;

            this.opAffected = new boolean[operators.size()];
            // noinspection unchecked
//            this.fillContexts = new SizedSafeCloseable[operators.size()];
            this.opContext = new UpdateByOperator.UpdateContext[operators.size()];
        }

        public boolean computeAffectedAndMakeContexts(@NotNull final TableUpdate upstream, @NotNull final TrackingRowSet source, @Nullable final ModifiedColumnSet[] inputModifiedColumnSets, final int chunkSize, final boolean initialStep) {
            // determine the affected rows for this window context
            if (initialStep) {
                // all rows are affected initially
                affectedRows = source.copy();
                influencerRows = affectedRows;

                // no need to invert, just create a flat rowset
                if (windowed && timestampColumnName == null) {
                    affectedRowPositions = RowSetFactory.flat(source.size());
                    influencerPositions = RowSetFactory.flat(source.size());
                }
                // mark all operators as affected by this update and create contexts
                for (int opIdx = 0; opIdx < operators.size(); opIdx++) {
                    opAffected[opIdx] = true;
                    opContext[opIdx] = operators.get(opIdx).makeUpdateContext(chunkSize, timestampSsa);
                }
                return true;
            } else {
                // determine which operators are affected by this change
                boolean anyAffected = false;
                boolean allAffected = upstream.added().isNonempty() ||
                        upstream.removed().isNonempty() ||
                        upstream.shifted().nonempty();

                for (int opIdx = 0; opIdx < operators.size(); opIdx++) {
                    opAffected[opIdx] = allAffected
                        || (upstream.modifiedColumnSet().nonempty() && (inputModifiedColumnSets == null
                        || upstream.modifiedColumnSet().containsAny(inputModifiedColumnSets[opIdx])));

                    // mark the operators affected by this update and create contexts
                    if (opAffected[opIdx]) {
                        anyAffected = true;
                        opContext[opIdx] = operators.get(opIdx).makeUpdateContext(chunkSize, timestampSsa);
                    }
                }

                if (source.isEmpty() || !anyAffected) {
                    // no work to do for this window this cycle
                    return false;
                }

                // handle the three major types of windows: cumulative, windowed by ticks, windowed by time

                // cumulative is simple, just find the smallest key and return the range from smallest to end
                if (!windowed) {
                    long smallestModifiedKey = smallestAffectedKey(upstream.added(), upstream.modified(), upstream.removed(),
                            upstream.shifted(), source);

                    affectedRows = smallestModifiedKey == Long.MAX_VALUE
                            ? RowSetFactory.empty()
                            : source.subSetByKeyRange(smallestModifiedKey, source.lastRowKey());
                    influencerRows = affectedRows;
                    return true;
                }

                // changed rows are all mods+adds
                WritableRowSet changed = upstream.added().copy();
                changed.insert(upstream.modified());

                WritableRowSet tmpAffected;

                // compute the rows affected from these changes
                if (timestampColumnName == null) {
                    try (final WritableRowSet changedInverted = source.invert(changed)) {
                        tmpAffected = computeAffectedRowsTicks(source, changed, changedInverted, prevUnits, fwdUnits);
                    }
                } else {
                    tmpAffected = computeAffectedRowsTime(source, changed, prevUnits, fwdUnits);
                }

                // other rows can be affected by removes
                if (upstream.removed().isNonempty()) {
                    try (final RowSet prev = source.copyPrev();
                         final RowSet removedPositions = timestampColumnName == null
                                 ? null : prev.invert(upstream.removed());
                         final WritableRowSet affectedByRemoves = timestampColumnName == null
                                 ? computeAffectedRowsTicks(prev, upstream.removed(), removedPositions, prevUnits, fwdUnits)
                                 : computeAffectedRowsTime(prev, upstream.removed(), prevUnits, fwdUnits)) {
                        // apply shifts to get back to pos-shift space
                        upstream.shifted().apply(affectedByRemoves);
                        // retain only the rows that still exist in the source
                        affectedByRemoves.retain(source);
                        tmpAffected.insert(affectedByRemoves);
                    }
                }

                affectedRows = tmpAffected;

                // now get influencer rows for the affected rows
                if (timestampColumnName == null) {
                    // generate position data rowsets for efficiently computed position offsets
                    affectedRowPositions = source.invert(affectedRows);

                    influencerRows = computeInfluencerRowsTicks(source, affectedRows, affectedRowPositions, prevUnits, fwdUnits);
                    influencerPositions = source.invert(influencerRows);
                } else {
                    influencerRows = computeInfluencerRowsTime(source, affectedRows, prevUnits, fwdUnits);
                    affectedRowPositions = null;
                    influencerPositions = null;
                }
            }
            return true;
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
        private long smallestAffectedKey(@NotNull final RowSet added,
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
                    for (int shiftIdx = 0; shiftIdx < shifted.size()
                            && (!modShiftFound || !affectedFound); shiftIdx++) {
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

        private WritableRowSet computeAffectedRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos,
                                                       long fwdNanos) {
            // swap fwd/rev to get the affected windows
            return computeInfluencerRowsTime(sourceSet, subset, fwdNanos, revNanos);
        }

        private WritableRowSet computeInfluencerRowsTime(final RowSet sourceSet, final RowSet subset, long revNanos,
                                                         long fwdNanos) {
            if (sourceSet.size() == subset.size()) {
                return sourceSet.copy();
            }

            int chunkSize = (int) Math.min(subset.size(), 4096);
            try (final RowSequence.Iterator it = subset.getRowSequenceIterator();
                 final ChunkSource.GetContext context = timestampColumnSource.makeGetContext(chunkSize)) {
                final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
                LongSegmentedSortedArray.Iterator ssaIt = timestampSsa.iterator(false, false);
                while (it.hasMore() && ssaIt.hasNext()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(chunkSize);
                    LongChunk<? extends Values> timestamps = timestampColumnSource.getChunk(context, rs).asLongChunk();

                    for (int ii = 0; ii < rs.intSize(); ii++) {
                        // if the timestamp of the row is null, it won't belong to any set and we can ignore it
                        // completely
                        final long ts = timestamps.get(ii);
                        if (ts != NULL_LONG) {
                            // look at every row timestamp, compute the head and tail in nanos
                            final long head = ts - revNanos;
                            final long tail = ts + fwdNanos;

                            // advance the iterator to the beginning of the window
                            if (ssaIt.nextValue() < head) {
                                ssaIt.advanceToBeforeFirst(head);
                                if (!ssaIt.hasNext()) {
                                    // SSA is exhausted
                                    break;
                                }
                            }

                            Assert.assertion(ssaIt.hasNext() && ssaIt.nextValue() >= head,
                                    "SSA Iterator outside of window");

                            // step through the SSA and collect keys until outside of the window
                            while (ssaIt.hasNext() && ssaIt.nextValue() <= tail) {
                                builder.appendKey(ssaIt.nextKey());
                                ssaIt.next();
                            }

                            if (!ssaIt.hasNext()) {
                                // SSA is exhausted
                                break;
                            }
                        }
                    }
                }
                return builder.build();
            }
        }

        private WritableRowSet computeAffectedRowsTicks(final RowSet sourceSet, final RowSet subset, final RowSet invertedSubSet, long revTicks, long fwdTicks, @Nullable final WritableRowSet ) {
            // swap fwd/rev to get the influencer windows
            return computeInfluencerRowsTicks(sourceSet, subset, invertedSubSet, fwdTicks, revTicks);
        }

        private WritableRowSet computeInfluencerRowsTicks(final RowSet sourceSet, final RowSet subset, final RowSet invertedSubSet, long revTicks,
                                                          long fwdTicks) {
            if (sourceSet.size() == subset.size()) {
                return sourceSet.copy();
            }

            long maxPos = sourceSet.size() - 1;

            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            final MutableLong minPos = new MutableLong(0L);

            invertedSubSet.forAllRowKeyRanges((s, e) -> {
                long sPos = Math.max(s - revTicks, minPos.longValue());
                long ePos = Math.min(e + fwdTicks, maxPos);
                builder.appendRange(sPos, ePos);
                minPos.setValue(ePos + 1);
            });

            try (final RowSet positions = builder.build()) {
                return sourceSet.subSetForPositions(positions);
            }
        }

        public RowSet getAffectedRows() {
            return affectedRows;
        }

        public RowSet getInfluencerRows() {
            if (!windowed) {
                return affectedRows;
            }
            return influencerRows;
        }

        @Override
        public void close() {
            if (influencerRows != null && influencerRows != affectedRows) {
                influencerRows.close();
            }
            if (influencerPositions != null && influencerPositions != affectedRowPositions) {
                influencerPositions.close();
            }
            try (final RowSet ignoredRs1 = affectedRows;
                 final RowSet ignoredRs2 = affectedRowPositions;
                 final RowSet ignoredRs3 = newModified) {
            }
        }
    }

    public UpdateByWindowContext makeWindowContext(final ColumnSource<?> timestampColumnSource, final LongSegmentedSortedArray timestampSsa) {
        return new UpdateByWindowContext(timestampColumnSource, timestampSsa);
    }

    private UpdateByWindow(boolean windowed, @Nullable String timestampColumnName, long prevUnits, long fwdUnits) {
        this.windowed = windowed;
        this.timestampColumnName = timestampColumnName;
        this.prevUnits = prevUnits;
        this.fwdUnits = fwdUnits;

        this.operators = new ArrayList<>();
    }

    public static UpdateByWindow createFromOperator(final UpdateByOperator op) {
        return new UpdateByWindow(op instanceof UpdateByWindowedOperator,
                op.getTimestampColumnName(),
                op.getPrevWindowUnits(),
                op.getPrevWindowUnits());
    }

    public void addOperator(UpdateByOperator op) {
        operators.add(op);
    }

    @Nullable
    public String getTimestampColumnName() {
        return timestampColumnName;
    }

    @Override
    public int hashCode() {
        int hash = Boolean.hashCode(windowed);
        if (timestampColumnName != null) {
            hash = 31 * hash + timestampColumnName.hashCode();
        }
        hash = 31 * hash + Long.hashCode(prevUnits);
        hash = 31 * hash + Long.hashCode(fwdUnits);
        return hash;
    }
}
