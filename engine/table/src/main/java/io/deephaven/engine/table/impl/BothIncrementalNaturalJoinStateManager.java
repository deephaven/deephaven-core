//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;

public interface BothIncrementalNaturalJoinStateManager extends IncrementalNaturalJoinStateManager {
    InitialBuildContext makeInitialBuildContext();

    void buildFromRightSide(final Table rightTable, ColumnSource<?>[] rightSources);

    void decorateLeftSide(RowSet leftRowSet, ColumnSource<?>[] leftSources, InitialBuildContext ibc);

    void compactAll();

    WritableRowRedirection buildIndexedRowRedirection(QueryTable leftTable, InitialBuildContext ibc,
            ColumnSource<RowSet> indexRowSets, JoinControl.RedirectionType redirectionType);

    WritableRowRedirection buildRowRedirectionFromRedirections(QueryTable leftTable, InitialBuildContext ibc,
            JoinControl.RedirectionType redirectionType);

    Context makeProbeContext(ColumnSource<?>[] probeSources, long maxSize);

    Context makeBuildContext(ColumnSource<?>[] buildSources, long maxSize);

    void addRightSide(Context bc, RowSequence rightIndex, ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    void removeRight(final Context pc, RowSequence rightIndex, ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    void modifyByRight(Context pc, RowSet modified, ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    void applyRightShift(Context pc, ColumnSource<?>[] rightSources, RowSet shiftedRowSet, long shiftDelta,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    void addLeftSide(final Context bc, RowSequence leftIndex, ColumnSource<?>[] leftSources,
            LongArraySource leftRedirections, NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    void removeLeft(Context pc, RowSequence leftIndex, ColumnSource<?>[] leftSources,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    /**
     * Move the left rows of one shift range within their slots' left row sets.
     *
     * @param pc the probe context
     * @param leftSources the left key sources
     * @param shiftedRowSet the post-shift row keys of the shifted left rows
     * @param shiftDelta the shift range's delta
     * @param modifiedSlotTracker the tracker, whose per-slot builders accumulate the shifted keys of each slot
     */
    void applyLeftShift(Context pc, ColumnSource<?>[] leftSources, RowSet shiftedRowSet, long shiftDelta,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    /**
     * In a single pass over the modified left rows, determine which rows' key value actually changed (by comparing the
     * current key values at the post-shift row keys with the previous key values at the pre-shift row keys), remove
     * those changed rows from their previous-key hash slots, and report the changed keys. Rows whose key value is
     * unchanged are collapsed away entirely, so the caller avoids the hash lookups and per-key row set churn of
     * removing and re-adding them. The previous key values are read only once: the same values used for the equality
     * test are reused to drive the removal.
     *
     * @param leftSources the left key sources
     * @param modifiedPreShift the modified rows, in pre-shift key space, aligned positionally with
     *        {@code modifiedPostShift}
     * @param modifiedPostShift the modified rows, in post-shift key space
     * @param changedPreShift output, ascending, receives the pre-shift keys whose key value changed, aligned with
     *        {@code changedPostShift} (for the caller's redirection removal)
     * @param changedPostShift output, ascending, receives the post-shift keys whose key value changed (to be re-added
     *        by the caller)
     */
    void removeLeftModifications(ColumnSource<?>[] leftSources, RowSet modifiedPreShift, RowSet modifiedPostShift,
            RowSetBuilderSequential changedPreShift, RowSetBuilderSequential changedPostShift,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    /**
     * The right-side counterpart of {@link #removeLeftModifications}: in a single pass over the modified right rows,
     * determine which rows' key value actually changed, remove those rows from their previous-key hash slots, and
     * report the changed keys. Rows whose key value is unchanged keep their slot.
     *
     * @param rightSources the right key sources
     * @param modifiedPreShift the modified rows, in pre-shift key space, aligned positionally with
     *        {@code modifiedPostShift}
     * @param modifiedPostShift the modified rows, in post-shift key space
     * @param changedPreShift output, ascending, receives the pre-shift keys whose key value changed (to be excluded
     *        from the caller's shift processing)
     * @param changedPostShift output, ascending, receives the post-shift keys whose key value changed (to be re-added
     *        by the caller)
     */
    void removeRightModifications(ColumnSource<?>[] rightSources, RowSet modifiedPreShift, RowSet modifiedPostShift,
            RowSetBuilderSequential changedPreShift, RowSetBuilderSequential changedPostShift,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    interface InitialBuildContext extends Context {
    }
}
