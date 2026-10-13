//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.RightIncrementalAsOfJoinStateManager;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public abstract class RightIncrementalHashedAsOfJoinStateManager extends RightIncrementalAsOfJoinStateManager {
    protected RightIncrementalHashedAsOfJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }

    public static final byte ENTRY_RIGHT_MASK = 0x3;
    public static final byte ENTRY_RIGHT_IS_EMPTY = 0x0;
    public static final byte ENTRY_RIGHT_IS_BUILDER = 0x1;
    public static final byte ENTRY_RIGHT_IS_SSA = 0x2;
    public static final byte ENTRY_RIGHT_IS_ROWSET = 0x3;

    public static final byte ENTRY_LEFT_MASK = 0x30;
    public static final byte ENTRY_LEFT_IS_EMPTY = 0x00;
    public static final byte ENTRY_LEFT_IS_BUILDER = 0x10;
    public static final byte ENTRY_LEFT_IS_SSA = 0x20;
    public static final byte ENTRY_LEFT_IS_ROWSET = 0x30;

    protected void addToSequentialBuilder(long slot,
            @NotNull ObjectArraySource<RowSetBuilderSequential> sequentialBuilders, long indexKey) {
        RowSetBuilderSequential builder = sequentialBuilders.getUnsafe(slot);
        if (builder == null) {
            builder = RowSetFactory.builderSequential();
            sequentialBuilders.set(slot, builder);
        }
        builder.appendKey(indexKey);
    }

    /**
     * The number of live buckets, which bounds the number of distinct slots any probe can report and therefore the
     * capacity the per-slot output arrays need. A bucket released by {@link #releaseEmptyBuckets()} is not counted.
     *
     * @return the number of live buckets this manager holds
     */
    public abstract long getNumEntries();

    /**
     * Reserve room for {@code additionalCandidates} more calls to {@link #addTombstoneCandidate(int)} in this cycle.
     *
     * @param additionalCandidates the number of candidates that may be added
     */
    public abstract void ensureTombstoneCandidateCapacity(int additionalCandidates);

    /**
     * Record a slot whose left or right side may have become empty during this cycle, so that
     * {@link #releaseEmptyBuckets()} can release it. Room for the candidate must already have been reserved with
     * {@link #ensureTombstoneCandidateCapacity(int)}.
     *
     * @param slot the slot, as reported by a build or probe of this cycle
     */
    public abstract void addTombstoneCandidate(int slot);

    /**
     * Release every candidate bucket whose left and right sides are both empty, replacing it with a tombstone that a
     * later build may reuse. The slot numbers reported by this cycle's builds and probes must no longer be in use, as a
     * released slot may hold a different key after the next build.
     */
    public abstract void releaseEmptyBuckets();

    protected byte leftEntryAsRightType(byte entryType) {
        return (byte) ((entryType & ENTRY_LEFT_MASK) >> 4);
    }

    protected byte getRightEntryType(byte entryType) {
        return (byte) (entryType & ENTRY_RIGHT_MASK);
    }

    public abstract int buildFromLeftSide(RowSequence leftRowSet, ColumnSource<?>[] leftSources,
            @NotNull IntegerArraySource addedSlots);

    public abstract int buildFromRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull IntegerArraySource addedSlots, int usedSlots);

    public abstract void probeRightInitial(RowSequence rowsToProbe, ColumnSource<?>[] rightSources);

    public abstract int probeAdditions(RowSet restampAdditions, ColumnSource<?>[] sources, IntegerArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract int buildAdditions(boolean isLeftSide, RowSet additions, ColumnSource<?>[] sources,
            IntegerArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract SegmentedSortedArray getRightSsa(int slot);

    public abstract SegmentedSortedArray getRightSsa(int slot, Function<RowSet, SegmentedSortedArray> ssaFactory);

    public abstract SegmentedSortedArray getLeftSsa(int slot);

    public abstract SegmentedSortedArray getLeftSsa(int slot, Function<RowSet, SegmentedSortedArray> ssaFactory);

    public abstract SegmentedSortedArray getLeftSsaOrRowSet(int slot, MutableObject<WritableRowSet> indexOutput);

    public abstract SegmentedSortedArray getRightSsaOrRowSet(int slot, MutableObject<WritableRowSet> indexOutput);

    public abstract void setRightRowSet(int slot, RowSet rowSet);

    public abstract void setLeftRowSet(int slot, RowSet rowSet);

    public abstract WritableRowSet getLeftRowSet(int slot);

    public abstract WritableRowSet getRightRowSet(int slot);

    public abstract void populateRightRowSetsFromIndexTable(IntegerArraySource slots, int slotCount,
            ColumnSource<RowSet> rowSetSource);

    public abstract void populateLeftRowSetsFromIndexTable(IntegerArraySource slots, int slotCount,
            ColumnSource<RowSet> rowSetSource);

    public abstract WritableRowSet getAndClearLeftRowSet(int slot);

    public abstract int markForRemoval(RowSet restampRemovals, ColumnSource<?>[] sources, IntegerArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract int gatherShiftRowSet(RowSet restampAdditions, ColumnSource<?>[] sources, IntegerArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    /**
     * Make a probe context for
     * {@link #gatherShiftRowSet(Context, RowSet, ColumnSource[], IntegerArraySource, ObjectArraySource)}.
     *
     * @param probeSources the key sources that will be probed
     * @param maxSize the largest row set that will be probed with the context
     * @return a context the caller must close
     */
    public abstract Context makeProbeContext(ColumnSource<?>[] probeSources, long maxSize);

    /**
     * Gather the rows of {@code rowSet} by slot, as
     * {@link #gatherShiftRowSet(RowSet, ColumnSource[], IntegerArraySource, ObjectArraySource)} does, reusing a context
     * from {@link #makeProbeContext} so that a caller probing many row sets in one cycle allocates it once.
     */
    public abstract int gatherShiftRowSet(Context probeContext, RowSet rowSet, ColumnSource<?>[] sources,
            IntegerArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract int gatherModifications(RowSet restampAdditions, ColumnSource<?>[] sources,
            IntegerArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract byte getState(int slot);

    public abstract int getTableSize();
}
