package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.RightIncrementalAsOfJoinStateManager;
import io.deephaven.engine.table.impl.StaticAsOfJoinStateManager;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.function.LongUnaryOperator;

public abstract class RightIncrementalHashedAsOfJoinStateManager extends RightIncrementalAsOfJoinStateManager {
    protected RightIncrementalHashedAsOfJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }

    public abstract int buildFromLeftSide(RowSequence leftRowSet, ColumnSource<?>[] leftSources, @NotNull final LongArraySource addedSlots);
    public abstract int buildFromRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources, @NotNull final LongArraySource addedSlots, int usedSlots);

    public abstract void probeRightInitial(RowSequence rightIndex, ColumnSource<?>[] rightSources);
    public abstract int probeAdditions(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);
    public abstract int buildAdditions(boolean isLeftSide, RowSet additions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract SegmentedSortedArray getRightSsa(long slot);
    public abstract SegmentedSortedArray getRightSsa(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory);
    public abstract SegmentedSortedArray getLeftSsa(long slot);
    public abstract SegmentedSortedArray getLeftSsa(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory);

    public abstract SegmentedSortedArray getLeftSsaOrIndex(long slot, MutableObject<WritableRowSet> indexOutput);
    public abstract SegmentedSortedArray getRightSsaOrIndex(long slot, MutableObject<WritableRowSet> indexOutput);
    public abstract void setRightIndex(long slot, RowSet rowSet);
    public abstract void setLeftIndex(long slot, RowSet rowSet);

    public abstract WritableRowSet getAndClearLeftIndex(long slot);

    public abstract int markForRemoval(RowSet restampRemovals, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract int gatherShiftIndex(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract int gatherModifications(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract byte getState(long slot);

    public abstract int getTableSize();
    public abstract int getOverflowSize();
    public abstract WritableRowSet getLeftIndex(long slot);
    public abstract WritableRowSet getRightIndex(long slot);
}
