/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.RightIncrementalAsOfJoinStateManager;
import io.deephaven.engine.table.impl.StaticAsOfJoinStateManager;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.function.LongUnaryOperator;

public abstract class RightIncrementalHashedAsOfJoinStateManager extends RightIncrementalAsOfJoinStateManager {
    protected RightIncrementalHashedAsOfJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }

    public static final byte ENTRY_RIGHT_MASK = 0x3;
    public static final byte ENTRY_RIGHT_IS_EMPTY = 0x0;
    public static final byte ENTRY_RIGHT_IS_BUILDER = 0x1;
    public static final byte ENTRY_RIGHT_IS_SSA = 0x2;
    public static final byte ENTRY_RIGHT_IS_INDEX = 0x3;

    public static final byte ENTRY_LEFT_MASK = 0x30;
    public static final byte ENTRY_LEFT_IS_EMPTY = 0x00;
    public static final byte ENTRY_LEFT_IS_BUILDER = 0x10;
    public static final byte ENTRY_LEFT_IS_SSA = 0x20;
    public static final byte ENTRY_LEFT_IS_INDEX = 0x30;

    public static final byte ENTRY_INITIAL_STATE_LEFT = ENTRY_LEFT_IS_BUILDER|ENTRY_RIGHT_IS_EMPTY;
    public static final byte ENTRY_INITIAL_STATE_RIGHT = ENTRY_LEFT_IS_EMPTY|ENTRY_RIGHT_IS_BUILDER;

    protected void addToSequentialBuilder(long slot, @NotNull ObjectArraySource<RowSetBuilderSequential> sequentialBuilders, long indexKey) {
        RowSetBuilderSequential builder = sequentialBuilders.getUnsafe(slot);
        if (builder == null) {
            builder = RowSetFactory.builderSequential();
            sequentialBuilders.set(slot, builder);
        }
        builder.appendKey(indexKey);
    }

    protected byte leftEntryAsRightType(byte entryType) {
        return (byte)((entryType & ENTRY_LEFT_MASK) >> 4);
    }

    protected byte getRightEntryType(byte entryType) {
        return (byte)(entryType & ENTRY_RIGHT_MASK);
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
    public abstract WritableRowSet getLeftIndex(long slot);
    public abstract WritableRowSet getRightIndex(long slot);

    public abstract WritableRowSet getAndClearLeftIndex(long slot);

    public abstract int markForRemoval(RowSet restampRemovals, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract int gatherShiftIndex(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract int gatherModifications(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots, ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    public abstract byte getState(long slot);

    public abstract int getTableSize();
    public abstract int getOverflowSize();
}
