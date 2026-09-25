//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.StaticAsOfJoinStateManager;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import org.jetbrains.annotations.NotNull;

public abstract class StaticHashedAsOfJoinStateManager extends StaticAsOfJoinStateManager {

    protected StaticHashedAsOfJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }

    public abstract int buildFromLeftSide(RowSequence leftRowSet, ColumnSource<?>[] leftSources,
            @NotNull IntegerArraySource addedSlots);

    public abstract int buildFromRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull IntegerArraySource addedSlots);

    public abstract void probeLeft(RowSequence leftRowSet, ColumnSource<?>[] leftSources);

    public abstract int probeLeft(RowSequence leftRowSet, ColumnSource<?>[] leftSources, IntegerArraySource slots);

    public abstract void probeRight(RowSequence rightRowSet, ColumnSource<?>[] rightSources);

    public abstract int getTableSize();

    public abstract RowSet getLeftRowSet(int slot);

    /**
     * @param slot the slot in the table
     * @return the right row set of the slot, or null if the slot has no right rows or its row set has been released
     */
    public abstract RowSet getRightRowset(int slot);

    /**
     * Release the right row set of a slot once the caller no longer needs it. Afterwards {@link #getRightRowset(int)}
     * returns null for the slot, which remains in the table for probing.
     *
     * @param slot the slot in the table
     */
    public abstract void releaseRightRowSet(int slot);

    public abstract void convertRightBuildersToRowSet(IntegerArraySource slots, int slotCount);

    public abstract void populateRightRowSetsFromIndexTable(IntegerArraySource slots, int slotCount,
            ColumnSource<RowSet> rowSetSource);
}
