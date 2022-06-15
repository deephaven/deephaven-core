/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.StaticAsOfJoinStateManager;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import org.jetbrains.annotations.NotNull;

public abstract class StaticHashedAsOfJoinStateManager extends StaticAsOfJoinStateManager {
    protected StaticHashedAsOfJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }

    public abstract int buildFromLeftSide(RowSequence leftRowSet, ColumnSource<?>[] leftSources, @NotNull final LongArraySource addedSlots);
    public abstract int buildFromRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources, @NotNull final LongArraySource addedSlots);

    public abstract void probeLeft(RowSequence leftRowSet, ColumnSource<?>[] leftSources);
    public abstract int probeLeft(RowSequence leftRowSet, ColumnSource<?>[] leftSources, LongArraySource slots, RowSetBuilderRandom foundBuilder);
    public abstract void probeRight(RowSequence rightRowSet, ColumnSource<?>[] rightSources);

    public abstract int getTableSize();
    public abstract int getOverflowSize();
    public abstract RowSet getLeftIndex(long slot);
    public abstract RowSet getRightIndex(long slot);

    public abstract void convertRightBuildersToIndex(LongArraySource slots, int slotCount);
    public abstract void convertRightGrouping(LongArraySource slots, int slotCount, ObjectArraySource<RowSet> rowSetSource);
}
