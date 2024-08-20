//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

public abstract class RightIncrementalNaturalJoinStateManager extends StaticNaturalJoinStateManager
        implements IncrementalNaturalJoinStateManager {

    protected RightIncrementalNaturalJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }

    public abstract void buildFromLeftSide(final Table leftTable, ColumnSource<?>[] leftSources,
            final InitialBuildContext initialBuildContext);

    public abstract void convertLeftDataIndex(int groupingSize, InitialBuildContext initialBuildContext,
            ColumnSource<RowSet> rowSetSource);

    public abstract void addRightSide(RowSequence rightIndex, ColumnSource<?>[] rightSources);

    public abstract WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable, boolean exactMatch,
            InitialBuildContext initialBuildContext, JoinControl.RedirectionType redirectionType);

    public abstract WritableRowRedirection buildRowRedirectionFromHashSlotIndexed(QueryTable leftTable,
            ColumnSource<RowSet> rowSetSource, int groupingSize, boolean exactMatch,
            InitialBuildContext initialBuildContext, JoinControl.RedirectionType redirectionType);

    // modification probes
    public abstract void applyRightShift(Context pc, ColumnSource<?>[] rightSources, RowSet shiftedRowSet,
            long shiftDelta, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    public abstract void modifyByRight(Context pc, RowSet modified, ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    public abstract void removeRight(Context pc, RowSequence rightIndex, ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    public abstract void addRightSide(Context pc, RowSequence rightIndex, ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    public abstract Context makeProbeContext(ColumnSource<?>[] probeSources, long maxSize);

    public abstract InitialBuildContext makeInitialBuildContext(Table leftTable);

    public interface InitialBuildContext extends SafeCloseable {
    }
}
