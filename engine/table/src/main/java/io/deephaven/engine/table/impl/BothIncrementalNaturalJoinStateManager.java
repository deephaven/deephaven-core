//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
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

    WritableRowRedirection buildIndexedRowRedirection(QueryTable leftTable, boolean exactMatch, InitialBuildContext ibc,
            ColumnSource<RowSet> indexRowSets, JoinControl.RedirectionType redirectionType);

    WritableRowRedirection buildRowRedirectionFromRedirections(QueryTable leftTable, boolean exactMatch,
            InitialBuildContext ibc, JoinControl.RedirectionType redirectionType);

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

    void removeLeft(Context pc, RowSequence leftIndex, ColumnSource<?>[] leftSources);

    void applyLeftShift(Context pc, ColumnSource<?>[] leftSources, RowSet shiftedRowSet, long shiftDelta);

    interface InitialBuildContext extends Context {
    }
}
