package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

public abstract class RightIncrementalNaturalJoinStateManager extends StaticNaturalJoinStateManager implements IncrementalNaturalJoinStateManager {
    protected RightIncrementalNaturalJoinStateManager(ColumnSource<?>[] keySourcesForErrorMessages) {
        super(keySourcesForErrorMessages);
    }

    abstract void buildFromLeftSide(final Table leftTable, ColumnSource<?>[] leftSources, final InitialBuildContext initialBuildContext);
    abstract void convertLeftGroups(int groupingSize, InitialBuildContext initialBuildContext, ObjectArraySource<WritableRowSet> rowSetSource);
    abstract void addRightSide(RowSequence rightIndex, ColumnSource<?> [] rightSources);

    abstract WritableRowRedirection buildRowRedirectionFromHashSlot(QueryTable leftTable, boolean exactMatch, InitialBuildContext initialBuildContext, JoinControl.RedirectionType redirectionType);
    abstract WritableRowRedirection buildRowRedirectionFromHashSlotGrouped(QueryTable leftTable, ObjectArraySource<WritableRowSet> rowSetSource, int groupingSize, boolean exactMatch, InitialBuildContext initialBuildContext, JoinControl.RedirectionType redirectionType);

    // modification probes
    abstract void applyRightShift(Context pc, ColumnSource<?> [] rightSources, RowSet shiftedRowSet, long shiftDelta, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);
    abstract void modifyByRight(Context pc, RowSet modified, ColumnSource<?>[] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);
    abstract void removeRight(Context pc, RowSequence rightIndex, ColumnSource<?> [] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);
    abstract void addRightSide(Context pc, RowSequence rightIndex, ColumnSource<?> [] rightSources, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    abstract public Context makeProbeContext(ColumnSource<?>[] probeSources, long maxSize);

    abstract public InitialBuildContext makeInitialBuildContext(Table leftTable);

    public interface InitialBuildContext extends SafeCloseable {
    }
}
