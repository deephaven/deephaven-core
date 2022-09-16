package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.UnionSourceManager;
import io.deephaven.engine.table.impl.updateby.UpdateByWindow;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * An implementation of {@link UpdateBy} dedicated to zero key computation.
 */
class BucketedPartitionedUpdateBy extends UpdateBy {
    private final BucketedPartitionedUpdateByListener listener;
    private final LinkedList<BucketedPartitionedUpdateByListenerRecorder> recorders;
    private final QueryTable resultTable;

    /**
     * Perform an updateBy without any key columns.
     *
     * @param description the operation description
     * @param source the source table
     * @param ops the operations to perform
     * @param resultSources the result sources
     * @param redirContext the row redirection shared context
     * @param control the control object.
     * @return the result table
     */
    public static Table compute(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final UpdateByOperator[] ops,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final Collection<? extends ColumnName> byColumns,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control) {

        final BucketedPartitionedUpdateBy updateBy = new BucketedPartitionedUpdateBy(description,
                windows,
                ops,
                source,
                resultSources,
                byColumns,
                redirContext,
                control);

        return updateBy.resultTable;
    }

    protected BucketedPartitionedUpdateBy(@NotNull final String description,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final QueryTable source,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final Collection<? extends ColumnName> byColumns,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control) {
        super(windows, operators, source, redirContext, control);

        // create a source-listener that will listen to the source updates and apply the shifts to the output columns
        final QueryTable sourceListenerTable = new QueryTable(source.getRowSet(), source.getColumnSourceMap());

        // this table will always have the rowset of the source
        resultTable = new QueryTable(source.getRowSet(), resultSources);

        if (source.isRefreshing()) {
            source.listenForUpdates(new BaseTable.ListenerImpl("", source, sourceListenerTable) {
                @Override
                public void onUpdate(@NotNull final TableUpdate upstream) {
                    if (redirContext.isRedirected()) {
                        redirContext.processUpdateForRedirection(upstream, source.getRowSet());
                    } else if (upstream.shifted().nonempty()) {
                        try (final RowSet prevIdx = source.getRowSet().copyPrev()) {
                            upstream.shifted().apply((begin, end, delta) -> {
                                try (final RowSet subRowSet = prevIdx.subSetByKeyRange(begin, end)) {
                                    for (int opIdx = 0; opIdx < operators.length; opIdx++) {
                                        operators[opIdx].applyOutputShift(null, subRowSet, delta);
                                    }
                                }
                            });
                        }
                    }
                    super.onUpdate(upstream);
                }
            });

            recorders = new LinkedList<>();
            listener = newListener(description);

            // create a listener and recorder for the source table as first entry
            BucketedPartitionedUpdateByListenerRecorder recorder =
                    new BucketedPartitionedUpdateByListenerRecorder(description, source, resultTable);
            recorder.setMergedListener(listener);
            source.listenForUpdates(recorder);

            recorders.offerLast(recorder);
        } else {
            listener = null;
            recorders = null;
        }

        if (redirContext.isRedirected()) {
            // make a dummy update to generate the initial row keys
            final TableUpdateImpl fakeUpdate = new TableUpdateImpl(source.getRowSet(),
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.ALL);
            redirContext.processUpdateForRedirection(fakeUpdate, source.getRowSet());
        }

        final PartitionedTable pt = sourceListenerTable.partitionedAggBy(List.of(), true, null, byColumns);
        final PartitionedTable transformed = pt.transform(t -> {
            // create the table
            Table newTable = ZeroKeyUpdateBy.compute(
                    description,
                    (QueryTable) t,
                    windows,
                    operators,
                    resultSources,
                    redirContext,
                    control,
                    false);

            if (listener != null) {
                BucketedPartitionedUpdateByListenerRecorder recorder =
                        new BucketedPartitionedUpdateByListenerRecorder(description, newTable, resultTable);
                recorder.setMergedListener(listener);
                newTable.listenForUpdates(recorder);

                // add the listener only while synchronized
                synchronized (recorders) {
                    recorders.offerLast(recorder);
                }
            }

            // return the table
            return newTable;
        });
        resultTable.addParentReference(transformed);
    }

    BucketedPartitionedUpdateByListener newListener(@NotNull final String description) {
        return new BucketedPartitionedUpdateByListener(description);
    }

    private final class BucketedPartitionedUpdateByListenerRecorder extends ListenerRecorder {

        private final ModifiedColumnSet.Transformer modifiedColumnsTransformer;

        BucketedPartitionedUpdateByListenerRecorder(@NotNull String description, @NotNull final Table constituent,
                @NotNull final Table dependent) {
            super(description, constituent, dependent);
            modifiedColumnsTransformer = ((QueryTable) constituent).newModifiedColumnSetTransformer(
                    (QueryTable) dependent, constituent.getDefinition().getColumnNamesArray());
        }
    }

    /**
     * The Listener for apply to the constituent table updates
     */
    class BucketedPartitionedUpdateByListener extends MergedListener {
        private final ModifiedColumnSet[] inputModifiedColumnSets;
        private final ModifiedColumnSet[] outputModifiedColumnSets;

        public BucketedPartitionedUpdateByListener(@Nullable String description) {
            super(recorders, List.of(), description, resultTable);

            this.inputModifiedColumnSets = new ModifiedColumnSet[operators.length];
            this.outputModifiedColumnSets = new ModifiedColumnSet[operators.length];

            for (int ii = 0; ii < operators.length; ii++) {
                final String[] outputColumnNames = operators[ii].getOutputColumnNames();
                inputModifiedColumnSets[ii] = source.newModifiedColumnSet(operators[ii].getAffectingColumnNames());
                outputModifiedColumnSets[ii] = resultTable.newModifiedColumnSet(outputColumnNames);
            }
        }

        @Override
        protected void process() {
            final TableUpdateImpl downstream = new TableUpdateImpl();

            // get the adds/removes/shifts from the first (source) entry, make a copy since TableUpdateImpl#reset will
            // close them with the upstream update
            ListenerRecorder sourceRecorder = recorders.peekFirst();
            downstream.added = sourceRecorder.getAdded().copy();
            downstream.removed = sourceRecorder.getRemoved().copy();
            downstream.shifted = sourceRecorder.getShifted();

            // union the modifies from all the tables (including source)
            downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
            downstream.modifiedColumnSet.clear();

            WritableRowSet modifiedRowSet = RowSetFactory.empty();
            downstream.modified = modifiedRowSet;

            recorders.forEach(lr -> {
                if (lr.getModified().isNonempty()) {
                    modifiedRowSet.insert(lr.getModified());
                }
                // Transform any untouched modified columns to the output.
                lr.modifiedColumnsTransformer.transform(lr.getModifiedColumnSet(), downstream.modifiedColumnSet);

            });
            // should not include actual adds as modifies
            modifiedRowSet.remove(downstream.added);

            result.notifyListeners(downstream);
        }

        @Override
        protected boolean canExecute(final long step) {
            synchronized (recorders) {
                return recorders.stream().allMatch(lr -> lr.satisfied(step));
            }
        }
    }
}
