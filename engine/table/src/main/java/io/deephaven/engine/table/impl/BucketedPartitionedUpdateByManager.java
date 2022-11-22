package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.updateby.UpdateByWindow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * An implementation of {@link UpdateBy} dedicated to bucketed computation.
 */
class BucketedPartitionedUpdateByManager extends UpdateBy {
    /**
     * Perform a bucketed updateBy using {@code byColumns} as the keys
     *
     * @param description the operation description
     * @param source the source table
     * @param ops the operations to perform
     * @param resultSources the result sources
     * @param byColumns the columns to use for the bucket keys
     * @param redirContext the row redirection shared context
     * @param control the control object.
     * @return the result table
     */
    public static Table compute(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] ops,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final int[][] operatorInputSourceSlots,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final Collection<? extends ColumnName> byColumns,
            @Nullable final String timestampColumnName,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control) {

        final BucketedPartitionedUpdateByManager updateBy = new BucketedPartitionedUpdateByManager(description,
                ops,
                windows,
                inputSources,
                operatorInputSourceSlots,
                source,
                resultSources,
                byColumns,
                timestampColumnName,
                redirContext,
                control);

        return updateBy.result;
    }

    protected BucketedPartitionedUpdateByManager(@NotNull final String description,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final int[][] operatorInputSourceSlots,
            @NotNull final QueryTable source,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final Collection<? extends ColumnName> byColumns,
            @Nullable final String timestampColumnName,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control) {
        super(description, source, operators, windows, inputSources, operatorInputSourceSlots, resultSources,
                timestampColumnName, redirContext, control);

        // this table will always have the rowset of the source
        result = new QueryTable(source.getRowSet(), resultSources);

        final PartitionedTable pt;
        if (source.isRefreshing()) {
            // this is a refreshing source, we will need a listener and recorders
            recorders = new LinkedList<>();
            listener = newListener(description);

            // create a recorder instance sourced from the source table
            ListenerRecorder sourceRecorder = new ListenerRecorder(description, source, result);
            sourceRecorder.setMergedListener(listener);
            source.addUpdateListener(sourceRecorder);
            result.addParentReference(listener);
            recorders.offerLast(sourceRecorder);

            // create input and output modified column sets
            for (UpdateByOperator op : operators) {
                op.createInputModifiedColumnSet(source);
                op.createOutputModifiedColumnSet(result);
            }
            pt = source.partitionedAggBy(List.of(), true, null, byColumns);
        } else {
            // no shifting will be needed, can create directly from source
            pt = source.partitionedAggBy(List.of(), true, null, byColumns);

            // create input modified column sets only
            for (UpdateByOperator op : operators) {
                op.createInputModifiedColumnSet(source);
            }
        }

        // make the source->result transformer
        transformer = source.newModifiedColumnSetTransformer(result, source.getDefinition().getColumnNamesArray());

        final PartitionedTable transformed = pt.transform(t -> {
            UpdateByBucketHelper updateBy = new UpdateByBucketHelper(
                    description,
                    (QueryTable) t,
                    operators,
                    windows,
                    inputSources,
                    operatorInputSourceSlots,
                    resultSources,
                    timestampColumnName,
                    redirContext,
                    control);

            if (listener != null) {
                ListenerRecorder recorder = new ListenerRecorder(description, updateBy.result, result);
                recorder.setMergedListener(listener);
                updateBy.result.addUpdateListener(recorder);

                // add the listener only while synchronized
                synchronized (recorders) {
                    recorders.offerLast(recorder);
                }
            }
            // add this to the bucket list
            synchronized (buckets) {
                buckets.offerLast(updateBy);
            }
            // return the table
            return updateBy.result;
        });

        result.addParentReference(transformed);

        if (source.isRefreshing()) {
            // create a recorder instance sourced from the transformed table
            ListenerRecorder sourceRecorder = new ListenerRecorder(description, transformed.table(), result);
            sourceRecorder.setMergedListener(listener);
            transformed.table().addUpdateListener(sourceRecorder);
            result.addParentReference(listener);
            recorders.offerLast(sourceRecorder);
        }


        // make a dummy update to generate the initial row keys
        final TableUpdateImpl fakeUpdate = new TableUpdateImpl(source.getRowSet(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);

        // do the actual computations
        final StateManager sm = new StateManager(fakeUpdate, true);
        sm.processUpdate();
    }
}
