package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.Pair;
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
    /** The output table for this UpdateBy operation */
    final QueryTable result;

    /** The partitioned table used for identifying buckets */
    final Table transformedTable;

    /**
     * Perform a bucketed updateBy using {@code byColumns} as the keys
     *
     * @param description the operation description
     * @param operators the operations to perform
     * @param windows the unique windows for this UpdateBy
     * @param inputSources the primitive input sources
     * @param operatorInputSourceSlots maps the operators to source indices
     * @param source the source table
     * @param resultSources the result sources
     * @param byColumns the columns to use for the bucket keys
     * @param timestampColumnName the column to use for all time-aware operators
     * @param redirHelper the row redirection helper for dense output sources
     * @param control the control object.
     */
    protected BucketedPartitionedUpdateByManager(
            @NotNull final String description,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final int[][] operatorInputSourceSlots,
            @NotNull final QueryTable source,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final Collection<? extends ColumnName> byColumns,
            @Nullable final String timestampColumnName,
            @NotNull final UpdateByRedirectionHelper redirHelper,
            @NotNull final UpdateByControl control) {
        super(source, operators, windows, inputSources, operatorInputSourceSlots, timestampColumnName, redirHelper,
                control);

        // this table will always have the rowset of the source
        result = new QueryTable(source.getRowSet(), resultSources);

        final PartitionedTable pt;
        if (source.isRefreshing()) {
            // this is a refreshing source, we will need a listener
            listener = newUpdateByListener(description);
            source.addUpdateListener(listener);
            // result will depend on listener
            result.addParentReference(listener);

            // create input and output modified column sets
            for (UpdateByOperator op : operators) {
                op.createInputModifiedColumnSet(source);
                op.createOutputModifiedColumnSet(result);
            }
            pt = source.partitionedAggBy(List.of(), true, null, byColumns);

            // make the source->result transformer
            transformer = source.newModifiedColumnSetTransformer(result, source.getDefinition().getColumnNamesArray());
        } else {
            pt = source.partitionedAggBy(List.of(), true, null, byColumns);
        }

        final PartitionedTable transformed = pt.transform(t -> {
            UpdateByBucketHelper bucket = new UpdateByBucketHelper(
                    description,
                    (QueryTable) t,
                    operators,
                    windows,
                    inputSources,
                    operatorInputSourceSlots,
                    resultSources,
                    timestampColumnName,
                    redirHelper,
                    control);

            // add this to the bucket list
            synchronized (buckets) {
                buckets.offer(bucket);
            }
            // return the table
            return bucket.result;
        });

        if (source.isRefreshing()) {
            transformedTable = transformed.table();

            // result also depends on the transformedTable
            result.addParentReference(transformedTable);
        } else {
            transformedTable = null;
        }

        // make a dummy update to generate the initial row keys
        final TableUpdateImpl fakeUpdate = new TableUpdateImpl(source.getRowSet(),
                RowSetFactory.empty(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);

        // do the actual computations
        final PhasedUpdateProcessor sm = new PhasedUpdateProcessor(fakeUpdate, true);
        sm.processUpdate();
    }

    @Override
    protected QueryTable result() {
        return result;
    }

    @Override
    protected boolean upstreamSatisfied(final long step) {
        // For bucketed, need to verify the source and the transformed table is satisfied.
        if (transformedTable == null) {
            return false;
        }
        return source.satisfied(step) && transformedTable.satisfied(step);
    }

}
