package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.updategraph.LogicalClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * An implementation of {@link UpdateBy} dedicated to bucketed computation.
 */
class BucketedPartitionedUpdateByManager extends UpdateBy {

    /** The output table for this UpdateBy operation */
    final QueryTable result;

    /** Listener to the partitioned table used for identifying buckets */
    final TransformFailureListener transformFailureListener;

    /**
     * Perform a bucketed updateBy using {@code byColumns} as the keys
     *
     * @param description the operation description
     * @param operators the operations to perform
     * @param windows the unique windows for this UpdateBy
     * @param inputSources the primitive input sources
     * @param source the source table
     * @param resultSources the result sources
     * @param byColumns the columns to use for the bucket keys
     * @param timestampColumnName the column to use for all time-aware operators
     * @param rowRedirection the row redirection for dense output sources
     * @param control the control object.
     */
    protected BucketedPartitionedUpdateByManager(
            @NotNull final String description,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final QueryTable source,
            final String[] persistentColumns,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final Collection<? extends ColumnName> byColumns,
            @Nullable final String timestampColumnName,
            @Nullable final WritableRowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {
        super(source, operators, windows, inputSources, timestampColumnName, rowRedirection, control);

        // this table will always have the rowset of the source
        result = new QueryTable(source.getRowSet(), resultSources);

        final String[] byColumnNames = byColumns.stream().map(ColumnName::name).toArray(String[]::new);

        final PartitionedTable pt;
        if (source.isRefreshing()) {
            // this is a refreshing source, we will need a listener
            listener = newUpdateByListener();
            source.addUpdateListener(listener);
            // result will depend on listener
            result.addParentReference(listener);

            // create input and output modified column sets
            for (UpdateByOperator op : operators) {
                op.createInputModifiedColumnSet(source);
                op.createOutputModifiedColumnSet(result);
            }
            pt = source.partitionedAggBy(List.of(), true, null, byColumnNames);

            // make the source->result transformer from only the columns in the source that are present in result
            transformer = source.newModifiedColumnSetTransformer(result, persistentColumns);
        } else {
            pt = source.partitionedAggBy(List.of(), true, null, byColumnNames);
        }

        final PartitionedTable transformed = pt.transform(t -> {
            final long firstSourceRowKey = t.getRowSet().firstRowKey();
            final String bucketDescription = BucketedPartitionedUpdateByManager.this + "-bucket-" +
                    Arrays.stream(byColumnNames)
                            .map(bcn -> Objects.toString(t.getColumnSource(bcn).get(firstSourceRowKey)))
                            .collect(Collectors.joining(", ", "[", "]"));
            UpdateByBucketHelper bucket = new UpdateByBucketHelper(
                    bucketDescription,
                    (QueryTable) t,
                    windows,
                    inputSources,
                    resultSources,
                    timestampColumnName,
                    control,
                    (oe, se) -> deliverUpdateError(oe, se, true));

            bucket.parentUpdateBy = this;
            bucket.createdStep = LogicalClock.DEFAULT.currentStep();

            // add this to the bucket list
            synchronized (buckets) {
                buckets.offer(bucket);
            }
            // return the table
            return bucket.result;
        });

        if (source.isRefreshing()) {
            final Table transformedTable = transformed.table();
            transformFailureListener = new TransformFailureListener(transformedTable);
            transformedTable.addUpdateListener(transformFailureListener);
            result.addParentReference(transformFailureListener);
        } else {
            transformFailureListener = null;
        }

        // make a dummy update to generate the initial row keys
        final TableUpdateImpl fakeUpdate = new TableUpdateImpl(
                source.getRowSet().copy(), // send a copy since this will be closed by release()
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
        // For bucketed, need to verify the source and the transformed table listener are satisfied.
        return source.satisfied(step) && transformFailureListener.satisfied(step);
    }

    private final class TransformFailureListener extends InstrumentedTableUpdateListenerAdapter {

        private TransformFailureListener(@NotNull final Table transformed) {
            super(BucketedPartitionedUpdateByManager.this + "-TransformFailureListener", transformed, false);
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            // No-op: We react to bucket creation inside the transform function, no need to do anything here.
            // Validation: We expect only adds, because the partitioned table was created by partitionedAggBy with
            // preserveEmpty==true
            Assert.assertion(upstream.removed().isEmpty(), "upstream.removed().isEmpty()");
            Assert.assertion(upstream.modified().isEmpty(), "upstream.modified().isEmpty()");
            Assert.assertion(upstream.shifted().empty(), "upstream.shifted().empty()");
        }

        @Override
        public void onFailureInternal(@NotNull final Throwable originalException, @Nullable final Entry sourceEntry) {
            deliverUpdateError(originalException, sourceEntry, true);
        }
    }
}
