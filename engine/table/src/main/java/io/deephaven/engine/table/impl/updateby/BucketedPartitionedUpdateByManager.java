package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.updategraph.DynamicNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

/**
 * An implementation of {@link UpdateBy} dedicated to bucketed computation.
 */
class BucketedPartitionedUpdateByManager extends UpdateBy {

    /** The output table for this UpdateBy operation */
    private final QueryTable result;

    /** Listener to react to upstream changes to refreshing source tables */
    private final UpdateByListener sourceListener;

    /** ColumnSet transformer from source to downstream */
    private final ModifiedColumnSet.Transformer mcsTransformer;

    /** Pending failure encountered in a bucket update. */
    private volatile Throwable bucketFailureThrowable;

    /** Entry associated with {@link #bucketFailureThrowable}. */
    private TableListener.Entry bucketFailureSourceEntry;

    /** Listener to the partitioned table used for identifying buckets */
    private final TransformFailureListener transformFailureListener;

    /**
     * Perform a bucketed updateBy using {@code byColumns} as the keys
     *
     * @param windows the unique windows for this UpdateBy, each window contains operators that can share processing
     *        resources
     * @param inputSources the primitive input sources
     * @param source the source table
     * @param preservedColumns columns from the source table that are unchanged in the result table
     * @param resultSources the result sources
     * @param byColumns the columns to use for the bucket keys
     * @param timestampColumnName the column to use for all time-aware operators
     * @param rowRedirection the row redirection for dense output sources
     * @param control the control object.
     */
    protected BucketedPartitionedUpdateByManager(
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final QueryTable source,
            @NotNull final String[] preservedColumns,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @NotNull final Collection<? extends ColumnName> byColumns,
            @Nullable final String timestampColumnName,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {
        super(source, windows, inputSources, timestampColumnName, rowRedirection, control);

        // this table will always have the rowset of the source
        result = new QueryTable(source.getRowSet(), resultSources);

        final String[] byColumnNames = byColumns.stream().map(ColumnName::name).toArray(String[]::new);

        final Table transformedTable = LivenessScopeStack.computeEnclosed(() -> {
            final PartitionedTable partitioned = source.partitionedAggBy(List.of(), true, null, byColumnNames);
            final PartitionedTable transformed = partitioned.transform(t -> {
                final long firstSourceRowKey = t.getRowSet().firstRowKey();
                final String bucketDescription = BucketedPartitionedUpdateByManager.this + "-bucket-" +
                        Arrays.stream(byColumnNames)
                                .map(bcn -> Objects.toString(t.getColumnSource(bcn).get(firstSourceRowKey)))
                                .collect(Collectors.joining(", ", "[", "]"));
                UpdateByBucketHelper bucket = new UpdateByBucketHelper(
                        bucketDescription,
                        (QueryTable) t,
                        windows,
                        resultSources,
                        timestampColumnName,
                        control,
                        this::onBucketFailure);
                // add this to the bucket list
                synchronized (buckets) {
                    buckets.offer(bucket);
                }
                // return the table
                return bucket.result;
            });
            return transformed.table();
        }, source::isRefreshing, DynamicNode::isRefreshing);

        if (source.isRefreshing()) {
            // this is a refreshing source, we will need a listener
            sourceListener = newUpdateByListener();
            source.addUpdateListener(sourceListener);
            // result will depend on listener
            result.addParentReference(sourceListener);

            // create input and output modified column sets
            forAllOperators(op -> {
                op.createInputModifiedColumnSet(source);
                op.createOutputModifiedColumnSet(result);
            });

            // make the source->result transformer from only the columns in the source that are present in result
            mcsTransformer = source.newModifiedColumnSetTransformer(result, preservedColumns);

            // we also need to monitor for failures in bucketing or transformation
            transformFailureListener = new TransformFailureListener(transformedTable);
            transformedTable.addUpdateListener(transformFailureListener);
            result.addParentReference(transformFailureListener);
        } else {
            sourceListener = null;
            mcsTransformer = null;
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
    protected UpdateByListener sourceListener() {
        return sourceListener;
    }

    @Override
    protected ModifiedColumnSet.Transformer mcsTransformer() {
        return mcsTransformer;
    }

    @Override
    protected boolean upstreamSatisfied(final long step) {
        // For bucketed, need to verify the source and the transformed table listener are satisfied.
        return source.satisfied(step) && transformFailureListener.satisfied(step);
    }

    private void onBucketFailure(
            @NotNull final Throwable originalException,
            @Nullable final TableListener.Entry sourceEntry) {
        if (bucketFailureThrowable != null) {
            return;
        }
        synchronized (this) {
            if (bucketFailureThrowable != null) {
                return;
            }
            bucketFailureSourceEntry = sourceEntry;
            bucketFailureThrowable = originalException;
        }
    }

    @Override
    protected boolean maybeDeliverPendingFailure() {
        final Throwable localBucketFailureThrowable = bucketFailureThrowable;
        if (localBucketFailureThrowable != null) {
            deliverUpdateError(localBucketFailureThrowable, bucketFailureSourceEntry, true);
            return true;
        }
        return false;
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
