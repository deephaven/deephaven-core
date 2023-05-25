package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public class ZeroKeyUpdateByManager extends UpdateBy {

    /** The output table for this UpdateBy operation */
    private final QueryTable result;

    /** Listener to react to upstream changes to refreshing source tables */
    private final UpdateByListener sourceListener;

    /** ColumnSet transformer from source to downstream */
    private final ModifiedColumnSet.Transformer mcsTransformer;

    // this manager has only one bucket, managed by this object
    final UpdateByBucketHelper zeroKeyUpdateBy;

    /**
     * Perform an updateBy without any key columns.
     *
     * @param windows the unique windows for this UpdateBy, each window contains operators that can share processing
     *        resources
     * @param inputSources the primitive input sources
     * @param preservedColumns columns from the source table that are unchanged in the result table
     * @param source the source table
     * @param resultSources the result sources
     * @param timestampColumnName the column to use for all time-aware operators
     * @param rowRedirection the row redirection for dense output sources
     * @param control the control object.
     */
    protected ZeroKeyUpdateByManager(
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final QueryTable source,
            @NotNull final String[] preservedColumns,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable final String timestampColumnName,
            @Nullable final RowRedirection rowRedirection,
            @NotNull final UpdateByControl control) {
        super(source, windows, inputSources, timestampColumnName, rowRedirection, control);
        final String bucketDescription = this + "-bucket-[]";

        if (source.isRefreshing()) {
            result = new QueryTable(source.getRowSet(), resultSources);

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

            // create an updateby bucket instance directly from the source table
            zeroKeyUpdateBy = new UpdateByBucketHelper(bucketDescription, source, windows, resultSources,
                    timestampColumnName, control, (oe, se) -> deliverUpdateError(oe, se, true));
            buckets.offer(zeroKeyUpdateBy);

            // make the source->result transformer
            mcsTransformer = source.newModifiedColumnSetTransformer(result, preservedColumns);

            // result will depend on zeroKeyUpdateBy
            result.addParentReference(zeroKeyUpdateBy.result);
        } else {
            zeroKeyUpdateBy = new UpdateByBucketHelper(bucketDescription, source, windows, resultSources,
                    timestampColumnName, control, (oe, se) -> {
                        throw new IllegalStateException("Update failure from static zero key updateBy");
                    });
            result = zeroKeyUpdateBy.result;
            buckets.offer(zeroKeyUpdateBy);
            sourceListener = null;
            mcsTransformer = null;
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
        // for Zero-Key, verify the source and the single bucket are satisfied
        return source.satisfied(step) && zeroKeyUpdateBy.result.satisfied(step);
    }

    @Override
    protected boolean maybeDeliverPendingFailure() {
        return false;
    }
}
