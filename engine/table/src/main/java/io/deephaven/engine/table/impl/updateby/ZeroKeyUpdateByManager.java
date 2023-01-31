package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

public class ZeroKeyUpdateByManager extends UpdateBy {
    /** The output table for this UpdateBy operation */
    final QueryTable result;

    // this manager has only one bucket, managed by this object
    final UpdateByBucketHelper zeroKeyUpdateBy;

    /**
     * Perform an updateBy without any key columns.
     *
     * @param operators the operations to perform
     * @param windows the unique windows for this UpdateBy
     * @param inputSources the primitive input sources
     * @param source the source table
     * @param resultSources the result sources
     * @param timestampColumnName the column to use for all time-aware operators
     * @param rowRedirection the row redirection for dense output sources
     * @param control the control object.
     */
    protected ZeroKeyUpdateByManager(
            @NotNull UpdateByOperator[] operators,
            @NotNull UpdateByWindow[] windows,
            @NotNull ColumnSource<?>[] inputSources,
            @NotNull QueryTable source,
            final String[] persistentColumns,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable String timestampColumnName,
            @Nullable WritableRowRedirection rowRedirection,
            @NotNull UpdateByControl control) {
        super(source, operators, windows, inputSources, timestampColumnName, rowRedirection, control);
        final String bucketDescription = this + "-bucket-[]";

        if (source.isRefreshing()) {
            result = new QueryTable(source.getRowSet(), resultSources);

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

            // create an updateby bucket instance directly from the source table
            zeroKeyUpdateBy = new UpdateByBucketHelper(bucketDescription, source, windows, inputSources,
                    resultSources, timestampColumnName, control, (oe, se) -> deliverUpdateError(oe, se, true));
            buckets.offer(zeroKeyUpdateBy);

            // make the source->result transformer
            transformer = source.newModifiedColumnSetTransformer(result, persistentColumns);

            // result will depend on zeroKeyUpdateBy
            result.addParentReference(zeroKeyUpdateBy.result);
        } else {
            zeroKeyUpdateBy = new UpdateByBucketHelper(bucketDescription, source, windows, inputSources,
                    resultSources, timestampColumnName, control, (oe, se) -> {
                        throw new IllegalStateException("Update failure from static zero key updateBy");
                    });
            result = zeroKeyUpdateBy.result;
            buckets.offer(zeroKeyUpdateBy);
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
        // for Zero-Key, verify the source and the single bucket are satisfied
        return source.satisfied(step) && zeroKeyUpdateBy.result.satisfied(step);
    }
}
