package io.deephaven.engine.table.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.updateby.UpdateByWindow;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedList;
import java.util.Map;

public class ZeroKeyUpdateByManager extends UpdateBy {
    // this manager has only one bucket, managed by this object
    final UpdateByBucketHelper zeroKeyUpdateBy;

    protected ZeroKeyUpdateByManager(@NotNull final String description,
            @NotNull QueryTable source,
            @NotNull UpdateByOperator[] operators,
            @NotNull UpdateByWindow[] windows,
            @NotNull ColumnSource<?>[] inputSources,
            @NotNull int[][] operatorInputSourceSlots,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable String timestampColumnName,
            @NotNull UpdateByRedirectionContext redirContext,
            @NotNull UpdateByControl control) {
        super(description, source, operators, windows, inputSources, operatorInputSourceSlots, resultSources,
                timestampColumnName, redirContext, control);

        // this table will always have the rowset of the source
        result = new QueryTable(source.getRowSet(), resultSources);

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

            // create an updateby bucket instance sourced from the source table
            zeroKeyUpdateBy = new UpdateByBucketHelper(description, source, operators, windows, inputSources,
                    operatorInputSourceSlots, resultSources, timestampColumnName, redirContext, control);
            buckets.offerLast(zeroKeyUpdateBy);

            // create a recorder instance sourced from the bucket helper
            ListenerRecorder recorder = new ListenerRecorder(description, zeroKeyUpdateBy.result, result);
            recorder.setMergedListener(listener);
            zeroKeyUpdateBy.result.addUpdateListener(recorder);
            recorders.offerLast(recorder);
        } else {
            // no shifting will be needed, can create directly from source
            zeroKeyUpdateBy = new UpdateByBucketHelper(description, source, operators, windows, inputSources,
                    operatorInputSourceSlots, resultSources, timestampColumnName, redirContext, control);
            this.result = zeroKeyUpdateBy.result;
            buckets.offerLast(zeroKeyUpdateBy);

            // create input modified column sets only
            for (UpdateByOperator op : operators) {
                op.createInputModifiedColumnSet(source);
            }
        }

        // make the source->result transformer
        transformer = source.newModifiedColumnSetTransformer(result, source.getDefinition().getColumnNamesArray());

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

    /**
     * Perform an updateBy without any key columns.
     *
     * @param description the operation description
     * @param source the source table
     * @param operators the operations to perform
     * @param resultSources the result sources
     * @param redirContext the row redirection shared context
     * @param control the control object.
     * @return the result table
     */
    public static Table compute(@NotNull final String description,
            @NotNull final QueryTable source,
            @NotNull final UpdateByOperator[] operators,
            @NotNull final UpdateByWindow[] windows,
            @NotNull final ColumnSource<?>[] inputSources,
            @NotNull final int[][] operatorInputSourceSlots,
            @NotNull final Map<String, ? extends ColumnSource<?>> resultSources,
            @Nullable final String timestampColumnName,
            @NotNull final UpdateByRedirectionContext redirContext,
            @NotNull final UpdateByControl control) {

        final ZeroKeyUpdateByManager manager = new ZeroKeyUpdateByManager(description, source, operators, windows,
                inputSources, operatorInputSourceSlots, resultSources, timestampColumnName, redirContext, control);
        return manager.result;
    }
}
