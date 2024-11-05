//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.List;
import java.util.Map;

import static io.deephaven.engine.table.Table.APPEND_ONLY_TABLE_ATTRIBUTE;
import static io.deephaven.engine.table.Table.BLINK_TABLE_ATTRIBUTE;

/**
 * This class provides a single method to adapt an {@link Table#ADD_ONLY_TABLE_ATTRIBUTE add-only} or
 * {@link Table#APPEND_ONLY_TABLE_ATTRIBUTE append-only} table to a {@link Table#BLINK_TABLE_ATTRIBUTE blink} table.
 */
public final class AddOnlyToBlinkTableAdapter {

    /**
     * Convert an {@link Table#ADD_ONLY_TABLE_ATTRIBUTE add-only} or {@link Table#APPEND_ONLY_TABLE_ATTRIBUTE
     * append-only} table to a {@link Table#BLINK_TABLE_ATTRIBUTE blink} table. The callee must guarantee that the
     * passed in table is {@link Table#isRefreshing() refreshing} and only delivers additions in its updates.
     *
     * @param table An add-only or append-only table
     * @return A blink table based on the input table
     */
    public static Table toBlink(@NotNull final Table table) {
        if (!table.isRefreshing()) {
            throw new IllegalArgumentException("Input table is not refreshing");
        }

        if (BlinkTableTools.isBlink(table)) {
            LivenessScopeStack.peek().manage(table);
            return table;
        }

        final Table addOnlyTable;
        if (!Boolean.TRUE.equals(table.getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE))
                && !Boolean.TRUE.equals(table.getAttribute(APPEND_ONLY_TABLE_ATTRIBUTE))) {
            addOnlyTable = table.withAttributes(Map.of(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE));
        } else {
            addOnlyTable = table;
        }

        final MutableObject<QueryTable> resultHolder = new MutableObject<>();
        final MutableObject<AddOnlyToBlinkListener> listenerHolder = new MutableObject<>();
        final BaseTable<?> coalesced = (BaseTable<?>) addOnlyTable.coalesce();
        final OperationSnapshotControl snapshotControl =
                coalesced.createSnapshotControlIfRefreshing(OperationSnapshotControl::new);

        // noinspection DataFlowIssue snapshotControl cannot be null here, since we know the table is refreshing
        ConstructSnapshot.callDataSnapshotFunction("addOnlyToBlink", snapshotControl,
                (final boolean usePrev, final long beforeClockValue) -> {
                    // Start with the same rows as the original table
                    final TrackingRowSet resultRowSet = usePrev
                            ? addOnlyTable.getRowSet().copyPrev().toTracking()
                            : addOnlyTable.getRowSet().copy().toTracking();
                    final QueryTable result = new QueryTable(resultRowSet, addOnlyTable.getColumnSourceMap());
                    result.setRefreshing(true);
                    result.setAttribute(BLINK_TABLE_ATTRIBUTE, true);

                    final ListenerRecorder recorder =
                            new ListenerRecorder("AddOnlyToBlinkListenerRecorder", addOnlyTable, null);
                    final AddOnlyToBlinkListener listener = new AddOnlyToBlinkListener(recorder, result);
                    recorder.setMergedListener(listener);
                    result.addParentReference(listener);
                    snapshotControl.setListenerAndResult(recorder, result);

                    listenerHolder.setValue(listener);
                    resultHolder.setValue(result);
                    return true;
                });
        listenerHolder.getValue().getUpdateGraph().addSource(listenerHolder.getValue());
        return resultHolder.getValue();
    }

    private static final class AddOnlyToBlinkListener extends MergedListener implements Runnable {

        private final ListenerRecorder sourceRecorder;

        private AddOnlyToBlinkListener(
                @NotNull final ListenerRecorder sourceRecorder,
                @NotNull final QueryTable result) {
            super(List.of(sourceRecorder), List.of(), "AddOnlyToBlinkListener", result);
            this.sourceRecorder = sourceRecorder;
        }

        @Override
        protected void process() {
            final TableUpdate downstream = new TableUpdateImpl(
                    sourceRecorder.recordedVariablesAreValid()
                            ? sourceRecorder.getAdded().copy()
                            : RowSetFactory.empty(),
                    result.getRowSet().copy(),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY);
            result.getRowSet().writableCast().update(downstream.added(), downstream.removed());
            result.notifyListeners(downstream);
        }

        @Override
        protected boolean canExecute(final long step) {
            return getUpdateGraph().satisfied(step) && sourceRecorder.satisfied(step);
        }

        @Override
        public void run() {
            if (!result.isEmpty()) {
                notifyChanges();
            }
        }

        @OverridingMethodsMustInvokeSuper
        @Override
        protected void destroy() {
            super.destroy();
            getUpdateGraph().removeSource(this);
        }
    }
}
