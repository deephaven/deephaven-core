/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;

import static io.deephaven.engine.table.Table.BLINK_TABLE_ATTRIBUTE;

/**
 * This class provides a single method to adapt an {@link Table#ADD_ONLY_TABLE_ATTRIBUTE add only} table into a blink
 * table.
 */
public final class AddOnlyToBlinkTableAdapter {
    /**
     * Convert an {@link Table#ADD_ONLY_TABLE_ATTRIBUTE add only} table to a blink table. The callee must guarantee that
     * the passed in table is {@link Table#isRefreshing() refreshing} and add only.
     *
     * @param table An add only table
     * @return A blink table based on the input table
     */
    public static Table toBlink(@NotNull final Table table) {
        if(!table.isRefreshing()) {
            throw new IllegalArgumentException("Input table is not refreshing");
        }

        if (BlinkTableTools.isBlink(table)) {
            return table;
        }

        if (!Boolean.TRUE.equals(table.getAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE))) {
            throw new IllegalArgumentException("Argument table is not Add Only");
        }

        final MutableObject<QueryTable> resultHolder = new MutableObject<>();
        final MutableObject<AppendToBlinkListener> listenerHolder = new MutableObject<>();
        final BaseTable<?> coalesced = (BaseTable<?>) table.coalesce();
        final SwapListener swapListener = coalesced.createSwapListenerIfRefreshing(SwapListener::new);

        ConstructSnapshot.callDataSnapshotFunction("addOnlyToBlink", swapListener.makeSnapshotControl(),
                (final boolean usePrev, final long beforeClockValue) -> {
                    // Start with the same rows as the original table
                    final TrackingRowSet resultRowSet = usePrev
                            ? table.getRowSet().copyPrev().toTracking()
                            : table.getRowSet().copy().toTracking();
                    final QueryTable result = new QueryTable(resultRowSet, table.getColumnSourceMap());
                    result.setRefreshing(true);
                    result.setAttribute(BLINK_TABLE_ATTRIBUTE, true);

                    final ListenerRecorder recorder =
                            new ListenerRecorder("AppendToBlinkListenerRecorder", table, result);
                    final AppendToBlinkListener listener = new AppendToBlinkListener(recorder, result);
                    recorder.setMergedListener(listener);
                    result.addParentReference(listener);
                    swapListener.setListenerAndResult(recorder, result);

                    listenerHolder.setValue(listener);
                    resultHolder.setValue(result);
                    return true;
                });
        listenerHolder.getValue().getUpdateGraph().addSource(listenerHolder.getValue());
        return resultHolder.getValue();
    }

    private static final class AppendToBlinkListener extends MergedListener implements Runnable {
        private final ListenerRecorder sourceRecorder;

        private AppendToBlinkListener(@NotNull final ListenerRecorder recorder,
                @NotNull final QueryTable result) {
            super(Collections.singleton(recorder), Collections.emptyList(), "AppendToBlinkListener", result);
            this.sourceRecorder = recorder;
        }

        @Override
        protected void process() {
            if (sourceRecorder.recordedVariablesAreValid()) {
                Assert.eqTrue(sourceRecorder.getModified().isEmpty() &&
                        sourceRecorder.getRemoved().isEmpty() &&
                        sourceRecorder.getShifted().empty(), "source update is append only");
            }

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

        @Override
        protected void destroy() {
            getUpdateGraph().removeSource(this);
            super.destroy();
        }
    }
}
