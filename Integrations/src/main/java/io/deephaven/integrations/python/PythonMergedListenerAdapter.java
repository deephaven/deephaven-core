//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jpy.PyObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A Deephaven merged listener which fires when any of its bound listener recorders has updates and all of its
 * dependencies have been satisfied. The listener then invokes the Python listener object.
 *
 * The Python listener object must be a Python MergedListener instance that provides a "_process" method implementation
 * with no argument.
 */
@ScriptApi
public class PythonMergedListenerAdapter extends MergedListener {
    private static final Logger log = LoggerFactory.getLogger(PythonMergedListenerAdapter.class);

    private final PyObject pyListenerCallable;
    private final PyObject pyOnFailureCallback;

    /**
     * Create a Python merged listener.
     *
     * @param recorders The listener recorders to which this listener will subscribe.
     * @param dependencies The tables that must be satisfied before this listener is executed.
     * @param listenerDescription A description for the UpdatePerformanceTracker to append to its entry description, may
     *        be null.
     * @param pyListener Python listener object.
     */
    private PythonMergedListenerAdapter(
            @NotNull ListenerRecorder[] recorders,
            @Nullable NotificationQueue.Dependency[] dependencies,
            @Nullable String listenerDescription,
            @NotNull PyObject pyListener,
            @NotNull PyObject pyOnFailureCallback) {
        super(Arrays.asList(recorders), Arrays.asList(dependencies), listenerDescription, null);
        Arrays.stream(recorders).forEach(rec -> rec.setMergedListener(this));
        this.pyListenerCallable = PythonUtils.pyMergeListenerFunc(Objects.requireNonNull(pyListener));
        this.pyOnFailureCallback = Objects.requireNonNull(pyOnFailureCallback);
    }

    public static PythonMergedListenerAdapter create(
            @NotNull ListenerRecorder[] recorders,
            @Nullable NotificationQueue.Dependency[] dependencies,
            @Nullable String listenerDescription,
            @NotNull PyObject pyListener,
            @NotNull PyObject pyOnFailureCallback) {
        if (recorders.length < 2) {
            throw new IllegalArgumentException("At least two listener recorders must be provided");
        }

        final NotificationQueue.Dependency[] allItems =
                Stream.concat(Arrays.stream(recorders), Arrays.stream(dependencies))
                        .filter(Objects::nonNull)
                        .toArray(NotificationQueue.Dependency[]::new);

        final UpdateGraph updateGraph = allItems[0].getUpdateGraph(allItems);

        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            return new PythonMergedListenerAdapter(recorders, dependencies, listenerDescription, pyListener,
                    pyOnFailureCallback);
        }
    }

    public ArrayList<TableUpdate> currentRowsAsUpdates() {
        final ArrayList<TableUpdate> updates = new ArrayList<>();
        for (ListenerRecorder recorder : getRecorders()) {
            final TableUpdate update = new TableUpdateImpl(
                    recorder.getParent().getRowSet().copy(),
                    RowSetFactory.empty(),
                    RowSetFactory.empty(),
                    RowSetShiftData.EMPTY,
                    ModifiedColumnSet.EMPTY);
            updates.add(update);
        }
        return updates;
    }

    @Override
    protected void process() {
        pyListenerCallable.call("__call__");
    }

    @Override
    protected void propagateErrorDownstream(boolean fromProcess, @NotNull Throwable error,
            TableListener.@Nullable Entry entry) {
        if (!pyOnFailureCallback.isNone()) {
            try {
                pyOnFailureCallback.call("__call__", ExceptionUtils.getStackTrace(error));
            } catch (Exception e2) {
                // If the Python onFailure callback fails, log the new exception
                // and continue with the original exception.
                log.error().append("Python on_error callback failed: ").append(e2).endl();
            }
        } else {
            log.error().append("Python on_error callback is None: ").append(ExceptionUtils.getStackTrace(error)).endl();
        }
    }
}
