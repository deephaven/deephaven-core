//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.ListenerRecorder;
import io.deephaven.engine.table.impl.MergedListener;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jpy.PyObject;

import java.util.Arrays;

/**
 * A Deephaven merged listener which fires when any of its bound listener recorders has updates and all of its
 * dependencies have been satisfied. The listener then invokes the Python listener object.
 *
 * The Python listener object must be a Python MergedListener instance that provides a "process" method implementation
 * with no argument.
 */
public class PythonMergedListenerAdapter extends MergedListener {
    private final PyObject pyCallable;

    /**
     * Create a Python merged listener.
     *
     * @param recorders The listener recorders to which this listener will subscribe.
     * @param dependencies The tables that must be satisfied before this listener is executed.
     * @param listenerDescription A description for the UpdatePerformanceTracker to append to its entry description, may
     *        be null.
     * @param pyObjectIn Python listener object.
     */
    private PythonMergedListenerAdapter(
            @NotNull ListenerRecorder[] recorders,
            @Nullable NotificationQueue.Dependency[] dependencies,
            @Nullable String listenerDescription,
            @NotNull PyObject pyObjectIn) {
        super(Arrays.asList(recorders), Arrays.asList(dependencies), listenerDescription, null);
        Arrays.stream(recorders).forEach(rec -> rec.setMergedListener(this));
        this.pyCallable = PythonUtils.pyMergeListenerFunc(pyObjectIn);
    }

    public static PythonMergedListenerAdapter create(
            @NotNull ListenerRecorder[] recorders,
            @Nullable NotificationQueue.Dependency[] dependencies,
            @Nullable String listenerDescription,
            @NotNull PyObject pyObjectIn) {
        final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();

        if (!Arrays.stream(recorders).allMatch(t -> t.getParent().getUpdateGraph() == updateGraph)) {
            throw new IllegalArgumentException("All recorders must be from the same update graph");
        }

        if (!Arrays.stream(dependencies).allMatch(t -> t.getUpdateGraph() == updateGraph)) {
            throw new IllegalArgumentException("All dependencies must be from the same update graph");
        }

        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            return new PythonMergedListenerAdapter(recorders, dependencies, listenerDescription, pyObjectIn);
        }
    }

    @Override
    protected void process() {
        pyCallable.call("__call__");
    }
}
