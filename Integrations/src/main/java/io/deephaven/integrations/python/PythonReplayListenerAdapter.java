//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

import javax.annotation.Nullable;
import java.util.Arrays;


/**
 * A Deephaven table listener which passes update events to a Python listener object. The listener can also replay the
 * current table snapshot.
 *
 * The Python listener object can be either (1) a callable or (2) an object which provides an "onUpdate" method. In
 * either case, the method must take two arguments (isReplay, updates).
 */
@ScriptApi
public class PythonReplayListenerAdapter extends InstrumentedTableUpdateListenerAdapter
        implements TableSnapshotReplayer {
    private static final long serialVersionUID = -8882402061960621245L;
    private final PyObject pyCallable;
    private final NotificationQueue.Dependency[] dependencies;

    /**
     * Create a Python listener.
     *
     * @param description A description for the UpdatePerformanceTracker to append to its entry description, may be
     *        null.
     * @param source The source table to which this listener will subscribe.
     * @param retain Whether a hard reference to this listener should be maintained to prevent it from being collected.
     * @param pyObjectIn Python listener object.
     * @param dependencies The tables that must be satisfied before this listener is executed.
     */
    public static PythonReplayListenerAdapter create(@Nullable String description, Table source, boolean retain,
            PyObject pyObjectIn, NotificationQueue.Dependency... dependencies) {
        final UpdateGraph updateGraph = source.getUpdateGraph(dependencies);
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            return new PythonReplayListenerAdapter(description, source, retain, pyObjectIn, dependencies);
        }
    }

    private PythonReplayListenerAdapter(@Nullable String description, Table source, boolean retain, PyObject pyObjectIn,
            NotificationQueue.Dependency... dependencies) {
        super(description, source, retain);
        this.dependencies = dependencies;
        this.pyCallable = PythonUtils.pyListenerFunc(pyObjectIn);
    }

    @Override
    public void replay() {
        final RowSet emptyRowSet = RowSetFactory.empty();
        final RowSetShiftData emptyShift = RowSetShiftData.EMPTY;
        final ModifiedColumnSet emptyColumnSet = ModifiedColumnSet.EMPTY;
        final TableUpdate update =
                new TableUpdateImpl(source.getRowSet(), emptyRowSet, emptyRowSet, emptyShift, emptyColumnSet);
        final boolean isReplay = true;
        pyCallable.call("__call__", update, isReplay);
    }

    @Override
    public void onUpdate(final TableUpdate update) {
        final boolean isReplay = false;
        pyCallable.call("__call__", update, isReplay);
    }

    @Override
    public boolean canExecute(final long step) {
        return super.canExecute(step)
                && (dependencies.length == 0 || Arrays.stream(dependencies).allMatch(t -> t.satisfied(step)));
    }
}
