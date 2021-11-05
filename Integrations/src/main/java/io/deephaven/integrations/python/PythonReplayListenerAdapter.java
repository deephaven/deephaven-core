/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.python;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.v2.InstrumentedListenerAdapter;
import io.deephaven.engine.v2.ModifiedColumnSet;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetFactoryImpl;
import io.deephaven.engine.v2.utils.RowSetShiftData;
import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;


/**
 * A Deephaven table listener which passes update events to a Python listener object. The listener can also replay the
 * current table snapshot.
 *
 * The Python listener object can be either (1) a callable or (2) an object which provides an "onUpdate" method. In
 * either case, the method must take two arguments (isReplay, updates).
 */
@ScriptApi
public class PythonReplayListenerAdapter extends InstrumentedListenerAdapter
        implements TableSnapshotReplayer {
    private static final long serialVersionUID = -8882402061960621245L;
    private final PyObject pyCallable;

    /**
     * Create a Python listener.
     *
     * No description for this listener will be provided. A hard reference to this listener will be maintained to
     * prevent garbage collection. See
     * {@link #PythonReplayListenerAdapter(String, Table, boolean, PyObject)} if you do not want to
     * prevent garbage collection of this listener.
     *
     * @param source The source table to which this listener will subscribe.
     * @param pyObjectIn Python listener object.
     */
    public PythonReplayListenerAdapter(Table source, PyObject pyObjectIn) {
        this(null, source, true, pyObjectIn);
    }

    /**
     * Create a Python listener.
     *
     * A hard reference to this listener will be maintained to prevent garbage collection. See
     * {@link #PythonReplayListenerAdapter(String, Table, boolean, PyObject)} if you do not want to
     * prevent garbage collection of this listener.
     *
     * @param description A description for the UpdatePerformanceTracker to append to its entry description.
     * @param source The source table to which this listener will subscribe.
     * @param pyObjectIn Python listener object.
     */
    public PythonReplayListenerAdapter(String description, Table source, PyObject pyObjectIn) {
        this(description, source, true, pyObjectIn);
    }

    /**
     * Create a Python listener.
     *
     * @param description A description for the UpdatePerformanceTracker to append to its entry description.
     * @param source The source table to which this listener will subscribe.
     * @param retain Whether a hard reference to this listener should be maintained to prevent it from being collected.
     * @param pyObjectIn Python listener object.
     */
    public PythonReplayListenerAdapter(String description, Table source, boolean retain,
                                       PyObject pyObjectIn) {
        super(description, source, retain);
        pyCallable = PythonUtilities.pyListenerFunc(pyObjectIn);
    }

    @Override
    public void replay() {
        final RowSet emptyRowSet = RowSetFactoryImpl.INSTANCE.empty();
        final RowSetShiftData emptyShift = RowSetShiftData.EMPTY;
        final ModifiedColumnSet emptyColumnSet = ModifiedColumnSet.EMPTY;
        final Update update = new Update(source.getRowSet(), emptyRowSet, emptyRowSet, emptyShift, emptyColumnSet);
        final boolean isReplay = true;
        pyCallable.call("__call__", isReplay, update);
    }

    @Override
    public void onUpdate(final Update update) {
        final boolean isReplay = false;
        pyCallable.call("__call__", isReplay, update);
    }
}
