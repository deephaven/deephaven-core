/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.python;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.ShiftObliviousInstrumentedListenerAdapter;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;


/**
 * A Deephaven table listener which passes update events to a Python listener object. The listener can also replay the
 * current table snapshot.
 *
 * The Python listener object can be either (1) a callable or (2) an object which provides an "onUpdate" method. In
 * either case, the method must take four arguments (isReplay, added, removed, modified).
 */
@ScriptApi
public class PythonReplayShiftObliviousListenerAdapter extends ShiftObliviousInstrumentedListenerAdapter
        implements TableSnapshotReplayer {
    private static final long serialVersionUID = -356456046697273581L;
    private final PyObject pyCallable;

    /**
     * Create a Python listener.
     *
     * No description for this listener will be provided. A hard reference to this listener will be maintained to
     * prevent garbage collection. See
     * {@link #PythonReplayShiftObliviousListenerAdapter(String, Table, boolean, PyObject)} if you do not want to
     * prevent garbage collection of this listener.
     *
     * @param source The source table to which this listener will subscribe.
     * @param pyObjectIn Python listener object.
     */
    public PythonReplayShiftObliviousListenerAdapter(Table source, PyObject pyObjectIn) {
        this(null, source, true, pyObjectIn);
    }

    /**
     * Create a Python listener.
     *
     * A hard reference to this listener will be maintained to prevent garbage collection. See
     * {@link #PythonReplayShiftObliviousListenerAdapter(String, Table, boolean, PyObject)} if you do not want to
     * prevent garbage collection of this listener.
     *
     * @param description A description for the UpdatePerformanceTracker to append to its entry description.
     * @param source The source table to which this listener will subscribe.
     * @param pyObjectIn Python listener object.
     */
    public PythonReplayShiftObliviousListenerAdapter(String description, Table source, PyObject pyObjectIn) {
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
    public PythonReplayShiftObliviousListenerAdapter(String description, Table source, boolean retain,
            PyObject pyObjectIn) {
        super(description, source, retain);
        pyCallable = PythonUtils.pyListenerFunc(pyObjectIn);
    }

    @Override
    public void replay() {
        final RowSet emptyRowSet = RowSetFactory.empty();
        final boolean isReplay = true;
        pyCallable.call("__call__", isReplay, source.getRowSet(), emptyRowSet, emptyRowSet);
    }

    @Override
    public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
        final boolean isReplay = false;
        pyCallable.call("__call__", isReplay, added, removed, modified);
    }
}
