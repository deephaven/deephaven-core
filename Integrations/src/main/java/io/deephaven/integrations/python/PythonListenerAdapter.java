/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.python;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;


/**
 * A Deephaven table listener which passes update events to a Python listener object.
 *
 * The Python listener object can be either (1) a callable or (2) an object which provides an "onUpdate" method. In
 * either case, the method must take one argument (updates).
 */
@ScriptApi
public class PythonListenerAdapter extends InstrumentedTableUpdateListenerAdapter {
    private static final long serialVersionUID = -1811392916918850289L;
    private final PyObject pyCallable;

    /**
     * Creates a Python listener.
     *
     * No description for this listener will be provided. A hard reference to this listener will be maintained to
     * prevent garbage collection. See {@link #PythonListenerAdapter(String, Table, boolean, PyObject)} if you do not
     * want to prevent garbage collection of this listener.
     *
     * @param source The source table to which this listener will subscribe.
     * @param pyObjectIn Python listener object.
     */
    public PythonListenerAdapter(Table source, PyObject pyObjectIn) {
        this(null, source, true, pyObjectIn);
    }

    /**
     * Create a Python listener.
     *
     * A hard reference to this listener will be maintained to prevent garbage collection. See
     * {@link #PythonListenerAdapter(String, Table, boolean, PyObject)} if you do not want to prevent garbage collection
     * of this listener.
     *
     * @param description A description for the UpdatePerformanceTracker to append to its entry description.
     * @param source The source table to which this listener will subscribe.
     * @param pyObjectIn Python listener object.
     */
    public PythonListenerAdapter(String description, Table source, PyObject pyObjectIn) {
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
    public PythonListenerAdapter(String description, Table source, boolean retain,
            PyObject pyObjectIn) {
        super(description, source, retain);
        pyCallable = PythonUtils.pyListenerFunc(pyObjectIn);
    }

    @Override
    public void onUpdate(final TableUpdate update) {
        pyCallable.call("__call__", update);
    }
}
