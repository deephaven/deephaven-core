/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.python;

import io.deephaven.db.v2.DynamicTable;
import io.deephaven.db.v2.InstrumentedShiftAwareListenerAdapter;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
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
public class PythonShiftAwareReplayListenerAdapter extends InstrumentedShiftAwareListenerAdapter
        implements TableSnapshotReplayer {
    private static final long serialVersionUID = -8882402061960621245L;
    private final PyObject pyCallable;

    /**
     * Create a Python listener.
     *
     * No description for this listener will be provided. A hard reference to this listener will be maintained to
     * prevent garbage collection. See
     * {@link #PythonShiftAwareReplayListenerAdapter(String, DynamicTable, boolean, PyObject)} if you do not want to
     * prevent garbage collection of this listener.
     *
     * @param source The source table to which this listener will subscribe.
     * @param pyObjectIn Python listener object.
     */
    public PythonShiftAwareReplayListenerAdapter(DynamicTable source, PyObject pyObjectIn) {
        this(null, source, true, pyObjectIn);
    }

    /**
     * Create a Python listener.
     *
     * A hard reference to this listener will be maintained to prevent garbage collection. See
     * {@link #PythonShiftAwareReplayListenerAdapter(String, DynamicTable, boolean, PyObject)} if you do not want to
     * prevent garbage collection of this listener.
     *
     * @param description A description for the UpdatePerformanceTracker to append to its entry description.
     * @param source The source table to which this listener will subscribe.
     * @param pyObjectIn Python listener object.
     */
    public PythonShiftAwareReplayListenerAdapter(String description, DynamicTable source, PyObject pyObjectIn) {
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
    public PythonShiftAwareReplayListenerAdapter(String description, DynamicTable source, boolean retain,
            PyObject pyObjectIn) {
        super(description, source, retain);
        pyCallable = PythonUtilities.pyListenerFunc(pyObjectIn);
    }

    @Override
    public void replay() {
        final Index emptyIndex = Index.FACTORY.getEmptyIndex();
        final IndexShiftData emptyShift = IndexShiftData.EMPTY;
        final ModifiedColumnSet emptyColumnSet = ModifiedColumnSet.EMPTY;
        final Update update = new Update(source.getIndex(), emptyIndex, emptyIndex, emptyShift, emptyColumnSet);
        final boolean isReplay = true;
        pyCallable.call("__call__", isReplay, update);
    }

    @Override
    public void onUpdate(final Update update) {
        final boolean isReplay = false;
        pyCallable.call("__call__", isReplay, update);
    }
}
