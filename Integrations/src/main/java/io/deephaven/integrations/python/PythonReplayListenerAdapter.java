//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import org.apache.commons.lang3.exception.ExceptionUtils;
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
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;


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
    private static final Logger log = LoggerFactory.getLogger(PythonReplayListenerAdapter.class);

    private final PyObject pyListenerCallable;
    private final PyObject pyOnFailureCallback;
    private final NotificationQueue.Dependency[] dependencies;

    /**
     * Create a Python listener.
     *
     * @param description A description for the UpdatePerformanceTracker to append to its entry description, may be
     *        null.
     * @param source The source table to which this listener will subscribe.
     * @param retain Whether a hard reference to this listener should be maintained to prevent it from being collected.
     * @param pyListener Python listener object.
     * @param dependencies The tables that must be satisfied before this listener is executed.
     */
    public static PythonReplayListenerAdapter create(
            @Nullable String description,
            @NotNull Table source,
            boolean retain,
            @NotNull PyObject pyListener,
            @NotNull PyObject pyOnFailureCallback,
            @Nullable NotificationQueue.Dependency... dependencies) {
        final UpdateGraph updateGraph = source.getUpdateGraph(dependencies);
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            return new PythonReplayListenerAdapter(description, source, retain, pyListener, pyOnFailureCallback,
                    dependencies);
        }
    }

    private PythonReplayListenerAdapter(
            @Nullable String description,
            @NotNull Table source,
            boolean retain,
            @NotNull PyObject pyListener,
            @NotNull PyObject pyOnFailureCallback,
            @Nullable NotificationQueue.Dependency... dependencies) {
        super(description, source, retain);
        this.dependencies = dependencies;
        this.pyListenerCallable = PythonUtils.pyListenerFunc(Objects.requireNonNull(pyListener));
        this.pyOnFailureCallback = Objects.requireNonNull(pyOnFailureCallback);
    }

    @Override
    public void replay() {
        final RowSet emptyRowSet = RowSetFactory.empty();
        final RowSetShiftData emptyShift = RowSetShiftData.EMPTY;
        final ModifiedColumnSet emptyColumnSet = ModifiedColumnSet.EMPTY;
        final TableUpdate update =
                new TableUpdateImpl(source.getRowSet(), emptyRowSet, emptyRowSet, emptyShift, emptyColumnSet);
        final boolean isReplay = true;
        pyListenerCallable.call("__call__", update, isReplay);
    }

    @Override
    public void onUpdate(final TableUpdate update) {
        final boolean isReplay = false;
        pyListenerCallable.call("__call__", update, isReplay);
    }

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        if (!pyOnFailureCallback.isNone()) {
            try {
                pyOnFailureCallback.call("__call__", ExceptionUtils.getStackTrace(originalException));
            } catch (Throwable e) {
                // If the Python onFailure callback fails, log the new exception
                // and continue with the original exception.
                log.error().append("Python on_error callback failed: ").append(e).endl();
            }
        } else {
            log.error().append("Python on_error callback is None: ")
                    .append(ExceptionUtils.getStackTrace(originalException)).endl();
        }
        super.onFailureInternal(originalException, sourceEntry);
    }

    @Override
    public boolean canExecute(final long step) {
        return super.canExecute(step)
                && (dependencies.length == 0 || Arrays.stream(dependencies).allMatch(t -> t.satisfied(step)));
    }

    public boolean isFailed() {
        return failed;
    }
}
