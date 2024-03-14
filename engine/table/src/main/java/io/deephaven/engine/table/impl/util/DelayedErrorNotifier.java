//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.updategraph.UpdateGraph;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;

/**
 * Re-usable tool for scheduling delayed error notifications to be enqueued on the next update graph cycle. This is used
 * when an error is detected, but delivering the error would violate single-notification guarantees.
 */
public final class DelayedErrorNotifier implements Runnable {

    private final Throwable error;
    private final TableListener.Entry entry;
    private final UpdateGraph updateGraph;
    private final WeakReference<BaseTable<?>> tableReference;

    public DelayedErrorNotifier(
            @NotNull final Throwable error,
            @Nullable final TableListener.Entry entry,
            @NotNull final BaseTable<?> table) {
        this.error = error;
        updateGraph = table.getUpdateGraph();
        tableReference = new WeakReference<>(table);
        this.entry = entry;
        updateGraph.addSource(this);
    }

    @Override
    public void run() {
        updateGraph.removeSource(this);

        final BaseTable<?> table = tableReference.get();
        if (table == null) {
            return;
        }

        table.notifyListenersOnError(error, entry);
        table.forceReferenceCountToZero();
    }
}
