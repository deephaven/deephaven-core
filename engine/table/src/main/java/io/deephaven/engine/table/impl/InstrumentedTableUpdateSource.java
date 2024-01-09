/**
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.util.DelayedErrorNotifier;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;

public abstract class InstrumentedTableUpdateSource extends InstrumentedUpdateSource {

    private final WeakReference<BaseTable<?>> tableReference;

    @ReferentialIntegrity
    private Runnable delayedErrorReference;

    public InstrumentedTableUpdateSource(final BaseTable<?> table, final String description) {
        super(table.getUpdateGraph(), description);
        tableReference = new WeakReference<>(table);
    }

    @Override
    protected final void onRefreshError(@NotNull final Exception error) {
        final BaseTable<?> table = tableReference.get();
        if (table == null) {
            return;
        }

        if (table.satisfied(updateGraph.clock().currentStep())) {
            // If the result is already satisfied (because it managed to send its notification, or was otherwise
            // satisfied) we should not send our error notification on this cycle.
            if (!table.isFailed()) {
                // If the result isn't failed, we need to mark it as such on the next cycle.
                delayedErrorReference = new DelayedErrorNotifier(error, entry, table);
            }
        } else {
            table.notifyListenersOnError(error, entry);
        }
    }
}
