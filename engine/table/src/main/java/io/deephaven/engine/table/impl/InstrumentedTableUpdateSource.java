//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.util.DelayedErrorNotifier;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;

public abstract class InstrumentedTableUpdateSource extends InstrumentedUpdateSource {

    private final WeakReference<BaseTable<?>> tableReference;

    @ReferentialIntegrity
    private Runnable delayedErrorReference;

    public InstrumentedTableUpdateSource(
            final UpdateSourceRegistrar updateSourceRegistrar,
            final BaseTable<?> table,
            final String description) {
        super(updateSourceRegistrar, description);
        // verify that the updateSourceRegistrar's update graph is the same as the table's update graph (if applicable)
        updateSourceRegistrar.getUpdateGraph(table);
        tableReference = new WeakReference<>(table);
    }

    public InstrumentedTableUpdateSource(final BaseTable<?> table, final String description) {
        this(table.getUpdateGraph(), table, description);
    }

    @Override
    protected void onRefreshError(@NotNull final Exception error) {
        final BaseTable<?> table = tableReference.get();
        if (table == null) {
            return;
        }

        if (table.satisfied(updateSourceRegistrar.getUpdateGraph().clock().currentStep())) {
            // If the result is already satisfied (because it managed to send its notification, or was otherwise
            // satisfied) we should not send our error notification on this cycle.
            if (!table.isFailed()) {
                // If the result isn't failed, we need to mark it as such on the next cycle.
                delayedErrorReference = new DelayedErrorNotifier(error, entry, table);
            }
        } else {
            table.notifyListenersOnError(error, entry);
            table.forceReferenceCountToZero();
        }
    }
}
