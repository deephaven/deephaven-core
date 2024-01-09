/**
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.util.DelayedErrorNotifier;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.jetbrains.annotations.NotNull;

public abstract class InstrumentedTableUpdateSource extends InstrumentedUpdateSource {

    private final BaseTable<?> result;

    @ReferentialIntegrity
    private Runnable delayedErrorReference;

    public InstrumentedTableUpdateSource(final BaseTable<?> result, final String description) {
        super(result.getUpdateGraph(), description);
        this.result = result;
    }

    @Override
    protected final void onRefreshError(@NotNull final Exception error) {
        if (result.satisfied(updateGraph.clock().currentStep())) {
            // If the result is already satisfied (because it managed to send its notification, or was otherwise
            // satisfied) we should not send our error notification on this cycle.
            if (!result.isFailed()) {
                // If the result isn't failed, we need to mark it as such on the next cycle.
                delayedErrorReference = new DelayedErrorNotifier(error, entry, result);
            }
        } else {
            result.notifyListenersOnError(error, entry);
        }
    }

    protected abstract void instrumentedRefresh();
}
