package io.deephaven.util.progress;

import java.util.function.Supplier;

/**
 * Minimal root implementation of a Process Status. No interface is needed at this point.
 */
public class MinProcessStatus implements StatusCallback {
    private int progress;

    public MinProcessStatus() {
        progress = 0;
    }

    @Override
    public void update(final int progress, final Supplier<String> status) {
        this.progress = progress;
    }

    @Override
    public int getValue() {
        return progress;
    }

    @Override
    public int getStepValue() {
        return progress;
    }
}
