package io.deephaven.util.progress;


import io.deephaven.base.verify.Require;
import io.deephaven.io.logger.Logger;

import java.util.function.Supplier;

/**
 * Log the status and progress to a log file.
 *
 */
public class ProgressLogger implements StatusCallback {
    private final Logger log;
    private final StatusCallback parent;
    private int lastProgress = -1;

    public ProgressLogger(StatusCallback parent, Logger log) {
        Require.neqNull(log, "log");
        Require.neqNull(parent, "parent StatusCallback");

        this.log = log;
        this.parent = parent;
    }

    @Override
    public void update(final int progress, final Supplier<String> status) {
        parent.update(progress, status);
        if (progress != lastProgress) {
            log.info().append(status.get()).append(" - Progress: ").append(parent.getValue()).append("%").endl();
            lastProgress = progress;
        }
    }

    @Override
    public int getValue() {
        return parent.getValue();
    }
}
