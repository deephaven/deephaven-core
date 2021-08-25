package io.deephaven.internal.log;

import io.deephaven.io.logger.Logger;

/**
 * An implementation that always returns a single cached {@link Logger}. (This also means that the implementations does
 * not make use of the logger name.)
 */
public abstract class LoggerFactorySingleCache implements LoggerFactory {
    private volatile Logger INSTANCE = null;

    @Override
    public final Logger create(String name) {
        Logger local;
        if ((local = INSTANCE) == null) {
            synchronized (this) {
                if ((local = INSTANCE) == null) {
                    local = createInternal();
                    INSTANCE = local;
                }
            }
        }
        return local;
    }

    public abstract Logger createInternal();
}
