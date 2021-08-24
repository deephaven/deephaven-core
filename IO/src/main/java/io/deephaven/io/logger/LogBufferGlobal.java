package io.deephaven.io.logger;

import java.util.Objects;
import java.util.Optional;

public class LogBufferGlobal {

    private static LogBuffer INSTANCE;

    public static void setInstance(LogBuffer logBuffer) {
        synchronized (LogBufferGlobal.class) {
            if (INSTANCE != null) {
                throw new IllegalStateException("Should only LogBufferGlobal#setInstance once");
            }
            INSTANCE = Objects.requireNonNull(logBuffer, "logBuffer must not be null");
        }
    }

    public static Optional<LogBuffer> getInstance() {
        synchronized (LogBufferGlobal.class) {
            return Optional.ofNullable(INSTANCE);
        }
    }
}
