//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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

    public static void clear(LogBuffer logBuffer) {
        synchronized (LogBufferGlobal.class) {
            if (logBuffer != INSTANCE) {
                throw new IllegalStateException("Can only clear existing log buffer");
            }
            INSTANCE = null;
        }
    }
}
