/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base;

/**
 * A generic interface for signaling a fatal error fatal errors.
 */
public interface FatalErrorHandler {
    void signalFatalError(String message, Throwable x);
}
