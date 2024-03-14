//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

@SuppressWarnings("unused") // only used reflexively
public class ThrowRuntimeExceptionFatalErrorHandlerFactory implements FatalErrorHandlerFactory {
    public FatalErrorHandler get() {
        return (message, x) -> {
            throw new RuntimeException("Fatal error: " + message, x);
        };
    }
}
