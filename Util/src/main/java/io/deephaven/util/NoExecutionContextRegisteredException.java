package io.deephaven.util;

import io.deephaven.UncheckedDeephavenException;

/**
 * This exception is thrown when the thread-local QueryScope, QueryLibrary, or QueryCompiler are accessed from user-code
 * without an explicit ExecutionContext.
 */
public final class NoExecutionContextRegisteredException extends UncheckedDeephavenException {
    public NoExecutionContextRegisteredException() {
        super("ExecutionContext not registered");
    }
}
