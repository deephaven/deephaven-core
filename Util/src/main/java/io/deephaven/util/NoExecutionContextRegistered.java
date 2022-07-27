package io.deephaven.util;

import io.deephaven.UncheckedDeephavenException;

/**
 * This exception is thrown when the thread-local QueryScope, QueryLibrary, or CompilerTools.Context are accessed
 * from user-code without an explicit ExecutionContext.
 */
public final class NoExecutionContextRegistered extends UncheckedDeephavenException {
    public NoExecutionContextRegistered() {
        super("ExecutionContext not registered");
    }
}
