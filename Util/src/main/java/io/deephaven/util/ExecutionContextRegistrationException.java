//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when the {@link ThreadLocal} ExecutionContext or any of its components are accessed if they
 * have not been properly initialized.
 */
public final class ExecutionContextRegistrationException extends UncheckedDeephavenException {

    private static final Logger logger = LoggerFactory.getLogger(ExecutionContextRegistrationException.class);

    public ExecutionContextRegistrationException(@NotNull final String missingComponent) {
        super("No ExecutionContext registered, or current ExecutionContext has no " + missingComponent);
    }

    public static ExecutionContextRegistrationException onFailedComponentAccess(@NotNull final String componentName) {
        logger.error().append("No ExecutionContext registered, or current ExecutionContext has no ")
                .append(componentName).append('.')
                .append(" If this is being run in a thread, did you specify an ExecutionContext for the thread?")
                .append(" Please refer to the documentation on ExecutionContext for details.")
                .endl();
        return new ExecutionContextRegistrationException(componentName);
    }
}
