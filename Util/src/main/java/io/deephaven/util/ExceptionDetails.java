/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.function.Predicate;

/**
 * Class to help with determining details from Throwable instances.
 */
public class ExceptionDetails implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String errorMessage;
    private final String fullStackTrace;
    private final String shortCauses;

    public ExceptionDetails(final Throwable throwable) {
        errorMessage = throwable.toString();

        final StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));
        fullStackTrace = writer.toString();
        shortCauses = FindExceptionCause.shortCauses(throwable, "\n");
    }

    @SuppressWarnings("UnusedDeclaration")
    public String getErrorMessage() {
        return errorMessage;
    }

    public String getFullStackTrace() {
        return fullStackTrace;
    }

    public String getShortCauses() {
        return shortCauses;
    }

    @Override
    public String toString() {
        return errorMessage;
    }

    /**
     * Returns true if exceptionDetails is not null and the result of applying testToApply on
     * exceptionDetails is true
     *
     * @param exceptionDetails the exception to test
     * @param testToApply the test to apply
     * @return true if exceptionDetails is not null and testToApply returns true
     */
    public static boolean testExceptionDetails(@Nullable final ExceptionDetails exceptionDetails,
        @NotNull final Predicate<ExceptionDetails> testToApply) {
        return exceptionDetails != null && testToApply.test(exceptionDetails);
    }
}
