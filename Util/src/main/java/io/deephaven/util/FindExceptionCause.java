//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Optional;

public class FindExceptionCause {
    /**
     * Given an exception and a list of expected exception types, traverse the cause tree and return the first exception
     * that matches the list of expected cause types.
     */
    @SafeVarargs
    public static Exception findCause(Exception original, Class<? extends Exception>... expectedTypes) {
        Throwable cause = original.getCause();
        while (cause != null) {
            final Throwable checkCause = cause;
            if (Arrays.stream(expectedTypes).anyMatch(type -> type.isAssignableFrom(checkCause.getClass()))) {
                return (Exception) cause;
            }
            cause = cause.getCause();
        }
        return original;
    }

    /**
     * Given a throwable and a list of expected throwable types, traverse the cause tree and return the first exception
     * that matches the list of expected cause types.
     */
    @SafeVarargs
    public static Throwable findCause(Throwable original, Class<? extends Throwable>... expectedTypes) {
        Throwable cause = original.getCause();
        while (cause != null) {
            final Throwable checkCause = cause;
            if (Arrays.stream(expectedTypes).anyMatch(type -> type.isAssignableFrom(checkCause.getClass()))) {
                return cause;
            }
            cause = cause.getCause();
        }
        return original;
    }

    /**
     * Given a {@link Throwable}, and an expected type, return an optional that is populated if the original was an
     * instance of the expected type or was caused by the expected type.
     *
     * @param original The original throwable
     * @param expectedType The expected type to find
     * @return A completed {@link Optional} containing the found cause, or an empty {@link Optional}
     */
    public static <E extends Throwable> Optional<E> isOrCausedBy(
            @NotNull final Throwable original,
            @NotNull final Class<E> expectedType) {
        Throwable cause = original;
        while (cause != null) {
            if (expectedType.isAssignableFrom(cause.getClass())) {
                // noinspection unchecked
                return Optional.of((E) cause);
            }
            cause = cause.getCause();
        }
        return Optional.empty();
    }

    /**
     * Given an exception, provide a short description of the causes.
     *
     * We take each cause and return a String separated by line separator and "caused by".
     *
     * @param throwable the Throwable to get causes from
     * @param lineSeparator a separation string (e.g., newline or &lt;br&gt;)
     * @return the causes formatted one per line
     */
    public static String shortCauses(@NotNull Throwable throwable, String lineSeparator) {
        final StringBuilder builder = new StringBuilder();
        while (throwable != null) {
            final String cause = throwable.getMessage();
            if (builder.length() > 0) {
                builder.append(lineSeparator);
                builder.append("caused by ");
            }
            if (cause != null) {
                builder.append(cause);
            } else {
                builder.append(throwable.getClass().getName());
            }
            throwable = throwable.getCause();
        }
        return builder.toString();
    }

    /**
     * Given a throwable and a list of expected throwable types, traverse the cause tree and return the last exception
     * that matches the list of expected cause types.
     *
     * @param original the original Throwable
     * @param expectedTypes the list of expected types
     * @return the last Throwable of one of the defined types, or the original Throwable if none were found
     */
    @SafeVarargs
    public static Throwable findLastCause(Throwable original, Class<? extends Throwable>... expectedTypes) {
        Throwable cause = original.getCause();
        Throwable lastCause = original.getCause();
        while (cause != null) {
            final Throwable checkCause = cause;
            if (Arrays.stream(expectedTypes).anyMatch(type -> type.isAssignableFrom(checkCause.getClass()))) {
                lastCause = cause;
            }
            cause = cause.getCause();
        }
        return lastCause;
    }
}
