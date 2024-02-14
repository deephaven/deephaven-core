/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Exception thrown when a column is not found.
 */
public class NoSuchColumnException extends IllegalArgumentException {

    public static final String DELIMITER = ", ";

    public static final String DEFAULT_FORMAT_STR = "Unknown column names [%s], available column names are [%s]";

    public enum Type {
        MISSING, AVAILABLE, REQUESTED
    }

    /**
     * Equivalent to {@code throwIf(available, Collections.singleton(requested))}.
     *
     * @param available the available columns
     * @param requested the requested columns
     * @see #throwIf(Set, Collection)
     */
    public static void throwIf(Set<String> available, String requested) {
        throwIf(available, Collections.singleton(requested));
    }

    /**
     * Equivalent to {@code throwIf(available, requested, DEFAULT_FORMAT_STR, Type.MISSING, Type.AVAILABLE)} where
     * {@code DEFAULT_FORMAT_STR} is {@value DEFAULT_FORMAT_STR}.
     *
     * @param available the available columns
     * @param requested the requested columns
     * @see #throwIf(Set, Collection, String, Type...)
     */
    public static void throwIf(Set<String> available, Collection<String> requested) {
        throwIf(available, requested, DEFAULT_FORMAT_STR, Type.MISSING, Type.AVAILABLE);
    }

    /**
     * Throws a {@link NoSuchColumnException} if any name from {@code requested} is not in {@code available}. The
     * message will be constructed by {@link String#join(CharSequence, Iterable) joining} the respective collection with
     * {@value DELIMITER} and presenting them to {@link String#format(String, Object...) format} in {@code types} order.
     *
     * @param available the available columns
     * @param requested the requested columns
     * @param formatStr the format string
     * @param types the collection types order for formatting
     */
    public static void throwIf(Set<String> available, Collection<String> requested, String formatStr, Type... types) {
        final List<String> missing = requested
                .stream()
                .filter(Predicate.not(available::contains))
                .collect(Collectors.toList());
        if (!missing.isEmpty()) {
            final Object[] formatArgs = new Object[types.length];
            for (int i = 0; i < types.length; ++i) {
                switch (types[i]) {
                    case MISSING:
                        formatArgs[i] = String.join(DELIMITER, missing);
                        break;
                    case AVAILABLE:
                        formatArgs[i] = String.join(DELIMITER, available);
                        break;
                    case REQUESTED:
                        formatArgs[i] = String.join(DELIMITER, requested);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected case " + types[i]);
                }
            }
            throw new NoSuchColumnException(String.format(formatStr, formatArgs));
        }
    }

    /**
     * Thrown when an operation can not find a required column(s).
     *
     * <p>
     * Callers may prefer to use {@link #throwIf(Set, Collection, String, Type...)} when applicable.
     *
     * @param message the message
     */
    public NoSuchColumnException(String message) {
        super(message);
    }

    /**
     * Thrown when an operation can not find a required column(s).
     *
     * <p>
     * Callers may prefer to use {@link #throwIf(Set, Collection)} when applicable.
     *
     * @param presentColumns the column names present in the table
     * @param missingColumns the request column names that were not found
     */
    public NoSuchColumnException(Collection<String> presentColumns, Collection<String> missingColumns) {
        this(String.format(DEFAULT_FORMAT_STR,
                String.join(DELIMITER, missingColumns),
                String.join(DELIMITER, presentColumns)));
    }

    /**
     * Thrown when an operation can not find a required column.
     *
     * @param presentColumns the column names present in the table
     * @param missingColumn the request column name that was not found
     */
    public NoSuchColumnException(Collection<String> presentColumns, String missingColumn) {
        this(presentColumns, Collections.singleton(missingColumn));
    }
}
