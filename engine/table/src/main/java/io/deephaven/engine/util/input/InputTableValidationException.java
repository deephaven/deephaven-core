//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.input;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Thrown when an InputTableUpdater detects an invalid modification.
 *
 * <p>
 * If an InputTableUpdater detects an invalid modification, by throwing this Exception a detailed error message can be
 * passed back to the gRPC client that initiated the operation. Using a list of {@link StructuredError structured
 * errors}, the client can indicate to the user which of the cells (if known) were invalid.
 * </p>
 */
public class InputTableValidationException extends UncheckedDeephavenException {
    /**
     * An error indicating that invalid data was entered; with an optional row position and column name.
     *
     * <p>
     * Implementers of {@link InputTableUpdater} may use {@link StructuredErrorImpl} as an implementation.
     * </p>
     * .
     */
    public interface StructuredError {
        /**
         * @return the row position in the table to add that was invalid; or {@link OptionalLong#empty() empty} if
         *         unknown.
         */
        OptionalLong getRow();

        /*
         * @return the column name in the table to add that was invalid; or {@link Optional#empty() empty} if unknown.
         */
        Optional<String> getColumn();

        /**
         * @return the error message for this validation failure.
         */
        String getMessage();
    }

    private final List<StructuredError> errors;

    /**
     * Create an InputTableValidationException with the given message.
     *
     * <p>
     * No structured errors are supplied.
     * </p>
     *
     * @param message the error message
     */
    public InputTableValidationException(final String message) {
        super(message);
        errors = Collections.emptyList();
    }

    /**
     * Create an InputTableValidationException with the given message.
     *
     * @param message the error message
     * @param errors the structured errors for reporting to callers
     */
    public InputTableValidationException(final String message, final List<StructuredError> errors) {
        super(message);
        this.errors = errors;
    }

    /**
     * Create an InputTableValidationException with the given errors.
     *
     * <p>
     * The exception message is generated from the list of errors, attempting to provide a user-actionable summary for
     * callers that do not interpret structured errors.
     * </p>
     *
     * @param errors the structured errors for reporting to callers
     */
    public InputTableValidationException(final List<StructuredError> errors) {
        this(generateMessage(errors), errors);
    }

    private static String generateMessage(final List<StructuredError> errors) {
        if (errors.isEmpty()) {
            return "Unknown InputTable validation error.";
        }
        final StringBuilder message = new StringBuilder();
        if (errors.size() == 1) {
            final StructuredError firstError = errors.get(0);
            message.append("Input Table validation error");
            return formatOneMessage(message, firstError).toString();
        }
        final Map<String, List<StructuredError>> errorMap = errors.stream()
                .collect(Collectors.toMap(StructuredError::getMessage, e -> new ArrayList<>(List.of(e)), (l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                }));
        if (errorMap.size() == 1) {
            final List<StructuredError> errs = errorMap.values().iterator().next();
            return combineSimilarErrors(errors, message, errs).toString();
        }

        return formatOneMessage(message.append(errors.size()).append(" validation errors occurred. First error "),
                errors.get(0)).toString();
    }

    private static StringBuilder combineSimilarErrors(final List<StructuredError> errors, final StringBuilder message,
            final List<StructuredError> errs) {
        message.append("Input Table validation error occurred ").append(errors.size()).append(" times at ");
        errs.sort(Comparator.comparingLong((final StructuredError e) -> {
            if (e.getRow().isPresent()) {
                return e.getRow().getAsLong();
            } else {
                return -1;
            }
        }).thenComparing((final StructuredError e) -> {
            if (e.getColumn().isPresent()) {
                return e.getColumn().get();
            } else {
                return "";
            }
        }));
        final StructuredError firstError = errs.get(0);
        final boolean sameRow = errors.stream().allMatch(e -> e.getRow() == firstError.getRow());
        final boolean sameColumn = errors.stream().allMatch(e -> e.getColumn().equals(firstError.getColumn()));
        if (sameColumn) {
            if (sameRow) {
                // same for everything
                if (firstError.getRow().isEmpty()) {
                    if (firstError.getColumn().isEmpty()) {
                        message.append(" unknown location");
                    } else {
                        message.append(" column ").append(firstError.getColumn().get());
                    }
                } else {
                    message.append("row ").append(firstError.getRow().getAsLong());
                    if (firstError.getColumn().isPresent()) {
                        message.append(" column ").append(firstError.getColumn().get());
                    }
                }
            } else {
                // different rows, same column
                message.append("rows ").append(errors.stream().filter(e -> e.getRow().isPresent())
                        .map(e -> Long.toString(e.getRow().getAsLong())).collect(Collectors.joining(", ")));
                final long rowUnknown = errors.stream().filter(e -> e.getRow().isEmpty()).count();
                if (rowUnknown > 0) {
                    message.append(" and ").append(rowUnknown).append(" others");
                }
                if (firstError.getColumn().isPresent()) {
                    message.append(" column ").append(firstError.getColumn().get());
                }
            }
        } else {
            if (sameRow) {
                // same row, different columns
                if (firstError.getRow().isPresent()) {
                    message.append("row ").append(firstError.getRow().getAsLong());
                }
                message.append("columns ").append(errors.stream().filter(e -> e.getColumn().isPresent())
                        .map(e -> e.getColumn().get()).collect(Collectors.joining(", ")));
                final long columnUnknown = errors.stream().filter(e -> e.getColumn().isEmpty()).count();
                if (columnUnknown > 0) {
                    message.append(" and ").append(columnUnknown).append(" others");
                }
            } else {
                // different everything
                message.append("rows x columns ")
                        .append(errors.stream()
                                .map(e -> (e.getRow().isPresent() ? Long.toString(e.getRow().getAsLong()) : "unknown")
                                        + " x " + e.getColumn().orElse("unknown"))
                                .collect(Collectors.joining(", ")));
            }
        }
        return message.append(": ").append(errs.get(0).getMessage());
    }

    private static @NotNull StringBuilder formatOneMessage(final StringBuilder message,
            final StructuredError firstError) {
        if (firstError.getRow().isPresent()) {
            message.append(" at row ").append(firstError.getRow().getAsLong());
            if (firstError.getColumn().isPresent()) {
                message.append(" column ").append(firstError.getColumn().get());
            }
        } else if (firstError.getColumn().isPresent()) {
            message.append(" at column ").append(firstError.getColumn().get());
        } else {
            message.append(" at unknown location");
        }
        message.append(": ").append(firstError.getMessage());
        return message;
    }

    public Collection<StructuredError> getErrors() {
        return errors;
    }
}
