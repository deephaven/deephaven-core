//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.input;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * An implementation of {@link InputTableValidationException.StructuredError}.
 */
public class StructuredErrorImpl implements InputTableValidationException.StructuredError {
    private final String message;
    private final String column;
    private final long row;

    /**
     * Create a StructuredError for the provided parameters
     * 
     * @param message the error message
     * @param column the column, or null if unknown
     * @param row the row position, negative (canonically {@link io.deephaven.engine.rowset.RowSet#NULL_ROW_KEY}) if
     *        unknown
     */
    public StructuredErrorImpl(final String message, final String column, final long row) {
        this.message = message;
        this.column = column;
        this.row = row;
    }

    @Override
    public OptionalLong getRow() {
        return row < 0 ? OptionalLong.empty() : OptionalLong.of(row);
    }

    @Override
    public Optional<String> getColumn() {
        return Optional.ofNullable(column);
    }

    @Override
    public String getMessage() {
        return message;
    }
}
