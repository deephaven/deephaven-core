//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.UncheckedDeephavenException;

/**
 * Exception while compiling user formulas.
 */
public class FormulaCompilationException extends UncheckedDeephavenException {
    public FormulaCompilationException(String message) {
        super(message);
    }

    public FormulaCompilationException(String message, Throwable cause) {
        super(message, cause);
    }
}
