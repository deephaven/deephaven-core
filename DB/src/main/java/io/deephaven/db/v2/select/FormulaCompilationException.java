package io.deephaven.db.v2.select;

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
