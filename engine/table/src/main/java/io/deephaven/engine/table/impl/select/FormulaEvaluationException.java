/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

/**
 * Exception while evaluating user formulas.
 */
public class FormulaEvaluationException extends RuntimeException {
    public FormulaEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }
}
