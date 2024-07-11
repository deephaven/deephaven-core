//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.verify;

// ----------------------------------------------------------------
/**
 * {@link RuntimeException} to be thrown on requirement failures.
 */
public class RequirementFailure extends RuntimeException {

    public RequirementFailure(String message) {
        super(message);
    }

    public RequirementFailure(String message, Exception caughtException) {
        super(message, caughtException);
    }
}
