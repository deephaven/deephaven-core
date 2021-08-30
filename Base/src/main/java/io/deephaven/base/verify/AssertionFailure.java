/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.verify;

// --------------------------------------------------------------------
/**
 * {@link RuntimeException} to be thrown on assertion failures.
 */
public class AssertionFailure extends RuntimeException {

    public AssertionFailure(String message) {
        super(message);
    }

    public AssertionFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
