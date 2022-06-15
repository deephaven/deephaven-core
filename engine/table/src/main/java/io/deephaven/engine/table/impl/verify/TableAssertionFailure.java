/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.verify;

public class TableAssertionFailure extends RuntimeException {
    TableAssertionFailure(String message) {
        super(message);
    }

    TableAssertionFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
