package io.deephaven.db.tables.verify;

public class TableAssertionFailure extends RuntimeException {
    TableAssertionFailure(String message) {
        super(message);
    }

    TableAssertionFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
