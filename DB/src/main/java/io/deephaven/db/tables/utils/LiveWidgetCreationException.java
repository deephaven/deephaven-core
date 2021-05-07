package io.deephaven.db.tables.utils;

/**
 * This exception should be thrown when an issue occurs creating a LiveWidget.
 */
public class LiveWidgetCreationException extends RuntimeException {
    public LiveWidgetCreationException(String message, Throwable cause) {
        super(message, cause);
    }

    public LiveWidgetCreationException(Throwable cause) {
        super(cause);
    }

    public LiveWidgetCreationException(String message) {
        super(message);
    }
}
