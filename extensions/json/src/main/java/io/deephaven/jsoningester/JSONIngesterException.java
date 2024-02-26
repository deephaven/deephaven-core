package io.deephaven.jsoningester;

public class JSONIngesterException extends RuntimeException {
    public JSONIngesterException(String message) {
        super(message);
    }

    public JSONIngesterException(String message, Throwable cause) {
        super(message, cause);
    }
}
