package io.deephaven.jsoningester;

/**
 * Created by rbasralian on 9/28/22
 */
public class JSONIngesterException extends RuntimeException {
    public JSONIngesterException(String message) {
        super(message);
    }

    public JSONIngesterException(String message, Throwable cause) {
        super(message, cause);
    }
}
