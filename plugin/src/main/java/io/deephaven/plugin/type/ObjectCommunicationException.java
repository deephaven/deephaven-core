package io.deephaven.plugin.type;

/**
 * Thrown when errors occur when communicating with the client. This is a checked exception to ensure it doesn't escape
 * to somewhere that could crash part of the server.
 */
public class ObjectCommunicationException extends Exception {
    public ObjectCommunicationException() {
        super();
    }

    public ObjectCommunicationException(String message) {
        super(message);
    }

    public ObjectCommunicationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ObjectCommunicationException(Throwable cause) {
        super(cause);
    }
}
