package io.deephaven;

/**
 * The root of all Checked Deephaven exceptions.
 */
public class DeephavenException extends Exception {
    public DeephavenException() {
        super();
    }

    public DeephavenException(String reason) {
        super(reason);
    }

    public DeephavenException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public DeephavenException(Throwable cause) {
        super(cause);
    }
}
