package io.deephaven.util.codec;

/**
 * Exception class for {@link CodecCache} item construction issues.
 */
public class CodecCacheException extends RuntimeException {

    CodecCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
