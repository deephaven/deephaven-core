//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.codec;

/**
 * Exception class for {@link CodecCache} item construction issues.
 */
public class CodecCacheException extends RuntimeException {

    CodecCacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
