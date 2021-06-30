/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package com.google.protobuf;

import java.nio.ByteBuffer;

public class ByteStringAccess {
    /**
     * Wraps the given bytes into a {@link ByteString}.
     */
    public static ByteString wrap(ByteBuffer buffer) {
        return ByteString.wrap(buffer);
    }

    /**
     * Wraps the given bytes into a {@link ByteString}.
     */
    public static ByteString wrap(byte[] bytes) {
        return ByteString.wrap(bytes);
    }

    /**
     * Wraps the given bytes into a {@link ByteString}.
     */
    public static ByteString wrap(byte[] bytes, int offset, int length) {
        return ByteString.wrap(bytes, offset, length);
    }
}
