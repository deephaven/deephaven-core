/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import java.nio.ByteBuffer;

// --------------------------------------------------------------------
/**
 * An object that can write itself to a byte buffer.
 */
public interface Writable {
    /** Writes this object to the given byte buffer. */
    void write(ByteBuffer byteBuffer);
}
