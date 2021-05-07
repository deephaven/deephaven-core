/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Interface to abstract-away local or remote file access.
 */
public interface FileAccessor extends NamedImplementation, LogOutputAppendable {

    /**
     * The "null" size hint.
     */
    long NULL_REQUIRED_SIZE = Long.MIN_VALUE;

    /**
     * Request an up-to-date size value for the file data space backing this accessor.
     *
     * @param requiredSize Hint about the minimum size the caller would like as a result
     * @return The size of the file data space backing this accessor
     */
    long size(long requiredSize);

    /**
     * Fill the supplied buffer with data starting at the supplied offset (position) into this file accessor.
     * A successful invocation will fill the buffer from buffer.position(), inclusive, to at least buffer.limit(),
     * exclusive, possibly up to buffer.capacity(), exclusive.
     *
     * @param buffer A buffer with state appropriate for a call to FileChannel.read()
     * @param position The start position in this file accessor's data space to read from
     * @return The number of bytes read, or -1 on error
     */
    int read(ByteBuffer buffer, long position);

    /**
     * Write the supplied buffer (from buffer.position(), inclusive, to buffer.limit(), exclusive), starting at the
     * supplied offset (position) into this file accessor.
     * @param buffer A buffer with state appropriate for a call to FileChannel.write()
     * @param position The start position in this file accessor's data space to write to
     * @return The number of bytes written, or -1 on error
     */
    default int write(@NotNull ByteBuffer buffer, long position) {
        throw new UnsupportedOperationException(this + " file accessor not writable");
    }

    /**
     * Truncate the file data space backing this accessor to the supplied size.
     *
     * @param size The new size
     */
    default void truncate(final long size) {
        throw new UnsupportedOperationException(this + " file accessor not writable");
    }

    /**
     * Make sure any previous writes to the underlying file through this file accessor are persisted, synchronously.
     */
    default void force() {
        throw new UnsupportedOperationException(this + " file accessor not writable");
    }

    /**
     * Get an offset view into this FileAccessor.
     *
     * @param startOffset The offset
     * @return The new offset view accessor
     */
    default FileAccessor getOffsetView(final long startOffset) {
        return new OffsetFileAccessor<>(this, startOffset);
    }
}
