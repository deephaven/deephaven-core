package io.deephaven.db.exceptions;

import io.deephaven.db.v2.utils.Index;
import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.Nullable;

/**
 * <p>
 * Unchecked exception thrown when {@link Index}, {@link io.deephaven.db.tables.Table} or
 * {@link io.deephaven.db.tables.DataColumn} operations are invoked (directly, or indirectly as data updates) that
 * cannot be completed correctly due to size constraints on the underlying data structures.
 * <p>
 * For example, the current implementations of {@link io.deephaven.db.v2.utils.RedirectionIndex}, required for
 * {@link io.deephaven.db.tables.Table#sort}, can only support an 32-bit integer number of keys.
 */
public class SizeException extends UncheckedDeephavenException {

    /**
     * Construct an exception, with a message appropriate for the given arguments.
     * 
     * @param messagePrefix An optional prefix for the message
     * @param inputSize The input size for the message
     * @param maximumSize The maximum size for the message
     */
    public SizeException(@Nullable final String messagePrefix, final long inputSize, final long maximumSize) {
        super((messagePrefix == null ? "" : messagePrefix + ": ") + "Input size " + inputSize + " larger than maximum "
                + maximumSize);
    }

    /**
     * Construct an exception, with a message appropriate for the given arguments. Maximum size is assumed to be
     * {@link Integer#MAX_VALUE}.
     * 
     * @param messagePrefix An optional prefix for the message
     * @param inputSize The input size for the message
     */
    public SizeException(@Nullable final String messagePrefix, final long inputSize) {
        this(messagePrefix, inputSize, Integer.MAX_VALUE);
    }

    /**
     * Construct an exception, with a message appropriate for the given arguments. Maximum size is assumed to be
     * {@link Integer#MAX_VALUE}, and no prefix is included.
     * 
     * @param inputSize The input size for the message
     */
    @SuppressWarnings("unused")
    public SizeException(final long inputSize) {
        this(null, inputSize, Integer.MAX_VALUE);
    }
}
