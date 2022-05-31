package io.deephaven.util.datastructures;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.Nullable;

/**
 * <p>
 * Unchecked exception thrown when operations are invoked (directly or indirectly) that cannot be completed correctly
 * due to size constraints on the underlying data structures. Most commonly this occurs when a
 * {@link LongSizedDataStructure} cannot perform some operations if the input size exceeds {@value Integer#MAX_VALUE}.
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
