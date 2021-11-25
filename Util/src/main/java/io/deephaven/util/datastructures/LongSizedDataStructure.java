package io.deephaven.util.datastructures;

import org.jetbrains.annotations.NotNull;

/**
 * Common interface for Deephaven data structures with a long (64-bit signed integer) size.
 */
public interface LongSizedDataStructure {

    /**
     * The size of this data structure.
     * 
     * @return The size
     */
    long size();

    /**
     * <p>
     * Returns {@link #size()} cast to an int, if this can be done without losing precision. Otherwise, throws
     * {@link SizeException}.
     * 
     * @return {@link #size()}, cast to an int
     * @throws SizeException If {@link #size()} cannot be cast without precision loss
     */
    default int intSize() {
        return intSize("int cast");
    }

    /**
     * <p>
     * Returns {@link #size()} cast to an int, if this can be done without losing precision. Otherwise, throws
     * {@link SizeException}.
     * 
     * @param operation A description of the operation that requires an int size
     * @return {@link #size()}, cast to an int
     * @throws SizeException If {@link #size()} cannot be cast without precision loss
     */
    default int intSize(@NotNull final String operation) {
        return intSize(operation, size());
    }

    /**
     * Returns the size argument cast to an int, if this can be done without losing precision. Otherwise, throws
     * {@link SizeException}.
     * 
     * @param operation A description of the operation that requires an int size
     * @param size The size
     * @return size, cast to an int
     * @throws SizeException If size cannot be cast without precision loss
     */
    static int intSize(@NotNull final String operation, final long size) {
        if (size <= Integer.MAX_VALUE) {
            return (int) size;
        }
        throw new SizeException(operation + " cannot proceed without losing precision", size);
    }
}
