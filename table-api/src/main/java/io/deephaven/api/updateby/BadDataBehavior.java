package io.deephaven.api.updateby;

/**
 * Directives for how to handle {@code null} and {@code NaN} values while processing EMAs
 */
public enum BadDataBehavior {
    /** Reset the state for the bucket to {@code null} when invalid data is encountered */
    RESET,

    /** Skip and do not process the invalid data without changing state */
    SKIP,

    /** Throw an exception and abort processing when bad data is encountered */
    THROW,

    /** Allow the bad data to poison the result. This is only valid for use with NaN */
    POISON
}
