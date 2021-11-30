package io.deephaven.util.datastructures;

@FunctionalInterface
public interface LongRangeConsumer {

    /**
     * Provides a range to this consumer. As consecutive calls to accept are made, delivered ranges are assumed to be
     * non overlapping and increasing in their first value; in particular, the first of a range has to be strictly
     * greater than the last of the previous range.
     *
     * @param first the range first value.
     * @param last the range last value.
     */
    void accept(long first, long last);
}
