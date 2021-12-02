package io.deephaven.util.datastructures;

@FunctionalInterface
public interface LongRangeAbortableConsumer {

    /**
     * Provides a range to this consumer. A false return value indicates that the application providing ranges to this
     * consumer should not invoke it again. As consecutive calls to accept are made, delivered ranges are assumed to be
     * non overlapping and increasing in their first value; in particular, the first of a range has to be strictly
     * greater than the last of the previous range.
     *
     * @param first the range first value.
     * @param last the range last value, inclusive.
     * @return false if don't want any more values after this one, true otherwise.
     */
    boolean accept(long first, long last);
}
