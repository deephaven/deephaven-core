/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.histogram;

/**
 * Range which has been split into indexed bins (subranges).
 */
public interface DiscretizedRange {

    /**
     * Gets the min value of the bin at {@code index}.
     *
     * @param index index
     * @return min value of the bin specified by {@code index}
     */
    double binMin(long index);

    /**
     * Gets the max value of the bin at {@code index}.
     *
     * @param index index
     * @return max value of the bin specified by {@code index}
     */
    double binMax(long index);

    /**
     * Gets the index of the bin the {@code value} lies in.
     *
     * @param value value
     * @return max value of the bin specified by {@code index}
     */
    long index(double value);
}
