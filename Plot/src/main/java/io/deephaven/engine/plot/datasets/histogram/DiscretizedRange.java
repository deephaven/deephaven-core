/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.plot.datasets.histogram;

/**
 * Range which has been split into indexed bins (subranges).
 */
public interface DiscretizedRange {

    /**
     * Gets the min value of the bin at {@code rowSet}.
     *
     * @param index rowSet
     * @return min value of the bin specified by {@code rowSet}
     */
    double binMin(long index);

    /**
     * Gets the max value of the bin at {@code rowSet}.
     *
     * @param index rowSet
     * @return max value of the bin specified by {@code rowSet}
     */
    double binMax(long index);

    /**
     * Gets the rowSet of the bin the {@code value} lies in.
     *
     * @param value value
     * @return max value of the bin specified by {@code rowSet}
     */
    long index(double value);
}
