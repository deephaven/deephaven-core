/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotExceptionCause;
import io.deephaven.db.plot.errors.PlotInfo;

import java.io.Serializable;
import java.util.Spliterator;
import java.util.function.DoubleConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;

/**
 * Dataset with indexed numeric values.
 */
public abstract class IndexableNumericData implements PlotExceptionCause, Serializable {

    private final PlotInfo plotInfo;

    /**
     * @param plotInfo plot information
     */
    public IndexableNumericData(final PlotInfo plotInfo) {
        this.plotInfo = plotInfo;
    }

    @Override
    public PlotInfo getPlotInfo() {
        return plotInfo;
    }

    /**
     * Gets the size of this dataset.
     *
     * @return size of this dataset
     */
    public abstract int size();

    /**
     * Gets the value at the specified {@code index} as a double.
     *
     * @param index index
     * @return value at {@code index} as a double
     */
    public abstract double get(int index);

    /**
     * Gets the iterator over this dataset.
     *
     * @return dataset iterator
     */
    public Spliterator.OfDouble doubleIterator() {
        return new Spliterator.OfDouble() {
            private int index = 0;

            @Override
            public OfDouble trySplit() {
                return null;
            }

            @Override
            public boolean tryAdvance(DoubleConsumer action) {
                if (index >= size()) {
                    return false;
                }

                action.accept(get(index++));
                return true;
            }

            @Override
            public long estimateSize() {
                return size();
            }

            @Override
            public int characteristics() {
                return Spliterator.IMMUTABLE;
            }
        };
    }

    /**
     * Gets the values of this dataset as a stream of doubles.
     *
     * @return stream of this dataset's values as doubles
     */
    public DoubleStream stream() {
        return StreamSupport.doubleStream(doubleIterator(), false);
    }
}
