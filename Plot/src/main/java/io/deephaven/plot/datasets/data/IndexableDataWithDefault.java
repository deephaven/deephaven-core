/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;

/**
 * {@link IndexableData} with a default value for missing indices and nulls.
 */
public class IndexableDataWithDefault<T> extends IndexableData<T> {

    private static final long serialVersionUID = -2083123815426589347L;
    private T defaultValue;
    private IndexableData<? extends T> specificValues;

    /**
     * @param plotInfo plot information
     */
    public IndexableDataWithDefault(PlotInfo plotInfo) {
        super(plotInfo);
    }

    @Override
    public int size() {
        return specificValues == null ? Integer.MAX_VALUE : specificValues.size();
    }

    @Override
    public T get(final int index) {
        if (specificValues == null) {
            return defaultValue;
        } else {
            T v = specificValues.get(index);
            return v == null ? defaultValue : v;
        }
    }

    /**
     * Defines the default value for this dataset.
     *
     * @param v value
     */
    public void setDefault(final T v) {
        defaultValue = v;
    }

    /**
     * Gets the default value for this dataset.
     *
     * @return default value for this dataset
     */
    public T getDefaultValue() {
        return defaultValue;
    }

    /**
     * Sets this datasets indexed values.
     *
     * If infinite is true, null values are returned for out-of-bounds indices. If not, an exception will be thrown.
     *
     * @param specificValues data
     * @param infinite if this dataset should return nulls for out-of-bounds indices.
     * @param <TT> type of the data in {@code specificValues}
     */
    public <TT extends T> void setSpecific(final IndexableData<TT> specificValues, final boolean infinite) {
        if (infinite) {
            this.specificValues = new IndexableDataInfinite<>(specificValues);
        } else {
            this.specificValues = specificValues;
        }
    }

    /**
     * Sets this datasets values equal to the values in another dataset.
     *
     * @param data dataset to get values from.
     */
    public void set(final IndexableDataWithDefault<T> data) {
        this.defaultValue = data.defaultValue;
        this.specificValues = data.specificValues;
    }


}
