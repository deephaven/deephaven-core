/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;

import static io.deephaven.db.plot.util.NullCategory.INSTANCE;

/**
 * {@link IndexableData} dataset backed by an array. If the array contains a null value, return a NULL_CATEGORY.
 */
public class IndexableDataArrayNullCategory<T> extends IndexableData<T> {
    private static final long serialVersionUID = -3605356450513219514L;
    private final T[] data;

    /**
     * Creates an IndexableDataArray instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableDataArrayNullCategory(T[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public T get(int index) {
        final T value = data[index];
        return value == null ? (T) INSTANCE : value;
    }
}
