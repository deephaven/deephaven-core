//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;

/**
 * {@link IndexableData} dataset backed by an array.
 */
public class IndexableDataArray<T> extends IndexableData<T> {
    private static final long serialVersionUID = -3605356450513219514L;
    private final T[] data;

    /**
     * Creates an IndexableDataArray instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableDataArray(T[] data, final PlotInfo plotInfo) {
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
        return data[index];
    }
}
