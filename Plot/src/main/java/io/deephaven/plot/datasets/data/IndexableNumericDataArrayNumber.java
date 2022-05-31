/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;

/**
 * {@link IndexableNumericData} dataset backed by an array of {@link Number}s. When accessed, data values are converted
 * to doubles before being returned.
 */
public class IndexableNumericDataArrayNumber<T extends Number> extends IndexableNumericData {
    private static final long serialVersionUID = -4587124538812025714L;
    private final T[] data;

    /**
     * Creates an IndexableNumericDataArrayNumber instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataArrayNumber(T[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public double get(int i) {
        if (i >= data.length) {
            return Double.NaN;
        }
        T v = data[i];
        return v == null ? Double.NaN : v.doubleValue();
    }
}
