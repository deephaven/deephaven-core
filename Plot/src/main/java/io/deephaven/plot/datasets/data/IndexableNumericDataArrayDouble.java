/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

/**
 * {@link IndexableNumericData} dataset backed by an array of doubles.
 */
public class IndexableNumericDataArrayDouble extends IndexableNumericData {
    private static final long serialVersionUID = 6885562283145177741L;
    private final double[] data;

    /**
     * Creates an IndexableNumericDataArrayDouble instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataArrayDouble(double[] data, final PlotInfo plotInfo) {
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
        return data[i] == NULL_DOUBLE ? Double.NaN : data[i];
    }
}
