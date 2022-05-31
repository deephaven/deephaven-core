/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;


import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * {@link IndexableNumericData} dataset backed by an array of shorts. When accessed, data values are converted to
 * doubles before being returned.
 */
public class IndexableNumericDataArrayShort extends IndexableNumericData {
    private static final long serialVersionUID = 2340751903216609352L;
    private final short[] data;

    /**
     * Creates an IndexableNumericDataArrayShort instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataArrayShort(short[] data, final PlotInfo plotInfo) {
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
        return data[i] == NULL_SHORT ? Double.NaN : data[i];
    }
}
