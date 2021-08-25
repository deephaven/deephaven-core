/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;

import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * {@link IndexableNumericData} dataset backed by an array of longs. When accessed, data values are
 * converted to doubles before being returned.
 */
public class IndexableNumericDataArrayLong extends IndexableNumericData {
    private static final long serialVersionUID = -1725079204730360920L;
    private final long[] data;

    /**
     * Creates an IndexableNumericDataArrayLong instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataArrayLong(long[] data, final PlotInfo plotInfo) {
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
        return data[i] == NULL_LONG ? Double.NaN : data[i];
    }
}
