/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;

import java.util.List;

/**
 * {@link IndexableNumericData} dataset backed by a list of {@link Number}s. When accessed, data
 * values are converted to doubles before being returned.
 */
public class IndexableNumericDataListNumber<T extends Number> extends IndexableNumericData {
    private static final long serialVersionUID = -382291808039710173L;
    private final List<T> data;

    /**
     * Creates an IndexableNumericDataListNumber instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataListNumber(List<T> data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public double get(int i) {
        if (i >= data.size()) {
            return Double.NaN;
        }
        T v = data.get(i);
        return v == null ? Double.NaN : v.doubleValue();
    }
}
