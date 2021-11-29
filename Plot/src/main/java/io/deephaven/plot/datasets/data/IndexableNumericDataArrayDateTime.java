/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.time.DateTime;

/**
 * {@link IndexableNumericData} dataset comprised of an array of {@link DateTime}s.
 *
 * Date values are accessed as nanoseconds from epoch. Data conversion to double means these values are accurate to
 * about 250 nanoseconds.
 */
public class IndexableNumericDataArrayDateTime extends IndexableNumericData {
    private static final long serialVersionUID = 2006200987348909028L;
    private final DateTime[] data;

    /**
     * Creates an IndexableNumericDataArrayDateTime instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataArrayDateTime(DateTime[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public double get(final int i) {
        if (i >= data.length) {
            return Double.NaN;
        }
        double result = Double.NaN;
        DateTime v = data[i];
        if (v != null) {
            result = v.getNanos();
        }

        return result;
    }
}
