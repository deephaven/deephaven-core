/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;

import java.util.Date;

/**
 * {@link IndexableNumericData} dataset comprised of an array of {@link Date}s.
 *
 * Date values are accessed as nanoseconds from epoch. Data conversion to double means these values
 * are accurate to about 250 nanoseconds.
 */
public class IndexableNumericDataArrayDate extends IndexableNumericData {
    private static final long serialVersionUID = 7132588196200176807L;
    private final Date[] data;

    /**
     * Creates an IndexableNumericDataArrayDate instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataArrayDate(final Date[] data, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return data.length;
    }

    @Override
    public double get(final int index) {
        if (index >= data.length) {
            return Double.NaN;
        }
        final Date v = data[index];
        return v == null ? Double.NaN : v.getTime() * 1000000; // convert to nanos
    }
}
