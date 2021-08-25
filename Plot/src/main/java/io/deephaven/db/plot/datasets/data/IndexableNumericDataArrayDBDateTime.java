/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.tables.utils.DBDateTime;

/**
 * {@link IndexableNumericData} dataset comprised of an array of {@link DBDateTime}s.
 *
 * Date values are accessed as nanoseconds from epoch. Data conversion to double means these values are accurate to
 * about 250 nanoseconds.
 */
public class IndexableNumericDataArrayDBDateTime extends IndexableNumericData {
    private static final long serialVersionUID = 2006200987348909028L;
    private final DBDateTime[] data;

    /**
     * Creates an IndexableNumericDataArrayDBDateTime instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataArrayDBDateTime(DBDateTime[] data, final PlotInfo plotInfo) {
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
        DBDateTime v = data[i];
        if (v != null) {
            result = v.getNanos();
        }

        return result;
    }
}
