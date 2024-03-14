//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;
import io.deephaven.time.DateTimeUtils;

import java.time.Instant;

/**
 * {@link IndexableNumericData} dataset comprised of an array of {@link Instant instants}.
 *
 * Date values are accessed as nanoseconds from epoch. Data conversion to double means these values are accurate to
 * about 250 nanoseconds.
 */
public class IndexableNumericDataArrayInstant extends IndexableNumericData {
    private static final long serialVersionUID = 2006200987348909028L;
    private final Instant[] data;

    /**
     * Creates an IndexableNumericDataArrayInstant instance.
     *
     * @throws io.deephaven.base.verify.RequirementFailure {@code data} must not be null
     * @param data data
     * @param plotInfo plot information
     */
    public IndexableNumericDataArrayInstant(Instant[] data, final PlotInfo plotInfo) {
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
        Instant v = data[i];
        if (v != null) {
            result = DateTimeUtils.epochNanos(v);
        }

        return result;
    }
}
