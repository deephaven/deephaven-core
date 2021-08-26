/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import java.io.Serializable;

/**
 * Location of Axis within a figure.
 */
public class AxisLocation implements Serializable {
    private static final long serialVersionUID = 9034132734092464678L;

    private final ChartLocation chartLocation;
    private final int dim;
    private final int id;

    AxisLocation(final AxisImpl axis) {
        this.chartLocation = new ChartLocation(axis.chart());
        this.dim = axis.dim();
        this.id = axis.id();
    }

    AxisImpl get(final BaseFigureImpl figure) {
        final ChartImpl chart = chartLocation.get(figure);
        return chart.getAxis()[dim].get(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AxisLocation that = (AxisLocation) o;

        if (dim != that.dim)
            return false;
        if (id != that.id)
            return false;
        return chartLocation != null ? chartLocation.equals(that.chartLocation)
            : that.chartLocation == null;
    }

    @Override
    public int hashCode() {
        int result = chartLocation != null ? chartLocation.hashCode() : 0;
        result = 31 * result + dim;
        result = 31 * result + id;
        return result;
    }
}
