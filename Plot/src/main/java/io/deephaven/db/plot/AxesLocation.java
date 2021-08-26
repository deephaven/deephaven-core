/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import java.io.Serializable;

/**
 * Location of Axes within a figure.
 */
class AxesLocation implements Serializable {
    private static final long serialVersionUID = 3878962519670912774L;

    private final ChartLocation chartLocation;
    private final int id;

    AxesLocation(final AxesImpl axes) {
        this.chartLocation = new ChartLocation(axes.chart());
        this.id = axes.id();
    }

    AxesImpl get(final BaseFigureImpl figure) {
        final ChartImpl chart = chartLocation.get(figure);
        return chart.getAxes().get(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AxesLocation that = (AxesLocation) o;

        if (id != that.id)
            return false;
        return chartLocation != null ? chartLocation.equals(that.chartLocation) : that.chartLocation == null;
    }

    @Override
    public int hashCode() {
        int result = chartLocation != null ? chartLocation.hashCode() : 0;
        result = 31 * result + id;
        return result;
    }
}
