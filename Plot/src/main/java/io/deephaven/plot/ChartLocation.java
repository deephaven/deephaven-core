/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import java.io.Serializable;

/**
 * Location of Chart within a figure.
 */
class ChartLocation implements Serializable {
    private static final long serialVersionUID = 1594957448496772360L;

    private final int row;
    private final int col;

    ChartLocation(final ChartImpl chart) {
        this.row = chart.row();
        this.col = chart.column();
    }

    ChartImpl get(final BaseFigureImpl figure) {
        final ChartArray charts = figure.getCharts();

        return charts.getChart(row, col);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ChartLocation location = (ChartLocation) o;

        if (row != location.row)
            return false;
        return col == location.col;
    }

    @Override
    public int hashCode() {
        int result = row;
        result = 31 * result + col;
        return result;
    }
}
