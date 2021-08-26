/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import java.io.Serializable;

/**
 * Location of Series within a figure.
 */
public class SeriesLocation implements Serializable {
    private static final long serialVersionUID = 3878962519670912774L;

    private final AxesLocation axesLocation;
    private final int id;

    public SeriesLocation(final SeriesInternal series) {
        this.axesLocation = new AxesLocation(series.axes());
        this.id = series.id();
    }

    public SeriesInternal get(final BaseFigureImpl figure) {
        final AxesImpl axes = this.axesLocation.get(figure);
        return axes.series(this.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SeriesLocation that = (SeriesLocation) o;

        if (id != that.id)
            return false;
        return axesLocation != null ? axesLocation.equals(that.axesLocation) : that.axesLocation == null;
    }

    @Override
    public int hashCode() {
        int result = axesLocation != null ? axesLocation.hashCode() : 0;
        result = 31 * result + id;
        return result;
    }
}
