/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.errors;

/**
 * IllegalStateException that contains information about the plot.
 */
public class PlotIllegalStateException extends IllegalStateException {

    public PlotIllegalStateException(final String exception, final PlotExceptionCause exceptionCause) {
        this(exception, exceptionCause == null ? null : exceptionCause.getPlotInfo());
    }

    public PlotIllegalStateException(final PlotInfo exceptionCause) {
        super(exceptionCause == null ? null : exceptionCause.toString());
    }

    public PlotIllegalStateException(final String exception, final PlotInfo plotInfo) {
        super("" + (plotInfo == null || plotInfo.toString() == null ? "" : "Plot Information: " + plotInfo + " ")
                + exception);
    }
}
