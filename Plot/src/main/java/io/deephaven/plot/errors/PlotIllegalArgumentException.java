//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.errors;

/**
 * IllegalArgumentException that contains information about the plot.
 */
public class PlotIllegalArgumentException extends IllegalArgumentException {

    public PlotIllegalArgumentException(final String exception, final PlotExceptionCause exceptionCause) {
        this(exception, exceptionCause == null ? null : exceptionCause.getPlotInfo());
    }

    public PlotIllegalArgumentException(final String exception, final PlotInfo plotInfo) {
        super("" + (plotInfo == null || plotInfo.toString() == null ? "" : "Plot Information: " + plotInfo + " ")
                + exception);
    }
}
