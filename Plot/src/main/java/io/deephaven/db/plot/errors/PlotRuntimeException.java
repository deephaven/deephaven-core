/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.errors;

import io.deephaven.UncheckedDeephavenException;

/**
 * RuntimeException which contains information about the plot.
 */
public class PlotRuntimeException extends UncheckedDeephavenException {

    public PlotRuntimeException(final String exception, final PlotExceptionCause exceptionCause) {
        this(exception, exceptionCause == null ? null : exceptionCause.getPlotInfo());
    }

    public PlotRuntimeException(final String exception, final Throwable cause,
            final PlotExceptionCause exceptionCause) {
        this(exception, cause, exception == null ? null : exceptionCause.getPlotInfo());
    }

    public PlotRuntimeException(final String exception, final Throwable cause, final PlotInfo plotInfo) {
        super("" + (plotInfo == null || plotInfo.toString() == null ? "" : "Plot Information: " + plotInfo + " ")
                + exception, cause);
    }

    public PlotRuntimeException(final String exception, final PlotInfo plotInfo) {
        super("" + (plotInfo == null || plotInfo.toString() == null ? "" : "Plot Information: " + plotInfo + " ")
                + exception);
    }
}
