/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.errors;

/**
 * RuntimeException which occurs when rendering the plot.
 */
public class PlotRenderingException extends RuntimeException {

    public PlotRenderingException(final PlotRuntimeException e) {
        super(e);
    }

    public PlotRenderingException(final Throwable cause, final PlotInfo plotInfo) {
        super("" + (plotInfo == null || plotInfo.toString() == null ? ""
            : "Plot Information: " + plotInfo), cause);
    }
}
