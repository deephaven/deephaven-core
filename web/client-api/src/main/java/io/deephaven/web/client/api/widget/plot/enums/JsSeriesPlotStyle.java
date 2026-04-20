//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "SeriesPlotStyle", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsSeriesPlotStyle {
    /**
     * Series items should each be draw as bars. Each item will have a unique category value to be drawn on the CATEGORY
     * axis, and a numeric value drawn on the NUMBER axis.
     */
    public static final int BAR = FigureDescriptor.SeriesPlotStyle.BAR.getNumber();
    /**
     * Like BAR, except there may be more than one such series, and they will share axes, and each subsequent series
     * should have its items drawn on top of the previous one.
     */
    public static final int STACKED_BAR = FigureDescriptor.SeriesPlotStyle.STACKED_BAR.getNumber();
    /**
     * Series items will be drawn as points connected by a line, with two NUMBER axes.
     */
    public static final int LINE = FigureDescriptor.SeriesPlotStyle.LINE.getNumber();
    /**
     * Series items will be drawn as points connected by a line with a filled area under the line.
     */
    public static final int AREA = FigureDescriptor.SeriesPlotStyle.AREA.getNumber();
    /**
     * Like AREA
     */
    public static final int STACKED_AREA = FigureDescriptor.SeriesPlotStyle.STACKED_AREA.getNumber();
    /**
     * Series items should each be draw as pie slices. Each item will have a unique category value to be drawn on the
     * CATEGORY axis, and a numeric value drawn on the NUMBER axis.
     */
    public static final int PIE = FigureDescriptor.SeriesPlotStyle.PIE.getNumber();
    /**
     * Series items with 6 data sources, three on X and three on Y, to represent the start/end/mid of each item.
     */
    public static final int HISTOGRAM = FigureDescriptor.SeriesPlotStyle.HISTOGRAM.getNumber();
    /**
     * Stands for "Open/High/Low/Close" series. Five numeric data sources exist, four on one axis (OPEN, HIGH, LOW,
     * CLOSE), and TIME on the other axis.
     */
    public static final int OHLC = FigureDescriptor.SeriesPlotStyle.OHLC.getNumber();
    /**
     * Series items will be individually drawn as points, one two or three NUMBER axes
     */
    public static final int SCATTER = FigureDescriptor.SeriesPlotStyle.SCATTER.getNumber();
    public static final int STEP = FigureDescriptor.SeriesPlotStyle.STEP.getNumber();
    /**
     * Series items with 6 data sources, three on X and three on Y, to represent the low/mid/high of each item.
     */
    public static final int ERROR_BAR = FigureDescriptor.SeriesPlotStyle.ERROR_BAR.getNumber();
    public static final int TREEMAP = FigureDescriptor.SeriesPlotStyle.TREEMAP.getNumber();
}
