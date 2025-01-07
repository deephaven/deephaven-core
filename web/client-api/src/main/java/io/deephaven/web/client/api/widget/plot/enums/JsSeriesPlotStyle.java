//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.FigureDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "SeriesPlotStyle", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsSeriesPlotStyle {
    /**
     * Series items should each be draw as bars. Each item will have a unique category value to be drawn on the CATEGORY
     * axis, and a numeric value drawn on the NUMBER axis.
     */
    public static final int BAR = FigureDescriptor.SeriesPlotStyle.getBAR();
    /**
     * Like BAR, except there may be more than one such series, and they will share axes, and each subsequent series
     * should have its items drawn on top of the previous one.
     */
    public static final int STACKED_BAR = FigureDescriptor.SeriesPlotStyle.getSTACKED_BAR();
    /**
     * Series items will be drawn as points connected by a line, with two NUMBER axes.
     */
    public static final int LINE = FigureDescriptor.SeriesPlotStyle.getLINE();
    /**
     * Series items will be drawn as points connected by a line with a filled area under the line.
     */
    public static final int AREA = FigureDescriptor.SeriesPlotStyle.getAREA();
    /**
     * Like AREA
     */
    public static final int STACKED_AREA = FigureDescriptor.SeriesPlotStyle.getSTACKED_AREA();
    /**
     * Series items should each be draw as pie slices. Each item will have a unique category value to be drawn on the
     * CATEGORY axis, and a numeric value drawn on the NUMBER axis.
     */
    public static final int PIE = FigureDescriptor.SeriesPlotStyle.getPIE();
    /**
     * Series items with 6 data sources, three on X and three on Y, to represent the start/end/mid of each item.
     */
    public static final int HISTOGRAM = FigureDescriptor.SeriesPlotStyle.getHISTOGRAM();
    /**
     * Stands for "Open/High/Low/Close" series. Five numeric data sources exist, four on one axis (OPEN, HIGH, LOW,
     * CLOSE), and TIME on the other axis.
     */
    public static final int OHLC = FigureDescriptor.SeriesPlotStyle.getOHLC();
    /**
     * Series items will be individually drawn as points, one two or three NUMBER axes
     */
    public static final int SCATTER = FigureDescriptor.SeriesPlotStyle.getSCATTER();
    public static final int STEP = FigureDescriptor.SeriesPlotStyle.getSTEP();
    /**
     * Series items with 6 data sources, three on X and three on Y, to represent the low/mid/high of each item.
     */
    public static final int ERROR_BAR = FigureDescriptor.SeriesPlotStyle.getERROR_BAR();
    public static final int TREEMAP = FigureDescriptor.SeriesPlotStyle.getTREEMAP();
}
