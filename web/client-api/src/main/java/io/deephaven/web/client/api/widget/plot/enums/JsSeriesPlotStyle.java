package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.web.shared.data.plot.SeriesPlotStyle;
import jsinterop.annotations.JsType;

@JsType(name = "SeriesPlotStyle")
@SuppressWarnings("unusable-by-js")
public class JsSeriesPlotStyle {
    public static final SeriesPlotStyle BAR = SeriesPlotStyle.BAR;
    public static final SeriesPlotStyle STACKED_BAR = SeriesPlotStyle.STACKED_BAR;
    public static final SeriesPlotStyle LINE = SeriesPlotStyle.LINE;
    public static final SeriesPlotStyle AREA = SeriesPlotStyle.AREA;
    public static final SeriesPlotStyle STACKED_AREA = SeriesPlotStyle.STACKED_AREA;
    public static final SeriesPlotStyle PIE = SeriesPlotStyle.PIE;
    public static final SeriesPlotStyle HISTOGRAM = SeriesPlotStyle.HISTOGRAM;
    public static final SeriesPlotStyle OHLC = SeriesPlotStyle.OHLC;
    public static final SeriesPlotStyle SCATTER = SeriesPlotStyle.SCATTER;
    public static final SeriesPlotStyle STEP = SeriesPlotStyle.STEP;
    public static final SeriesPlotStyle ERROR_BAR = SeriesPlotStyle.ERROR_BAR;
}
