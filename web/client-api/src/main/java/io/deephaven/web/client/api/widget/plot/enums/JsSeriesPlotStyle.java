package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FigureDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "SeriesPlotStyle")
@SuppressWarnings("unusable-by-js")
public class JsSeriesPlotStyle {
    public static final int BAR = FigureDescriptor.SeriesPlotStyle.getBAR();
    public static final int STACKED_BAR = FigureDescriptor.SeriesPlotStyle.getSTACKED_BAR();
    public static final int LINE = FigureDescriptor.SeriesPlotStyle.getLINE();
    public static final int AREA = FigureDescriptor.SeriesPlotStyle.getAREA();
    public static final int STACKED_AREA = FigureDescriptor.SeriesPlotStyle.getSTACKED_AREA();
    public static final int PIE = FigureDescriptor.SeriesPlotStyle.getPIE();
    public static final int HISTOGRAM = FigureDescriptor.SeriesPlotStyle.getHISTOGRAM();
    public static final int OHLC = FigureDescriptor.SeriesPlotStyle.getOHLC();
    public static final int SCATTER = FigureDescriptor.SeriesPlotStyle.getSCATTER();
    public static final int STEP = FigureDescriptor.SeriesPlotStyle.getSTEP();
    public static final int ERROR_BAR = FigureDescriptor.SeriesPlotStyle.getERROR_BAR();
}
