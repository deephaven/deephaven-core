//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.FigureDescriptor;
import jsinterop.annotations.JsType;

/**
 * This enum describes the source it is in, and how this aspect of the data in the series should be used to render the
 * item. For example, a point in a error-bar plot might have a X value, three Y values (Y, Y_LOW, Y_HIGH), and some
 * COLOR per item - the three SeriesDataSources all would share the same Axis instance, but would have different
 * SourceType enums set. The exact meaning of each source type will depend on the series that they are in.
 */
@JsType(name = "SourceType", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsSourceType {
    /**
     * LINE, AREA, STACKED_LINE, STACKED_AREA, ERROR_BAR, HISTOGRAM, SCATTER, STEP. Also used in PIE, but only to
     * identify the correct axis.
     */
    public static final int X = FigureDescriptor.SourceType.getX();
    /**
     * LINE, AREA, STACKED_LINE, STACKED_AREA, ERROR_BAR, HISTOGRAM, SCATTER, STEP. Also used in PIE, but only to
     * identify the correct axis.
     */
    public static final int Y = FigureDescriptor.SourceType.getY();
    /**
     * STACKED_AREA, SCATTER
     */
    public static final int Z = FigureDescriptor.SourceType.getZ();
    /**
     * ERROR_BAR, HISTOGRAM
     */
    public static final int X_LOW = FigureDescriptor.SourceType.getX_LOW();
    /**
     * ERROR_BAR, HISTOGRAM
     */
    public static final int X_HIGH = FigureDescriptor.SourceType.getX_HIGH();
    /**
     * ERROR_BAR, HISTOGRAM
     */
    public static final int Y_LOW = FigureDescriptor.SourceType.getY_LOW();
    /**
     * ERROR_BAR, HISTOGRAM
     */
    public static final int Y_HIGH = FigureDescriptor.SourceType.getY_HIGH();
    /**
     * OHLC
     */
    public static final int TIME = FigureDescriptor.SourceType.getTIME();
    /**
     * OHLC
     */
    public static final int OPEN = FigureDescriptor.SourceType.getOPEN();
    /**
     * OHLC
     */
    public static final int HIGH = FigureDescriptor.SourceType.getHIGH();
    /**
     * OHLC
     */
    public static final int LOW = FigureDescriptor.SourceType.getLOW();
    /**
     * OHLC
     */
    public static final int CLOSE = FigureDescriptor.SourceType.getCLOSE();
    public static final int SHAPE = FigureDescriptor.SourceType.getSHAPE();
    public static final int SIZE = FigureDescriptor.SourceType.getSIZE();
    public static final int LABEL = FigureDescriptor.SourceType.getLABEL();
    public static final int COLOR = FigureDescriptor.SourceType.getCOLOR();
    public static final int PARENT = FigureDescriptor.SourceType.getPARENT();
    public static final int TEXT = FigureDescriptor.SourceType.getTEXT();
    public static final int HOVER_TEXT = FigureDescriptor.SourceType.getHOVER_TEXT();
}
