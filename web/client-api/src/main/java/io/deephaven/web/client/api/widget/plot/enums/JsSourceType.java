//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
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
    public static final int X = FigureDescriptor.SourceType.X.getNumber();
    /**
     * LINE, AREA, STACKED_LINE, STACKED_AREA, ERROR_BAR, HISTOGRAM, SCATTER, STEP. Also used in PIE, but only to
     * identify the correct axis.
     */
    public static final int Y = FigureDescriptor.SourceType.Y.getNumber();
    /**
     * STACKED_AREA, SCATTER
     */
    public static final int Z = FigureDescriptor.SourceType.Z.getNumber();
    /**
     * ERROR_BAR, HISTOGRAM
     */
    public static final int X_LOW = FigureDescriptor.SourceType.X_LOW.getNumber();
    /**
     * ERROR_BAR, HISTOGRAM
     */
    public static final int X_HIGH = FigureDescriptor.SourceType.X_HIGH.getNumber();
    /**
     * ERROR_BAR, HISTOGRAM
     */
    public static final int Y_LOW = FigureDescriptor.SourceType.Y_LOW.getNumber();
    /**
     * ERROR_BAR, HISTOGRAM
     */
    public static final int Y_HIGH = FigureDescriptor.SourceType.Y_HIGH.getNumber();
    /**
     * OHLC
     */
    public static final int TIME = FigureDescriptor.SourceType.TIME.getNumber();
    /**
     * OHLC
     */
    public static final int OPEN = FigureDescriptor.SourceType.OPEN.getNumber();
    /**
     * OHLC
     */
    public static final int HIGH = FigureDescriptor.SourceType.HIGH.getNumber();
    /**
     * OHLC
     */
    public static final int LOW = FigureDescriptor.SourceType.LOW.getNumber();
    /**
     * OHLC
     */
    public static final int CLOSE = FigureDescriptor.SourceType.CLOSE.getNumber();
    public static final int SHAPE = FigureDescriptor.SourceType.SHAPE.getNumber();
    public static final int SIZE = FigureDescriptor.SourceType.SIZE.getNumber();
    public static final int LABEL = FigureDescriptor.SourceType.LABEL.getNumber();
    public static final int COLOR = FigureDescriptor.SourceType.COLOR.getNumber();
    public static final int PARENT = FigureDescriptor.SourceType.PARENT.getNumber();
    public static final int TEXT = FigureDescriptor.SourceType.TEXT.getNumber();
    public static final int HOVER_TEXT = FigureDescriptor.SourceType.HOVER_TEXT.getNumber();
}
