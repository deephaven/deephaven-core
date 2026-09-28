//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import jsinterop.annotations.JsType;

/**
 * This enum describes what kind of chart is being drawn. This may limit what kinds of series can be found on it, or how
 * those series should be rendered.
 */
@JsType(name = "ChartType", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsChartType {
    /**
     * The chart will be drawn on a cartesian plane, with series items being plotting with X and Y positions, as lines,
     * scattered points, areas, error bars, etc.
     */
    public static final int XY = FigureDescriptor.ChartDescriptor.ChartType.XY.getNumber();
    /**
     * The chart will be a pie chart, with only pie series rendered on it.
     */
    public static final int PIE = FigureDescriptor.ChartDescriptor.ChartType.PIE.getNumber();

    /**
     * Similar to an XY plot, except that this will be TIME instead of X, and OPEN/HIGH/LOW/CLOSE instead of Y.
     */
    @Deprecated
    public static final int OHLC = FigureDescriptor.ChartDescriptor.ChartType.OHLC.getNumber();
    /**
     * Similar to a XY chart, except one of the axis will be based on a category instead of being numeric.
     */
    public static final int CATEGORY = FigureDescriptor.ChartDescriptor.ChartType.CATEGORY.getNumber();
    /**
     * Similar to XY, this chart will have three numeric dimensions, plotting each with an X, Y, and Z value.
     */
    public static final int XYZ = FigureDescriptor.ChartDescriptor.ChartType.XYZ.getNumber();
    /**
     * Similar to a CATEGORY chart, this will have two category axes, and one numeric axis.
     */
    public static final int CATEGORY_3D = FigureDescriptor.ChartDescriptor.ChartType.CATEGORY_3D.getNumber();
    public static final int TREEMAP = FigureDescriptor.ChartDescriptor.ChartType.TREEMAP.getNumber();
}
