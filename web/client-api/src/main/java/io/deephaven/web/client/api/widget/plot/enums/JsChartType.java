//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.ChartDescriptor;
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
    public static final int XY = ChartDescriptor.ChartType.getXY();
    /**
     * The chart will be a pie chart, with only pie series rendered on it.
     */
    public static final int PIE = ChartDescriptor.ChartType.getPIE();

    /**
     * Similar to an XY plot, except that this will be TIME instead of X, and OPEN/HIGH/LOW/CLOSE instead of Y.
     */
    @Deprecated
    public static final int OHLC = ChartDescriptor.ChartType.getOHLC();
    /**
     * Similar to a XY chart, except one of the axis will be based on a category instead of being numeric.
     */
    public static final int CATEGORY = ChartDescriptor.ChartType.getCATEGORY();
    /**
     * Similar to XY, this chart will have three numeric dimensions, plotting each with an X, Y, and Z value.
     */
    public static final int XYZ = ChartDescriptor.ChartType.getXYZ();
    /**
     * Similar to a CATEGORY chart, this will have two category axes, and one numeric axis.
     */
    public static final int CATEGORY_3D = ChartDescriptor.ChartType.getCATEGORY_3D();
    public static final int TREEMAP = ChartDescriptor.ChartType.getTREEMAP();
}
