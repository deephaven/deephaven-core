//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisPosition", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsAxisPosition {
    /**
     * The axis should be drawn at the top of the chart.
     */
    public static final int TOP = FigureDescriptor.AxisDescriptor.AxisPosition.TOP.getNumber();
    /**
     * The axis should be drawn at the bottom of the chart.
     */
    public static final int BOTTOM = FigureDescriptor.AxisDescriptor.AxisPosition.BOTTOM.getNumber();
    /**
     * The axis should be drawn at the left side of the chart.
     */
    public static final int LEFT = FigureDescriptor.AxisDescriptor.AxisPosition.LEFT.getNumber();
    /**
     * The axis should be drawn at the right side of the chart.
     */
    public static final int RIGHT = FigureDescriptor.AxisDescriptor.AxisPosition.RIGHT.getNumber();
    /**
     * No position makes sense for this axis, or the position is apparent from the axis type.
     */
    public static final int NONE = FigureDescriptor.AxisDescriptor.AxisPosition.NONE.getNumber();
}
