//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisType", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsAxisType {
    /**
     * Indicates that this is an X-axis, typically drawn on the bottom or top of the chart, depending on position
     * attribute
     */
    public static final int X = AxisDescriptor.AxisType.X.getNumber();
    /**
     * Indicates that this is a Y-Axis, typically drawn on the left or right of the chart, depending on position
     * attribute
     */
    public static final int Y = AxisDescriptor.AxisType.Y.getNumber();
    /**
     * Indicates that this axis is used to represent that items when drawn as a point may have a specialized shape
     */
    public static final int SHAPE = AxisDescriptor.AxisType.SHAPE.getNumber();
    /**
     * Indicates that this axis is used to represent that items when drawn as a point may have a specific size
     */
    public static final int SIZE = AxisDescriptor.AxisType.SIZE.getNumber();
    /**
     * Indicates that this axis is used to represent that items when drawn as a point may have label specified from the
     * underlying data
     */
    public static final int LABEL = AxisDescriptor.AxisType.LABEL.getNumber();
    /**
     * Indicates that this axis is used to represent that items when drawn as a point may have a specific color
     */
    public static final int COLOR = AxisDescriptor.AxisType.COLOR.getNumber();
}
