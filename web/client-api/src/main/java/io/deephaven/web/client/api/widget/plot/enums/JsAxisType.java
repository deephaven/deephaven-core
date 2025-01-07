//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisType", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsAxisType {
    /**
     * Indicates that this is an X-axis, typically drawn on the bottom or top of the chart, depending on position
     * attribute
     */
    public static final int X = AxisDescriptor.AxisType.getX();
    /**
     * Indicates that this is a Y-Axis, typically drawn on the left or right of the chart, depending on position
     * attribute
     */
    public static final int Y = AxisDescriptor.AxisType.getY();
    /**
     * Indicates that this axis is used to represent that items when drawn as a point may have a specialized shape
     */
    public static final int SHAPE = AxisDescriptor.AxisType.getSHAPE();
    /**
     * Indicates that this axis is used to represent that items when drawn as a point may have a specific size
     */
    public static final int SIZE = AxisDescriptor.AxisType.getSIZE();
    /**
     * Indicates that this axis is used to represent that items when drawn as a point may have label specified from the
     * underlying data
     */
    public static final int LABEL = AxisDescriptor.AxisType.getLABEL();
    /**
     * Indicates that this axis is used to represent that items when drawn as a point may have a specific color
     */
    public static final int COLOR = AxisDescriptor.AxisType.getCOLOR();
}
