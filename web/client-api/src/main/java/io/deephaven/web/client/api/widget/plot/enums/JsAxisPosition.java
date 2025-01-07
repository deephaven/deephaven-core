//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisPosition", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsAxisPosition {
    /**
     * The axis should be drawn at the top of the chart.
     */
    public static final int TOP = AxisDescriptor.AxisPosition.getTOP();
    /**
     * The axis should be drawn at the bottom of the chart.
     */
    public static final int BOTTOM = AxisDescriptor.AxisPosition.getBOTTOM();
    /**
     * The axis should be drawn at the left side of the chart.
     */
    public static final int LEFT = AxisDescriptor.AxisPosition.getLEFT();
    /**
     * The axis should be drawn at the right side of the chart.
     */
    public static final int RIGHT = AxisDescriptor.AxisPosition.getRIGHT();
    /**
     * No position makes sense for this axis, or the position is apparent from the axis type.
     */
    public static final int NONE = AxisDescriptor.AxisPosition.getNONE();
}
