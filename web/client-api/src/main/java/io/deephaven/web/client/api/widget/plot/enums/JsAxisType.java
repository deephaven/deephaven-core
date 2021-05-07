package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.web.shared.data.plot.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisType")
@SuppressWarnings("unusable-by-js")
public class JsAxisType {
    public static final AxisDescriptor.AxisType X = AxisDescriptor.AxisType.X;
    public static final AxisDescriptor.AxisType Y = AxisDescriptor.AxisType.Y;
    public static final AxisDescriptor.AxisType SHAPE = AxisDescriptor.AxisType.SHAPE;
    public static final AxisDescriptor.AxisType SIZE = AxisDescriptor.AxisType.SIZE;
    public static final AxisDescriptor.AxisType LABEL = AxisDescriptor.AxisType.LABEL;
    public static final AxisDescriptor.AxisType COLOR = AxisDescriptor.AxisType.COLOR;
}
