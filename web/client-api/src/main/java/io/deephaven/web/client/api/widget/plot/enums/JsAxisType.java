package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisType")
@SuppressWarnings("unusable-by-js")
public class JsAxisType {
    public static final int X = AxisDescriptor.AxisType.getX();
    public static final int Y = AxisDescriptor.AxisType.getY();
    public static final int SHAPE = AxisDescriptor.AxisType.getSHAPE();
    public static final int SIZE = AxisDescriptor.AxisType.getSIZE();
    public static final int LABEL = AxisDescriptor.AxisType.getLABEL();
    public static final int COLOR = AxisDescriptor.AxisType.getCOLOR();
}
