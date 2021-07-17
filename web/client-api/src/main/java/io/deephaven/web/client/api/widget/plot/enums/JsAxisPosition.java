package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisPosition")
@SuppressWarnings("unusable-by-js")
public class JsAxisPosition {
    public static final int TOP = AxisDescriptor.AxisPosition.getTOP();
    public static final int BOTTOM = AxisDescriptor.AxisPosition.getBOTTOM();
    public static final int LEFT = AxisDescriptor.AxisPosition.getLEFT();
    public static final int RIGHT = AxisDescriptor.AxisPosition.getRIGHT();
    public static final int NONE = AxisDescriptor.AxisPosition.getNONE();
}
