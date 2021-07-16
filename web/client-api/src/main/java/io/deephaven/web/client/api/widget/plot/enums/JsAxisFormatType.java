package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisFormatType")
@SuppressWarnings("unusable-by-js")
public class JsAxisFormatType {
    public static final int CATEGORY = AxisDescriptor.AxisFormatType.getCATEGORY();
    public static final int NUMBER = AxisDescriptor.AxisFormatType.getNUMBER();
}
