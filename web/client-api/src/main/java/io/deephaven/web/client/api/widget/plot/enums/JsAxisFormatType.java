/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisFormatType", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsAxisFormatType {
    public static final int CATEGORY = AxisDescriptor.AxisFormatType.getCATEGORY();
    public static final int NUMBER = AxisDescriptor.AxisFormatType.getNUMBER();
}
