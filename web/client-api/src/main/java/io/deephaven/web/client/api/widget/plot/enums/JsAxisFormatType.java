//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.figuredescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisFormatType", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsAxisFormatType {
    /**
     * Indicates that this axis will have discrete values rather than be on a continuous numeric axis.
     */
    public static final int CATEGORY = AxisDescriptor.AxisFormatType.getCATEGORY();
    /**
     * Indicates that the values are numeric, and should be plotted on a continuous axis.
     */
    public static final int NUMBER = AxisDescriptor.AxisFormatType.getNUMBER();
}
