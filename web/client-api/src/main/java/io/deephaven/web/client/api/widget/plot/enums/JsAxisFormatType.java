//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisFormatType", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsAxisFormatType {
    /**
     * Indicates that this axis will have discrete values rather than be on a continuous numeric axis.
     */
    public static final int CATEGORY = FigureDescriptor.AxisDescriptor.AxisFormatType.CATEGORY.getNumber();
    /**
     * Indicates that the values are numeric, and should be plotted on a continuous axis.
     */
    public static final int NUMBER = FigureDescriptor.AxisDescriptor.AxisFormatType.NUMBER.getNumber();
}
