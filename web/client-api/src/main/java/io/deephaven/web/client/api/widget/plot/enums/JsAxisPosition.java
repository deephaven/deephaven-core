/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.widget.plot.enums;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisPosition", namespace = "dh.plot")
@TsTypeDef(tsType = "number")
public class JsAxisPosition {
    public static final int TOP = AxisDescriptor.AxisPosition.getTOP();
    public static final int BOTTOM = AxisDescriptor.AxisPosition.getBOTTOM();
    public static final int LEFT = AxisDescriptor.AxisPosition.getLEFT();
    public static final int RIGHT = AxisDescriptor.AxisPosition.getRIGHT();
    public static final int NONE = AxisDescriptor.AxisPosition.getNONE();
}
