package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.web.shared.data.plot.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisFormatType")
@SuppressWarnings("unusable-by-js")
public class JsAxisFormatType {
    public static final AxisDescriptor.AxisFormatType CATEGORY = AxisDescriptor.AxisFormatType.CATEGORY;
    public static final AxisDescriptor.AxisFormatType NUMBER = AxisDescriptor.AxisFormatType.NUMBER;
}
