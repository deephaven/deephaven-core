package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.web.shared.data.plot.AxisDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "AxisPosition")
@SuppressWarnings("unusable-by-js")
public class JsAxisPosition {
    public static final AxisDescriptor.AxisPosition TOP = AxisDescriptor.AxisPosition.TOP;
    public static final AxisDescriptor.AxisPosition BOTTOM = AxisDescriptor.AxisPosition.BOTTOM;
    public static final AxisDescriptor.AxisPosition LEFT = AxisDescriptor.AxisPosition.LEFT;
    public static final AxisDescriptor.AxisPosition RIGHT = AxisDescriptor.AxisPosition.RIGHT;
    public static final AxisDescriptor.AxisPosition NONE = AxisDescriptor.AxisPosition.NONE;
}
