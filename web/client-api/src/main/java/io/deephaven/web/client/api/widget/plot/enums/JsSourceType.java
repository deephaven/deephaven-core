package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.web.shared.data.plot.SourceType;
import jsinterop.annotations.JsType;

@JsType(name = "SourceType")
@SuppressWarnings("unusable-by-js")
public class JsSourceType {
    public static final SourceType X = SourceType.X;
    public static final SourceType Y = SourceType.Y;
    public static final SourceType Z = SourceType.Z;
    public static final SourceType X_LOW = SourceType.X_LOW;
    public static final SourceType X_HIGH = SourceType.X_HIGH;
    public static final SourceType Y_LOW = SourceType.Y_LOW;
    public static final SourceType Y_HIGH = SourceType.Y_HIGH;
    public static final SourceType TIME = SourceType.TIME;
    public static final SourceType OPEN = SourceType.OPEN;
    public static final SourceType HIGH = SourceType.HIGH;
    public static final SourceType LOW = SourceType.LOW;
    public static final SourceType CLOSE = SourceType.CLOSE;
    public static final SourceType SHAPE = SourceType.SHAPE;
    public static final SourceType SIZE = SourceType.SIZE;
    public static final SourceType LABEL = SourceType.LABEL;
    public static final SourceType COLOR = SourceType.COLOR;
}