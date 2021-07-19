package io.deephaven.web.client.api.widget.plot.enums;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FigureDescriptor;
import jsinterop.annotations.JsType;

@JsType(name = "SourceType")
@SuppressWarnings("unusable-by-js")
public class JsSourceType {
    public static final int X = FigureDescriptor.SourceType.getX();
    public static final int Y = FigureDescriptor.SourceType.getY();
    public static final int Z = FigureDescriptor.SourceType.getZ();
    public static final int X_LOW = FigureDescriptor.SourceType.getX_LOW();
    public static final int X_HIGH = FigureDescriptor.SourceType.getX_HIGH();
    public static final int Y_LOW = FigureDescriptor.SourceType.getY_LOW();
    public static final int Y_HIGH = FigureDescriptor.SourceType.getY_HIGH();
    public static final int TIME = FigureDescriptor.SourceType.getTIME();
    public static final int OPEN = FigureDescriptor.SourceType.getOPEN();
    public static final int HIGH = FigureDescriptor.SourceType.getHIGH();
    public static final int LOW = FigureDescriptor.SourceType.getLOW();
    public static final int CLOSE = FigureDescriptor.SourceType.getCLOSE();
    public static final int SHAPE = FigureDescriptor.SourceType.getSHAPE();
    public static final int SIZE = FigureDescriptor.SourceType.getSIZE();
    public static final int LABEL = FigureDescriptor.SourceType.getLABEL();
    public static final int COLOR = FigureDescriptor.SourceType.getCOLOR();
}