package io.deephaven.javascript.proto.dhinternal.flatbuffers;

import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.flatbuffers.Long", namespace = JsPackage.GLOBAL)
public class Long {
    public static Long ZERO;

    public static native Long create(double low, double high);

    public double high;
    public double low;

    public Long(double low, double high) {}

    @JsMethod(name = "equals")
    public native boolean equals_(Object other);

    public native double toFloat64();
}
