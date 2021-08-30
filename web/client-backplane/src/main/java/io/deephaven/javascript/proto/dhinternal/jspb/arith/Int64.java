package io.deephaven.javascript.proto.dhinternal.jspb.arith;

import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.jspb.arith.Int64", namespace = JsPackage.GLOBAL)
public class Int64 {
    public static native Int64 fromString(String str);

    public double hi;
    public double lo;

    public Int64(double lo, double hi) {}

    public native Int64 add(Int64 other);

    @JsMethod(name = "clone")
    public native Int64 clone_();

    public native Int64 sub(Int64 other);

    @JsMethod(name = "toString")
    public native String toString_();
}
