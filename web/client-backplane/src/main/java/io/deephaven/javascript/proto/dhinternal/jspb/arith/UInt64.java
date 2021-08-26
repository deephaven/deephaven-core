package io.deephaven.javascript.proto.dhinternal.jspb.arith;

import elemental2.core.JsArray;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.jspb.arith.UInt64", namespace = JsPackage.GLOBAL)
public class UInt64 {
    public static native UInt64 fromString(String str);

    public static native UInt64 mul32x32(double a, double b);

    public double hi;
    public double lo;

    public UInt64(double lo, double hi) {}

    public native UInt64 add(UInt64 other);

    @JsMethod(name = "clone")
    public native UInt64 clone_();

    public native double cmp(UInt64 other);

    public native JsArray<Object> div(double divisor);

    public native UInt64 leftShift();

    public native boolean lsb();

    public native boolean msb();

    public native UInt64 mul(double a);

    public native UInt64 rightShift();

    public native UInt64 sub(UInt64 other);

    @JsMethod(name = "toString")
    public native String toString_();

    public native boolean zero();
}
