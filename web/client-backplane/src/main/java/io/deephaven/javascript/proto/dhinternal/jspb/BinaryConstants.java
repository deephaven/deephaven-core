package io.deephaven.javascript.proto.dhinternal.jspb;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.jspb.BinaryConstants", namespace = JsPackage.GLOBAL)
public class BinaryConstants {
    @JsFunction
    public interface FieldTypeToWireTypeFn {
        int onInvoke(int p0);
    }

    public static double FLOAT32_EPS;
    public static double FLOAT32_MAX;
    public static double FLOAT32_MIN;
    public static double FLOAT64_EPS;
    public static double FLOAT64_MAX;
    public static double FLOAT64_MIN;
    public static BinaryConstants.FieldTypeToWireTypeFn FieldTypeToWireType;
    public static double INVALID_FIELD_NUMBER;
    public static double TWO_TO_20;
    public static double TWO_TO_23;
    public static double TWO_TO_31;
    public static double TWO_TO_32;
    public static double TWO_TO_52;
    public static double TWO_TO_63;
    public static double TWO_TO_64;
    public static String ZERO_HASH;
}
