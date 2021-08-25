package io.deephaven.javascript.proto.dhinternal;

import elemental2.core.Float32Array;
import elemental2.core.Float64Array;
import elemental2.core.Int32Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.flatbuffers", namespace = JsPackage.GLOBAL)
public class Flatbuffers {
    public static double FILE_IDENTIFIER_LENGTH;
    public static double SIZEOF_INT;
    public static double SIZEOF_SHORT;
    public static double SIZE_PREFIX_LENGTH;
    public static Float32Array float32;
    public static Float64Array float64;
    public static Int32Array int32;
    public static boolean isLittleEndian;
}
