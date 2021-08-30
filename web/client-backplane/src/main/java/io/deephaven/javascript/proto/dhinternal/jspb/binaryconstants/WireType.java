package io.deephaven.javascript.proto.dhinternal.jspb.binaryconstants;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.jspb.BinaryConstants.WireType",
    namespace = JsPackage.GLOBAL)
public class WireType {
    public static int DELIMITED,
        END_GROUP,
        FIXED32,
        FIXED64,
        INVALID,
        START_GROUP,
        VARINT;
}
