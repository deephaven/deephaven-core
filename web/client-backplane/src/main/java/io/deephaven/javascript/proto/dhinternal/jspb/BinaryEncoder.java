package io.deephaven.javascript.proto.dhinternal.jspb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.jspb.BinaryEncoder", namespace = JsPackage.GLOBAL)
public class BinaryEncoder {
    public native JsArray<Double> end();

    public native double length();

    public native void writeBool(boolean value);

    public native void writeBytes(Uint8Array bytes);

    public native void writeDouble(double value);

    public native void writeEnum(double value);

    public native void writeFixedHash64(String hash);

    public native void writeFloat(double value);

    public native void writeInt16(double value);

    public native void writeInt32(double value);

    public native void writeInt64(double value);

    public native void writeInt64String(String value);

    public native void writeInt8(double value);

    public native void writeSignedVarint32(double value);

    public native void writeSignedVarint64(double value);

    public native void writeSplitFixed64(double lowBits, double highBits);

    public native void writeSplitVarint64(double lowBits, double highBits);

    public native double writeString(String value);

    public native void writeUint16(double value);

    public native void writeUint32(double value);

    public native void writeUint64(double value);

    public native void writeUint8(double value);

    public native void writeUnsignedVarint32(double value);

    public native void writeUnsignedVarint64(double value);

    public native void writeVarintHash64(String hash);

    public native void writeZigzagVarint32(double value);

    public native void writeZigzagVarint64(double value);

    public native void writeZigzagVarint64String(String value);
}
