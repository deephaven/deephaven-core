package io.deephaven.javascript.proto.dhinternal.jspb;

import elemental2.core.ArrayBuffer;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(isNative = true, name = "dhinternal.jspb.BinaryDecoder", namespace = JsPackage.GLOBAL)
public class BinaryDecoder {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AllocBytesUnionType {
        @JsOverlay
        static BinaryDecoder.AllocBytesUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default ArrayBuffer asArrayBuffer() {
            return Js.cast(this);
        }

        @JsOverlay
        default JsArray<Double> asJsArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isArrayBuffer() {
            return (Object) this instanceof ArrayBuffer;
        }

        @JsOverlay
        default boolean isJsArray() {
            return (Object) this instanceof JsArray;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConstructorBytesUnionType {
        @JsOverlay
        static BinaryDecoder.ConstructorBytesUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default ArrayBuffer asArrayBuffer() {
            return Js.cast(this);
        }

        @JsOverlay
        default JsArray<Double> asJsArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isArrayBuffer() {
            return (Object) this instanceof ArrayBuffer;
        }

        @JsOverlay
        default boolean isJsArray() {
            return (Object) this instanceof JsArray;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetBlockDataUnionType {
        @JsOverlay
        static BinaryDecoder.SetBlockDataUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default ArrayBuffer asArrayBuffer() {
            return Js.cast(this);
        }

        @JsOverlay
        default JsArray<Double> asJsArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isArrayBuffer() {
            return (Object) this instanceof ArrayBuffer;
        }

        @JsOverlay
        default boolean isJsArray() {
            return (Object) this instanceof JsArray;
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    public static native BinaryDecoder alloc();

    public static native BinaryDecoder alloc(
        BinaryDecoder.AllocBytesUnionType bytes, double start, double length);

    public static native BinaryDecoder alloc(BinaryDecoder.AllocBytesUnionType bytes, double start);

    public static native BinaryDecoder alloc(BinaryDecoder.AllocBytesUnionType bytes);

    @JsOverlay
    public static final BinaryDecoder alloc(ArrayBuffer bytes, double start, double length) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(ArrayBuffer bytes, double start) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(ArrayBuffer bytes) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public static final BinaryDecoder alloc(JsArray<Double> bytes, double start, double length) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(JsArray<Double> bytes, double start) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(JsArray<Double> bytes) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public static final BinaryDecoder alloc(String bytes, double start, double length) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(String bytes, double start) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(String bytes) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public static final BinaryDecoder alloc(Uint8Array bytes, double start, double length) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(Uint8Array bytes, double start) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(Uint8Array bytes) {
        return alloc(Js.<BinaryDecoder.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public static final BinaryDecoder alloc(double[] bytes, double start, double length) {
        return alloc(Js.<JsArray<Double>>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(double[] bytes, double start) {
        return alloc(Js.<JsArray<Double>>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public static final BinaryDecoder alloc(double[] bytes) {
        return alloc(Js.<JsArray<Double>>uncheckedCast(bytes));
    }

    public BinaryDecoder() {}

    public BinaryDecoder(ArrayBuffer bytes, double start, double length) {}

    public BinaryDecoder(ArrayBuffer bytes, double start) {}

    public BinaryDecoder(ArrayBuffer bytes) {}

    public BinaryDecoder(
        BinaryDecoder.ConstructorBytesUnionType bytes, double start, double length) {}

    public BinaryDecoder(BinaryDecoder.ConstructorBytesUnionType bytes, double start) {}

    public BinaryDecoder(BinaryDecoder.ConstructorBytesUnionType bytes) {}

    public BinaryDecoder(JsArray<Double> bytes, double start, double length) {}

    public BinaryDecoder(JsArray<Double> bytes, double start) {}

    public BinaryDecoder(JsArray<Double> bytes) {}

    public BinaryDecoder(String bytes, double start, double length) {}

    public BinaryDecoder(String bytes, double start) {}

    public BinaryDecoder(String bytes) {}

    public BinaryDecoder(Uint8Array bytes, double start, double length) {}

    public BinaryDecoder(Uint8Array bytes, double start) {}

    public BinaryDecoder(Uint8Array bytes) {}

    public BinaryDecoder(double[] bytes, double start, double length) {}

    public BinaryDecoder(double[] bytes, double start) {}

    public BinaryDecoder(double[] bytes) {}

    public native void advance(double count);

    public native boolean atEnd();

    public native void clear();

    @JsMethod(name = "clone")
    public native BinaryDecoder clone_();

    public native void free();

    public native Uint8Array getBuffer();

    public native double getCursor();

    public native double getEnd();

    public native boolean getError();

    public native boolean pastEnd();

    public native boolean readBool();

    public native Uint8Array readBytes(double length);

    public native double readDouble();

    public native double readEnum();

    public native String readFixedHash64();

    public native double readFloat();

    public native double readInt16();

    public native double readInt32();

    public native double readInt64();

    public native String readInt64String();

    public native double readInt8();

    public native double readSignedVarint32();

    public native double readSignedVarint32String();

    public native double readSignedVarint64();

    public native double readSignedVarint64String();

    public native String readString(double length);

    public native String readStringWithLength();

    public native double readUint16();

    public native double readUint32();

    public native double readUint64();

    public native String readUint64String();

    public native double readUint8();

    public native double readUnsignedVarint32();

    public native double readUnsignedVarint32String();

    public native double readUnsignedVarint64();

    public native double readUnsignedVarint64String();

    public native String readVarintHash64();

    public native double readZigzagVarint32();

    public native double readZigzagVarint64();

    public native double readZigzagVarint64String();

    public native void reset();

    @JsOverlay
    public final void setBlock(ArrayBuffer data, double start, double length) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data), start, length);
    }

    @JsOverlay
    public final void setBlock(ArrayBuffer data, double start) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data), start);
    }

    @JsOverlay
    public final void setBlock(ArrayBuffer data) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    public final void setBlock(JsArray<Double> data, double start, double length) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data), start, length);
    }

    @JsOverlay
    public final void setBlock(JsArray<Double> data, double start) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data), start);
    }

    @JsOverlay
    public final void setBlock(JsArray<Double> data) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data));
    }

    public native void setBlock(
        BinaryDecoder.SetBlockDataUnionType data, double start, double length);

    public native void setBlock(BinaryDecoder.SetBlockDataUnionType data, double start);

    public native void setBlock(BinaryDecoder.SetBlockDataUnionType data);

    @JsOverlay
    public final void setBlock(String data, double start, double length) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data), start, length);
    }

    @JsOverlay
    public final void setBlock(String data, double start) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data), start);
    }

    @JsOverlay
    public final void setBlock(String data) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    public final void setBlock(Uint8Array data, double start, double length) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data), start, length);
    }

    @JsOverlay
    public final void setBlock(Uint8Array data, double start) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data), start);
    }

    @JsOverlay
    public final void setBlock(Uint8Array data) {
        setBlock(Js.<BinaryDecoder.SetBlockDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    public final void setBlock(double[] data, double start, double length) {
        setBlock(Js.<JsArray<Double>>uncheckedCast(data), start, length);
    }

    @JsOverlay
    public final void setBlock(double[] data, double start) {
        setBlock(Js.<JsArray<Double>>uncheckedCast(data), start);
    }

    @JsOverlay
    public final void setBlock(double[] data) {
        setBlock(Js.<JsArray<Double>>uncheckedCast(data));
    }

    public native void setCursor(double cursor);

    public native void setEnd(double end);

    public native void skipVarint();

    public native void unskipVarint(double value);
}
