package io.deephaven.javascript.proto.dhinternal.jspb;

import elemental2.core.ArrayBuffer;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(isNative = true, name = "dhinternal.jspb.BinaryReader", namespace = JsPackage.GLOBAL)
public class BinaryReader {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AllocBytesUnionType {
        @JsOverlay
        static BinaryReader.AllocBytesUnionType of(Object o) {
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
        static BinaryReader.ConstructorBytesUnionType of(Object o) {
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
    public interface ReadAnyUnionType {
        @JsOverlay
        static BinaryReader.ReadAnyUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default boolean asBoolean() {
            return Js.asBoolean(this);
        }

        @JsOverlay
        default double asDouble() {
            return Js.asDouble(this);
        }

        @JsOverlay
        default JsArray<Object> asJsArray() {
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
        default boolean isBoolean() {
            return (Object) this instanceof Boolean;
        }

        @JsOverlay
        default boolean isDouble() {
            return (Object) this instanceof Double;
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

    @JsFunction
    public interface ReadGroupReaderFn {
        void onInvoke(Object p0, BinaryReader p1);
    }

    @JsFunction
    public interface ReadMessageFn {
        @JsFunction
        public interface P1Fn {
            void onInvoke(Object p0, BinaryReader p1);
        }

        void onInvoke(Object p0, BinaryReader.ReadMessageFn.P1Fn p1);
    }

    @JsFunction
    public interface RegisterReadCallbackCallbackFn {
        Object onInvoke(BinaryReader p0);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SetBlockBytesUnionType {
        @JsOverlay
        static BinaryReader.SetBlockBytesUnionType of(Object o) {
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

    @JsMethod(name = "alloc")
    public static native BinaryReader alloc_STATIC();

    @JsMethod(name = "alloc")
    public static native BinaryReader alloc_STATIC(
        BinaryReader.AllocBytesUnionType bytes, double start, double length);

    @JsMethod(name = "alloc")
    public static native BinaryReader alloc_STATIC(
        BinaryReader.AllocBytesUnionType bytes, double start);

    @JsMethod(name = "alloc")
    public static native BinaryReader alloc_STATIC(BinaryReader.AllocBytesUnionType bytes);

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(ArrayBuffer bytes, double start, double length) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start,
            length);
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(ArrayBuffer bytes, double start) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(ArrayBuffer bytes) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(
        JsArray<Double> bytes, double start, double length) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start,
            length);
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(JsArray<Double> bytes, double start) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(JsArray<Double> bytes) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(String bytes, double start, double length) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start,
            length);
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(String bytes, double start) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(String bytes) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(Uint8Array bytes, double start, double length) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start,
            length);
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(Uint8Array bytes, double start) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    @JsMethod(name = "alloc")
    public static final BinaryReader alloc_STATIC(Uint8Array bytes) {
        return alloc_STATIC(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public static final BinaryReader alloc_STATIC(double[] bytes, double start, double length) {
        return alloc_STATIC(Js.<JsArray<Double>>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public static final BinaryReader alloc_STATIC(double[] bytes, double start) {
        return alloc_STATIC(Js.<JsArray<Double>>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public static final BinaryReader alloc_STATIC(double[] bytes) {
        return alloc_STATIC(Js.<JsArray<Double>>uncheckedCast(bytes));
    }

    public BinaryReader.ReadMessageFn readMessage;

    public BinaryReader() {}

    public BinaryReader(ArrayBuffer bytes, double start, double length) {}

    public BinaryReader(ArrayBuffer bytes, double start) {}

    public BinaryReader(ArrayBuffer bytes) {}

    public BinaryReader(BinaryReader.ConstructorBytesUnionType bytes, double start,
        double length) {}

    public BinaryReader(BinaryReader.ConstructorBytesUnionType bytes, double start) {}

    public BinaryReader(BinaryReader.ConstructorBytesUnionType bytes) {}

    public BinaryReader(JsArray<Double> bytes, double start, double length) {}

    public BinaryReader(JsArray<Double> bytes, double start) {}

    public BinaryReader(JsArray<Double> bytes) {}

    public BinaryReader(String bytes, double start, double length) {}

    public BinaryReader(String bytes, double start) {}

    public BinaryReader(String bytes) {}

    public BinaryReader(Uint8Array bytes, double start, double length) {}

    public BinaryReader(Uint8Array bytes, double start) {}

    public BinaryReader(Uint8Array bytes) {}

    public BinaryReader(double[] bytes, double start, double length) {}

    public BinaryReader(double[] bytes, double start) {}

    public BinaryReader(double[] bytes) {}

    public native void advance(double count);

    public native BinaryReader alloc();

    public native BinaryReader alloc(
        BinaryReader.AllocBytesUnionType bytes, double start, double length);

    public native BinaryReader alloc(BinaryReader.AllocBytesUnionType bytes, double start);

    public native BinaryReader alloc(BinaryReader.AllocBytesUnionType bytes);

    @JsOverlay
    public final BinaryReader alloc(ArrayBuffer bytes, double start, double length) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final BinaryReader alloc(ArrayBuffer bytes, double start) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final BinaryReader alloc(ArrayBuffer bytes) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public final BinaryReader alloc(JsArray<Double> bytes, double start, double length) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final BinaryReader alloc(JsArray<Double> bytes, double start) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final BinaryReader alloc(JsArray<Double> bytes) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public final BinaryReader alloc(String bytes, double start, double length) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final BinaryReader alloc(String bytes, double start) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final BinaryReader alloc(String bytes) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public final BinaryReader alloc(Uint8Array bytes, double start, double length) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final BinaryReader alloc(Uint8Array bytes, double start) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final BinaryReader alloc(Uint8Array bytes) {
        return alloc(Js.<BinaryReader.AllocBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public final BinaryReader alloc(double[] bytes, double start, double length) {
        return alloc(Js.<JsArray<Double>>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final BinaryReader alloc(double[] bytes, double start) {
        return alloc(Js.<JsArray<Double>>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final BinaryReader alloc(double[] bytes) {
        return alloc(Js.<JsArray<Double>>uncheckedCast(bytes));
    }

    public native void free();

    public native Uint8Array getBuffer();

    public native double getCursor();

    public native boolean getError();

    public native double getFieldCursor();

    public native BinaryDecoder getFieldDecoder();

    public native double getFieldNumber();

    public native int getWireType();

    public native boolean isEndGroup();

    public native boolean nextField();

    public native BinaryReader.ReadAnyUnionType readAny(int fieldType);

    public native boolean readBool();

    public native Uint8Array readBytes();

    public native double readDouble();

    public native double readEnum();

    public native double readFixed32();

    public native double readFixed64();

    public native String readFixed64String();

    public native String readFixedHash64();

    public native double readFloat();

    public native void readGroup(
        double field, Message message, BinaryReader.ReadGroupReaderFn reader);

    public native double readInt32();

    public native String readInt32String();

    public native double readInt64();

    public native String readInt64String();

    public native JsArray<Boolean> readPackedBool();

    public native JsArray<Double> readPackedDouble();

    public native JsArray<Double> readPackedEnum();

    public native JsArray<Double> readPackedFixed32();

    public native JsArray<Double> readPackedFixed64();

    public native JsArray<String> readPackedFixed64String();

    public native JsArray<String> readPackedFixedHash64();

    public native JsArray<Double> readPackedFloat();

    public native JsArray<Double> readPackedInt32();

    public native JsArray<String> readPackedInt32String();

    public native JsArray<Double> readPackedInt64();

    public native JsArray<String> readPackedInt64String();

    public native JsArray<Double> readPackedSfixed32();

    public native JsArray<Double> readPackedSfixed64();

    public native JsArray<String> readPackedSfixed64String();

    public native JsArray<Double> readPackedSint32();

    public native JsArray<Double> readPackedSint64();

    public native JsArray<String> readPackedSint64String();

    public native JsArray<Double> readPackedUint32();

    public native JsArray<String> readPackedUint32String();

    public native JsArray<Double> readPackedUint64();

    public native JsArray<String> readPackedUint64String();

    public native JsArray<String> readPackedVarintHash64();

    public native double readSfixed32();

    public native String readSfixed32String();

    public native double readSfixed64();

    public native String readSfixed64String();

    public native double readSint32();

    public native double readSint64();

    public native String readSint64String();

    public native String readString();

    public native double readUint32();

    public native String readUint32String();

    public native double readUint64();

    public native String readUint64String();

    public native String readVarintHash64();

    public native void registerReadCallback(
        String callbackName, BinaryReader.RegisterReadCallbackCallbackFn callback);

    public native void reset();

    public native Object runReadCallback(String callbackName);

    public native void setBlock();

    @JsOverlay
    public final void setBlock(ArrayBuffer bytes, double start, double length) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final void setBlock(ArrayBuffer bytes, double start) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final void setBlock(ArrayBuffer bytes) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public final void setBlock(JsArray<Double> bytes, double start, double length) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final void setBlock(JsArray<Double> bytes, double start) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final void setBlock(JsArray<Double> bytes) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes));
    }

    public native void setBlock(
        BinaryReader.SetBlockBytesUnionType bytes, double start, double length);

    public native void setBlock(BinaryReader.SetBlockBytesUnionType bytes, double start);

    public native void setBlock(BinaryReader.SetBlockBytesUnionType bytes);

    @JsOverlay
    public final void setBlock(String bytes, double start, double length) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final void setBlock(String bytes, double start) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final void setBlock(String bytes) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public final void setBlock(Uint8Array bytes, double start, double length) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final void setBlock(Uint8Array bytes, double start) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final void setBlock(Uint8Array bytes) {
        setBlock(Js.<BinaryReader.SetBlockBytesUnionType>uncheckedCast(bytes));
    }

    @JsOverlay
    public final void setBlock(double[] bytes, double start, double length) {
        setBlock(Js.<JsArray<Double>>uncheckedCast(bytes), start, length);
    }

    @JsOverlay
    public final void setBlock(double[] bytes, double start) {
        setBlock(Js.<JsArray<Double>>uncheckedCast(bytes), start);
    }

    @JsOverlay
    public final void setBlock(double[] bytes) {
        setBlock(Js.<JsArray<Double>>uncheckedCast(bytes));
    }

    public native void skipDelimitedField();

    public native void skipField();

    public native void skipFixed32Field();

    public native void skipFixed64Field();

    public native void skipGroup();

    public native void skipMatchingFields();

    public native void skipVarintField();

    public native void unskipHeader();
}
