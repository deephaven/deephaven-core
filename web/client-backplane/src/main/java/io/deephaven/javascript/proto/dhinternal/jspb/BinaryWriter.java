package io.deephaven.javascript.proto.dhinternal.jspb;

import elemental2.core.ArrayBuffer;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(isNative = true, name = "dhinternal.jspb.BinaryWriter", namespace = JsPackage.GLOBAL)
public class BinaryWriter {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface WriteAnyValueUnionType {
        @JsOverlay
        static BinaryWriter.WriteAnyValueUnionType of(Object o) {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface WriteBytesValueUnionType {
        @JsOverlay
        static BinaryWriter.WriteBytesValueUnionType of(Object o) {
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

    @JsFunction
    public interface WriteGroupWriteCallbackFn {
        void onInvoke(Object p0, BinaryWriter p1);
    }

    @JsFunction
    public interface WriteMessageFn {
        @JsFunction
        public interface P2Fn {
            void onInvoke(Object p0, BinaryWriter p1);
        }

        void onInvoke(double p0, Object p1, BinaryWriter.WriteMessageFn.P2Fn p2);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface WriteRepeatedBytesValueArrayUnionType {
        @JsOverlay
        static BinaryWriter.WriteRepeatedBytesValueArrayUnionType of(Object o) {
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

    @JsFunction
    public interface WriteRepeatedGroupWriterCallbackFn {
        void onInvoke(Object p0, BinaryWriter p1);
    }

    @JsFunction
    public interface WriteRepeatedMessageWriterCallbackFn {
        void onInvoke(Object p0, BinaryWriter p1);
    }

    public BinaryWriter.WriteMessageFn writeMessage;

    public native void beginSubMessage(double field);

    public native void endSubMessage(double field);

    public native String getResultBase64String();

    public native Uint8Array getResultBuffer();

    public native void maybeWriteSerializedMessage();

    public native void maybeWriteSerializedMessage(Uint8Array bytes, double start, double end);

    public native void maybeWriteSerializedMessage(Uint8Array bytes, double start);

    public native void maybeWriteSerializedMessage(Uint8Array bytes);

    public native void reset();

    @JsOverlay
    public final void writeAny(int fieldType, double field, JsArray<Object> value) {
        writeAny(fieldType, field, Js.<BinaryWriter.WriteAnyValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void writeAny(int fieldType, double field, Object[] value) {
        writeAny(fieldType, field, Js.<JsArray<Object>>uncheckedCast(value));
    }

    @JsOverlay
    public final void writeAny(int fieldType, double field, String value) {
        writeAny(fieldType, field, Js.<BinaryWriter.WriteAnyValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void writeAny(int fieldType, double field, Uint8Array value) {
        writeAny(fieldType, field, Js.<BinaryWriter.WriteAnyValueUnionType>uncheckedCast(value));
    }

    public native void writeAny(
        int fieldType, double field, BinaryWriter.WriteAnyValueUnionType value);

    @JsOverlay
    public final void writeAny(int fieldType, double field, boolean value) {
        writeAny(fieldType, field, Js.<BinaryWriter.WriteAnyValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void writeAny(int fieldType, double field, double value) {
        writeAny(fieldType, field, Js.<BinaryWriter.WriteAnyValueUnionType>uncheckedCast(value));
    }

    public native void writeBool(double field, boolean value);

    public native void writeBool(double field);

    @JsOverlay
    public final void writeBytes(double field, ArrayBuffer value) {
        writeBytes(field, Js.<BinaryWriter.WriteBytesValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void writeBytes(double field, JsArray<Double> value) {
        writeBytes(field, Js.<BinaryWriter.WriteBytesValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void writeBytes(double field, String value) {
        writeBytes(field, Js.<BinaryWriter.WriteBytesValueUnionType>uncheckedCast(value));
    }

    @JsOverlay
    public final void writeBytes(double field, Uint8Array value) {
        writeBytes(field, Js.<BinaryWriter.WriteBytesValueUnionType>uncheckedCast(value));
    }

    public native void writeBytes(double field, BinaryWriter.WriteBytesValueUnionType value);

    @JsOverlay
    public final void writeBytes(double field, double[] value) {
        writeBytes(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeBytes(double field);

    public native void writeDouble(double field, double value);

    public native void writeDouble(double field);

    public native void writeEnum(double field, double value);

    public native void writeEnum(double field);

    public native void writeFixed32(double field, double value);

    public native void writeFixed32(double field);

    public native void writeFixed64(double field, double value);

    public native void writeFixed64(double field);

    public native void writeFixed64String(double field, String value);

    public native void writeFixed64String(double field);

    public native void writeFixedHash64(double field, String value);

    public native void writeFixedHash64(double field);

    public native void writeFloat(double field, double value);

    public native void writeFloat(double field);

    public native void writeGroup(
        double field, Object value, BinaryWriter.WriteGroupWriteCallbackFn writeCallback);

    public native void writeInt32(double field, double value);

    public native void writeInt32(double field);

    public native void writeInt32String(double field, String value);

    public native void writeInt32String(double field);

    public native void writeInt64(double field, double value);

    public native void writeInt64(double field);

    public native void writeInt64String(double field, String value);

    public native void writeInt64String(double field);

    public native void writePackedBool(double field, JsArray<Boolean> value);

    @JsOverlay
    public final void writePackedBool(double field, boolean[] value) {
        writePackedBool(field, Js.<JsArray<Boolean>>uncheckedCast(value));
    }

    public native void writePackedBool(double field);

    public native void writePackedDouble(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedDouble(double field, double[] value) {
        writePackedDouble(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedDouble(double field);

    public native void writePackedEnum(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedEnum(double field, double[] value) {
        writePackedEnum(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedEnum(double field);

    public native void writePackedFixed32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedFixed32(double field, double[] value) {
        writePackedFixed32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedFixed32(double field);

    public native void writePackedFixed64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedFixed64(double field, double[] value) {
        writePackedFixed64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedFixed64(double field);

    public native void writePackedFixed64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedFixed64String(double field, String[] value) {
        writePackedFixed64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedFixed64String(double field);

    public native void writePackedFixedHash64(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedFixedHash64(double field, String[] value) {
        writePackedFixedHash64(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedFixedHash64(double field);

    public native void writePackedFloat(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedFloat(double field, double[] value) {
        writePackedFloat(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedFloat(double field);

    public native void writePackedInt32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedInt32(double field, double[] value) {
        writePackedInt32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedInt32(double field);

    public native void writePackedInt32String(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedInt32String(double field, String[] value) {
        writePackedInt32String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedInt32String(double field);

    public native void writePackedInt64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedInt64(double field, double[] value) {
        writePackedInt64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedInt64(double field);

    public native void writePackedInt64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedInt64String(double field, String[] value) {
        writePackedInt64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedInt64String(double field);

    public native void writePackedSfixed32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedSfixed32(double field, double[] value) {
        writePackedSfixed32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedSfixed32(double field);

    public native void writePackedSfixed64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedSfixed64(double field, double[] value) {
        writePackedSfixed64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedSfixed64(double field);

    public native void writePackedSfixed64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedSfixed64String(double field, String[] value) {
        writePackedSfixed64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedSfixed64String(double field);

    public native void writePackedSint32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedSint32(double field, double[] value) {
        writePackedSint32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedSint32(double field);

    public native void writePackedSint64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedSint64(double field, double[] value) {
        writePackedSint64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedSint64(double field);

    public native void writePackedSint64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedSint64String(double field, String[] value) {
        writePackedSint64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedSint64String(double field);

    public native void writePackedUint32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedUint32(double field, double[] value) {
        writePackedUint32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedUint32(double field);

    public native void writePackedUint32String(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedUint32String(double field, String[] value) {
        writePackedUint32String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedUint32String(double field);

    public native void writePackedUint64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writePackedUint64(double field, double[] value) {
        writePackedUint64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writePackedUint64(double field);

    public native void writePackedUint64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedUint64String(double field, String[] value) {
        writePackedUint64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedUint64String(double field);

    public native void writePackedVarintHash64(double field, JsArray<String> value);

    @JsOverlay
    public final void writePackedVarintHash64(double field, String[] value) {
        writePackedVarintHash64(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writePackedVarintHash64(double field);

    public native void writeRepeatedBool(double field, JsArray<Boolean> value);

    @JsOverlay
    public final void writeRepeatedBool(double field, boolean[] value) {
        writeRepeatedBool(field, Js.<JsArray<Boolean>>uncheckedCast(value));
    }

    public native void writeRepeatedBool(double field);

    public native void writeRepeatedBytes(
        double field, JsArray<BinaryWriter.WriteRepeatedBytesValueArrayUnionType> value);

    @JsOverlay
    public final void writeRepeatedBytes(
        double field, BinaryWriter.WriteRepeatedBytesValueArrayUnionType[] value) {
        writeRepeatedBytes(
            field,
            Js.<JsArray<BinaryWriter.WriteRepeatedBytesValueArrayUnionType>>uncheckedCast(value));
    }

    public native void writeRepeatedBytes(double field);

    public native void writeRepeatedDouble(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedDouble(double field, double[] value) {
        writeRepeatedDouble(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedDouble(double field);

    public native void writeRepeatedEnum(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedEnum(double field, double[] value) {
        writeRepeatedEnum(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedEnum(double field);

    public native void writeRepeatedFixed32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedFixed32(double field, double[] value) {
        writeRepeatedFixed32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedFixed32(double field);

    public native void writeRepeatedFixed64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedFixed64(double field, double[] value) {
        writeRepeatedFixed64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedFixed64(double field);

    public native void writeRepeatedFixed64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedFixed64String(double field, String[] value) {
        writeRepeatedFixed64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedFixed64String(double field);

    public native void writeRepeatedFixedHash64(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedFixedHash64(double field, String[] value) {
        writeRepeatedFixedHash64(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedFixedHash64(double field);

    public native void writeRepeatedFloat(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedFloat(double field, double[] value) {
        writeRepeatedFloat(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedFloat(double field);

    public native void writeRepeatedGroup(
        double field,
        JsArray<Message> value,
        BinaryWriter.WriteRepeatedGroupWriterCallbackFn writerCallback);

    @JsOverlay
    public final void writeRepeatedGroup(
        double field,
        Message[] value,
        BinaryWriter.WriteRepeatedGroupWriterCallbackFn writerCallback) {
        writeRepeatedGroup(field, Js.<JsArray<Message>>uncheckedCast(value), writerCallback);
    }

    public native void writeRepeatedInt32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedInt32(double field, double[] value) {
        writeRepeatedInt32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedInt32(double field);

    public native void writeRepeatedInt32String(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedInt32String(double field, String[] value) {
        writeRepeatedInt32String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedInt32String(double field);

    public native void writeRepeatedInt64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedInt64(double field, double[] value) {
        writeRepeatedInt64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedInt64(double field);

    public native void writeRepeatedInt64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedInt64String(double field, String[] value) {
        writeRepeatedInt64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedInt64String(double field);

    public native void writeRepeatedMessage(
        double field,
        JsArray<Message> value,
        BinaryWriter.WriteRepeatedMessageWriterCallbackFn writerCallback);

    @JsOverlay
    public final void writeRepeatedMessage(
        double field,
        Message[] value,
        BinaryWriter.WriteRepeatedMessageWriterCallbackFn writerCallback) {
        writeRepeatedMessage(field, Js.<JsArray<Message>>uncheckedCast(value), writerCallback);
    }

    public native void writeRepeatedSfixed32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedSfixed32(double field, double[] value) {
        writeRepeatedSfixed32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedSfixed32(double field);

    public native void writeRepeatedSfixed64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedSfixed64(double field, double[] value) {
        writeRepeatedSfixed64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedSfixed64(double field);

    public native void writeRepeatedSfixed64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedSfixed64String(double field, String[] value) {
        writeRepeatedSfixed64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedSfixed64String(double field);

    public native void writeRepeatedSint32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedSint32(double field, double[] value) {
        writeRepeatedSint32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedSint32(double field);

    public native void writeRepeatedSint64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedSint64(double field, double[] value) {
        writeRepeatedSint64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedSint64(double field);

    public native void writeRepeatedSint64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedSint64String(double field, String[] value) {
        writeRepeatedSint64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedSint64String(double field);

    public native void writeRepeatedString(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedString(double field, String[] value) {
        writeRepeatedString(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedString(double field);

    public native void writeRepeatedUint32(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedUint32(double field, double[] value) {
        writeRepeatedUint32(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedUint32(double field);

    public native void writeRepeatedUint32String(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedUint32String(double field, String[] value) {
        writeRepeatedUint32String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedUint32String(double field);

    public native void writeRepeatedUint64(double field, JsArray<Double> value);

    @JsOverlay
    public final void writeRepeatedUint64(double field, double[] value) {
        writeRepeatedUint64(field, Js.<JsArray<Double>>uncheckedCast(value));
    }

    public native void writeRepeatedUint64(double field);

    public native void writeRepeatedUint64String(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedUint64String(double field, String[] value) {
        writeRepeatedUint64String(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedUint64String(double field);

    public native void writeRepeatedVarintHash64(double field, JsArray<String> value);

    @JsOverlay
    public final void writeRepeatedVarintHash64(double field, String[] value) {
        writeRepeatedVarintHash64(field, Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void writeRepeatedVarintHash64(double field);

    public native void writeSerializedMessage(Uint8Array bytes, double start, double end);

    public native void writeSfixed32(double field, double value);

    public native void writeSfixed32(double field);

    public native void writeSfixed64(double field, double value);

    public native void writeSfixed64(double field);

    public native void writeSfixed64String(double field, String value);

    public native void writeSfixed64String(double field);

    public native void writeSint32(double field, double value);

    public native void writeSint32(double field);

    public native void writeSint64(double field, double value);

    public native void writeSint64(double field);

    public native void writeSint64String(double field, String value);

    public native void writeSint64String(double field);

    public native void writeString(double field, String value);

    public native void writeString(double field);

    public native void writeUint32(double field, double value);

    public native void writeUint32(double field);

    public native void writeUint32String(double field, String value);

    public native void writeUint32String(double field);

    public native void writeUint64(double field, double value);

    public native void writeUint64(double field);

    public native void writeUint64String(double field, String value);

    public native void writeUint64String(double field);

    public native void writeVarintHash64(double field, String value);

    public native void writeVarintHash64(double field);
}
