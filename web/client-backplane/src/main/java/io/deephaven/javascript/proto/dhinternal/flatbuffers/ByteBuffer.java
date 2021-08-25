package io.deephaven.javascript.proto.dhinternal.flatbuffers;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(isNative = true, name = "dhinternal.flatbuffers.ByteBuffer", namespace = JsPackage.GLOBAL)
public class ByteBuffer {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface __stringUnionType {
        @JsOverlay
        static ByteBuffer.__stringUnionType of(Object o) {
            return Js.cast(o);
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
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    public static native ByteBuffer allocate(double byte_size);

    public ByteBuffer(Uint8Array bytes) {}

    public native boolean __has_identifier(String ident);

    public native double __indirect(double offset);

    public native double __offset(double bb_pos, double vtable_offset);

    public native ByteBuffer.__stringUnionType __string(double offset, int optionalEncoding);

    public native ByteBuffer.__stringUnionType __string(double offset);

    public native <T> T __union(T t, double offset);

    public native double __vector(double offset);

    public native double __vector_len(double offset);

    public native Uint8Array bytes();

    public native double capacity();

    public native void clear();

    public native Long createLong(double low, double high);

    public native String getBufferIdentifier();

    public native double position();

    public native double readFloat32(double offset);

    public native double readFloat64(double offset);

    public native double readInt16(double offset);

    public native double readInt32(double offset);

    public native Long readInt64(double offset);

    public native double readInt8(double offset);

    public native double readUint16(double offset);

    public native double readUint32(double offset);

    public native Long readUint64(double offset);

    public native double readUint8(double offset);

    public native void setPosition(double position);

    public native void writeFloat32(double offset, double value);

    public native void writeFloat64(double offset, double value);

    public native void writeInt16(double offset, double value);

    public native void writeInt32(double offset, double value);

    public native void writeInt64(double offset, Long value);

    public native void writeInt8(double offset, double value);

    public native void writeUint16(double offset, double value);

    public native void writeUint32(double offset, double value);

    public native void writeUint64(double offset, Long value);

    public native void writeUint8(double offset, double value);
}
