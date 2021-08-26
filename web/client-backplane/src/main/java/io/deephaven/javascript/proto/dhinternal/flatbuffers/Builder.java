package io.deephaven.javascript.proto.dhinternal.flatbuffers;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(isNative = true, name = "dhinternal.flatbuffers.Builder", namespace = JsPackage.GLOBAL)
public class Builder {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateStringSUnionType {
        @JsOverlay
        static Builder.CreateStringSUnionType of(Object o) {
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

    public static native ByteBuffer growByteBuffer(ByteBuffer bb);

    public Builder() {}

    public Builder(double initial_size) {}

    public native void addFieldFloat32(double voffset, double value, double defaultValue);

    public native void addFieldFloat64(double voffset, double value, double defaultValue);

    public native void addFieldInt16(double voffset, double value, double defaultValue);

    public native void addFieldInt32(double voffset, double value, double defaultValue);

    public native void addFieldInt64(double voffset, Long value, Long defaultValue);

    public native void addFieldInt8(double voffset, double value, double defaultValue);

    public native void addFieldOffset(double voffset, double value, double defaultValue);

    public native void addFieldStruct(double voffset, double value, double defaultValue);

    public native void addFloat32(double value);

    public native void addFloat64(double value);

    public native void addInt16(double value);

    public native void addInt32(double value);

    public native void addInt64(Long value);

    public native void addInt8(double value);

    public native void addOffset(double offset);

    public native Uint8Array asUint8Array();

    public native void clear();

    public native Long createLong(double low, double high);

    public native double createString(Builder.CreateStringSUnionType s);

    @JsOverlay
    public final double createString(String s) {
        return createString(Js.<Builder.CreateStringSUnionType>uncheckedCast(s));
    }

    @JsOverlay
    public final double createString(Uint8Array s) {
        return createString(Js.<Builder.CreateStringSUnionType>uncheckedCast(s));
    }

    public native ByteBuffer dataBuffer();

    public native double endObject();

    public native double endVector();

    public native void finish(double root_table, String file_identifier, boolean size_prefix);

    public native void finish(double root_table, String file_identifier);

    public native void finish(double root_table);

    public native void finishSizePrefixed(double root_table, String file_identifier);

    public native void finishSizePrefixed(double root_table);

    public native void forceDefaults(boolean forceDefaults);

    public native void nested(double obj);

    public native void notNested();

    public native double offset();

    public native void pad(double byte_size);

    public native void prep(double size, double additional_bytes);

    public native void requiredField(double table, double field);

    public native void slot(double voffset);

    public native void startObject(double numfields);

    public native void startVector(double elem_size, double num_elems, double alignment);

    public native void writeFloat32(double value);

    public native void writeFloat64(double value);

    public native void writeInt16(double value);

    public native void writeInt32(double value);

    public native void writeInt64(Long value);

    public native void writeInt8(double value);
}
