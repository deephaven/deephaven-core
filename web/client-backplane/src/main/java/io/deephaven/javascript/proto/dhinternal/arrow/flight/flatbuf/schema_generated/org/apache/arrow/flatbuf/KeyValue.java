package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Encoding;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.KeyValue",
    namespace = JsPackage.GLOBAL)
public class KeyValue {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface KeyUnionType {
        @JsOverlay
        static KeyValue.KeyUnionType of(Object o) {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ValueUnionType {
        @JsOverlay
        static KeyValue.ValueUnionType of(Object o) {
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

    public static native void addKey(Builder builder, double keyOffset);

    public static native void addValue(Builder builder, double valueOffset);

    public static native double createKeyValue(Builder builder, double keyOffset,
        double valueOffset);

    public static native double endKeyValue(Builder builder);

    public static native KeyValue getRootAsKeyValue(ByteBuffer bb, KeyValue obj);

    public static native KeyValue getRootAsKeyValue(ByteBuffer bb);

    public static native KeyValue getSizePrefixedRootAsKeyValue(ByteBuffer bb, KeyValue obj);

    public static native KeyValue getSizePrefixedRootAsKeyValue(ByteBuffer bb);

    public static native void startKeyValue(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native KeyValue __init(double i, ByteBuffer bb);

    public native KeyValue.KeyUnionType key();

    public native KeyValue.KeyUnionType key(Encoding optionalEncoding);

    public native KeyValue.ValueUnionType value();

    public native KeyValue.ValueUnionType value(Encoding optionalEncoding);
}
