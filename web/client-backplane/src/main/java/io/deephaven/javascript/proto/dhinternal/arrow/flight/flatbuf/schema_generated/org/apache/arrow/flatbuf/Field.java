package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import elemental2.core.JsArray;
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
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Field",
    namespace = JsPackage.GLOBAL)
public class Field {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface NameUnionType {
        @JsOverlay
        static Field.NameUnionType of(Object o) {
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

    public static native void addChildren(Builder builder, double childrenOffset);

    public static native void addCustomMetadata(Builder builder, double customMetadataOffset);

    public static native void addDictionary(Builder builder, double dictionaryOffset);

    public static native void addName(Builder builder, double nameOffset);

    public static native void addNullable(Builder builder, boolean nullable);

    public static native void addType(Builder builder, double typeOffset);

    public static native void addTypeType(Builder builder, int typeType);

    public static native double createChildrenVector(Builder builder, JsArray<Double> data);

    @JsOverlay
    public static final double createChildrenVector(Builder builder, double[] data) {
        return createChildrenVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double createCustomMetadataVector(Builder builder, JsArray<Double> data);

    @JsOverlay
    public static final double createCustomMetadataVector(Builder builder, double[] data) {
        return createCustomMetadataVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double endField(Builder builder);

    public static native Field getRootAsField(ByteBuffer bb, Field obj);

    public static native Field getRootAsField(ByteBuffer bb);

    public static native Field getSizePrefixedRootAsField(ByteBuffer bb, Field obj);

    public static native Field getSizePrefixedRootAsField(ByteBuffer bb);

    public static native void startChildrenVector(Builder builder, double numElems);

    public static native void startCustomMetadataVector(Builder builder, double numElems);

    public static native void startField(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Field __init(double i, ByteBuffer bb);

    public native Field children(double index, Field obj);

    public native Field children(double index);

    public native double childrenLength();

    public native KeyValue customMetadata(double index, KeyValue obj);

    public native KeyValue customMetadata(double index);

    public native double customMetadataLength();

    public native DictionaryEncoding dictionary();

    public native DictionaryEncoding dictionary(DictionaryEncoding obj);

    public native Field.NameUnionType name();

    public native Field.NameUnionType name(Encoding optionalEncoding);

    public native boolean nullable();

    public native <T> T type(T obj);

    public native int typeType();
}
