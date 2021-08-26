package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import elemental2.core.Int32Array;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Union",
    namespace = JsPackage.GLOBAL)
public class Union {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateTypeIdsVectorDataUnionType {
        @JsOverlay
        static Union.CreateTypeIdsVectorDataUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default Int32Array asInt32Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default JsArray<Double> asJsArray() {
            return Js.cast(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isInt32Array() {
            return (Object) this instanceof Int32Array;
        }

        @JsOverlay
        default boolean isJsArray() {
            return (Object) this instanceof JsArray;
        }

        @JsOverlay
        default boolean isUint8Array() {
            return (Object) this instanceof Uint8Array;
        }
    }

    public static native void addMode(Builder builder, int mode);

    public static native void addTypeIds(Builder builder, double typeIdsOffset);

    @Deprecated
    public static native double createTypeIdsVector(
        Builder builder, Union.CreateTypeIdsVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createTypeIdsVector(Builder builder, Int32Array data) {
        return createTypeIdsVector(
            builder, Js.<Union.CreateTypeIdsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTypeIdsVector(Builder builder, JsArray<Double> data) {
        return createTypeIdsVector(
            builder, Js.<Union.CreateTypeIdsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTypeIdsVector(Builder builder, Uint8Array data) {
        return createTypeIdsVector(
            builder, Js.<Union.CreateTypeIdsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTypeIdsVector(Builder builder, double[] data) {
        return createTypeIdsVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double createUnion(Builder builder, int mode, double typeIdsOffset);

    public static native double endUnion(Builder builder);

    public static native Union getRootAsUnion(ByteBuffer bb, Union obj);

    public static native Union getRootAsUnion(ByteBuffer bb);

    public static native Union getSizePrefixedRootAsUnion(ByteBuffer bb, Union obj);

    public static native Union getSizePrefixedRootAsUnion(ByteBuffer bb);

    public static native void startTypeIdsVector(Builder builder, double numElems);

    public static native void startUnion(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Union __init(double i, ByteBuffer bb);

    public native int mode();

    public native double typeIds(double index);

    public native Int32Array typeIdsArray();

    public native double typeIdsLength();
}
