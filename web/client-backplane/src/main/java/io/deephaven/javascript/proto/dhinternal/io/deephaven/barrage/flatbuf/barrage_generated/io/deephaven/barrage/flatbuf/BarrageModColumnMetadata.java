package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf;

import elemental2.core.Int8Array;
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
    name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarrageModColumnMetadata",
    namespace = JsPackage.GLOBAL)
public class BarrageModColumnMetadata {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateModifiedRowsVectorDataUnionType {
        @JsOverlay
        static BarrageModColumnMetadata.CreateModifiedRowsVectorDataUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default Int8Array asInt8Array() {
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
        default boolean isInt8Array() {
            return (Object) this instanceof Int8Array;
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

    public static native void addModifiedRows(Builder builder, double modifiedRowsOffset);

    public static native double createBarrageModColumnMetadata(
        Builder builder, double modifiedRowsOffset);

    @Deprecated
    public static native double createModifiedRowsVector(
        Builder builder, BarrageModColumnMetadata.CreateModifiedRowsVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createModifiedRowsVector(Builder builder, Int8Array data) {
        return createModifiedRowsVector(
            builder,
            Js.<BarrageModColumnMetadata.CreateModifiedRowsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createModifiedRowsVector(Builder builder, JsArray<Double> data) {
        return createModifiedRowsVector(
            builder,
            Js.<BarrageModColumnMetadata.CreateModifiedRowsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createModifiedRowsVector(Builder builder, Uint8Array data) {
        return createModifiedRowsVector(
            builder,
            Js.<BarrageModColumnMetadata.CreateModifiedRowsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createModifiedRowsVector(Builder builder, double[] data) {
        return createModifiedRowsVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double endBarrageModColumnMetadata(Builder builder);

    public static native BarrageModColumnMetadata getRootAsBarrageModColumnMetadata(
        ByteBuffer bb, BarrageModColumnMetadata obj);

    public static native BarrageModColumnMetadata getRootAsBarrageModColumnMetadata(ByteBuffer bb);

    public static native BarrageModColumnMetadata getSizePrefixedRootAsBarrageModColumnMetadata(
        ByteBuffer bb, BarrageModColumnMetadata obj);

    public static native BarrageModColumnMetadata getSizePrefixedRootAsBarrageModColumnMetadata(
        ByteBuffer bb);

    public static native void startBarrageModColumnMetadata(Builder builder);

    public static native void startModifiedRowsVector(Builder builder, double numElems);

    public ByteBuffer bb;
    public double bb_pos;

    public native BarrageModColumnMetadata __init(double i, ByteBuffer bb);

    public native double modifiedRows(double index);

    public native Int8Array modifiedRowsArray();

    public native double modifiedRowsLength();
}
