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
        name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarrageMessageWrapper",
        namespace = JsPackage.GLOBAL)
public class BarrageMessageWrapper {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateMsgPayloadVectorDataUnionType {
        @JsOverlay
        static BarrageMessageWrapper.CreateMsgPayloadVectorDataUnionType of(Object o) {
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

    public static native void addMagic(Builder builder, double magic);

    public static native void addMsgPayload(Builder builder, double msgPayloadOffset);

    public static native void addMsgType(Builder builder, int msgType);

    public static native double createBarrageMessageWrapper(
            Builder builder, double magic, int msgType, double msgPayloadOffset);

    @Deprecated
    public static native double createMsgPayloadVector(
            Builder builder, BarrageMessageWrapper.CreateMsgPayloadVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createMsgPayloadVector(Builder builder, Int8Array data) {
        return createMsgPayloadVector(
                builder, Js.<BarrageMessageWrapper.CreateMsgPayloadVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createMsgPayloadVector(Builder builder, JsArray<Double> data) {
        return createMsgPayloadVector(
                builder, Js.<BarrageMessageWrapper.CreateMsgPayloadVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createMsgPayloadVector(Builder builder, Uint8Array data) {
        return createMsgPayloadVector(
                builder, Js.<BarrageMessageWrapper.CreateMsgPayloadVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createMsgPayloadVector(Builder builder, double[] data) {
        return createMsgPayloadVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double endBarrageMessageWrapper(Builder builder);

    public static native BarrageMessageWrapper getRootAsBarrageMessageWrapper(
            ByteBuffer bb, BarrageMessageWrapper obj);

    public static native BarrageMessageWrapper getRootAsBarrageMessageWrapper(ByteBuffer bb);

    public static native BarrageMessageWrapper getSizePrefixedRootAsBarrageMessageWrapper(
            ByteBuffer bb, BarrageMessageWrapper obj);

    public static native BarrageMessageWrapper getSizePrefixedRootAsBarrageMessageWrapper(
            ByteBuffer bb);

    public static native void startBarrageMessageWrapper(Builder builder);

    public static native void startMsgPayloadVector(Builder builder, double numElems);

    public ByteBuffer bb;
    public double bb_pos;

    public native BarrageMessageWrapper __init(double i, ByteBuffer bb);

    public native double magic();

    public native double msgPayload(double index);

    public native Int8Array msgPayloadArray();

    public native double msgPayloadLength();

    public native int msgType();
}
