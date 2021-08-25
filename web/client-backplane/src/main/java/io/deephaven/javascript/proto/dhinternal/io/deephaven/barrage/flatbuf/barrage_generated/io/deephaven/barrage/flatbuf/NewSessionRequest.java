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
    name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.NewSessionRequest",
    namespace = JsPackage.GLOBAL)
public class NewSessionRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreatePayloadVectorDataUnionType {
        @JsOverlay
        static NewSessionRequest.CreatePayloadVectorDataUnionType of(Object o) {
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

    public static native void addPayload(Builder builder, double payloadOffset);

    public static native void addProtocolVersion(Builder builder, double protocolVersion);

    public static native double createNewSessionRequest(
        Builder builder, double protocolVersion, double payloadOffset);

    @Deprecated
    public static native double createPayloadVector(
        Builder builder, NewSessionRequest.CreatePayloadVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createPayloadVector(Builder builder, Int8Array data) {
        return createPayloadVector(
            builder, Js.<NewSessionRequest.CreatePayloadVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createPayloadVector(Builder builder, JsArray<Double> data) {
        return createPayloadVector(
            builder, Js.<NewSessionRequest.CreatePayloadVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createPayloadVector(Builder builder, Uint8Array data) {
        return createPayloadVector(
            builder, Js.<NewSessionRequest.CreatePayloadVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createPayloadVector(Builder builder, double[] data) {
        return createPayloadVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double endNewSessionRequest(Builder builder);

    public static native NewSessionRequest getRootAsNewSessionRequest(
        ByteBuffer bb, NewSessionRequest obj);

    public static native NewSessionRequest getRootAsNewSessionRequest(ByteBuffer bb);

    public static native NewSessionRequest getSizePrefixedRootAsNewSessionRequest(
        ByteBuffer bb, NewSessionRequest obj);

    public static native NewSessionRequest getSizePrefixedRootAsNewSessionRequest(ByteBuffer bb);

    public static native void startNewSessionRequest(Builder builder);

    public static native void startPayloadVector(Builder builder, double numElems);

    public ByteBuffer bb;
    public double bb_pos;

    public native NewSessionRequest __init(double i, ByteBuffer bb);

    public native double payload(double index);

    public native Int8Array payloadArray();

    public native double payloadLength();

    public native double protocolVersion();
}
