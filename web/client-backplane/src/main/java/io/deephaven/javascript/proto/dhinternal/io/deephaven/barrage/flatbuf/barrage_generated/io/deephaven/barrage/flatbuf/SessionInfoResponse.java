package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf;

import elemental2.core.Int8Array;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.SessionInfoResponse",
    namespace = JsPackage.GLOBAL)
public class SessionInfoResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateMetadataHeaderVectorDataUnionType {
        @JsOverlay
        static SessionInfoResponse.CreateMetadataHeaderVectorDataUnionType of(Object o) {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateSessionTokenVectorDataUnionType {
        @JsOverlay
        static SessionInfoResponse.CreateSessionTokenVectorDataUnionType of(Object o) {
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

    public static native void addMetadataHeader(Builder builder, double metadataHeaderOffset);

    public static native void addSessionToken(Builder builder, double sessionTokenOffset);

    public static native void addTokenRefreshDeadlineMs(Builder builder,
        Long tokenRefreshDeadlineMs);

    @Deprecated
    public static native double createMetadataHeaderVector(
        Builder builder, SessionInfoResponse.CreateMetadataHeaderVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createMetadataHeaderVector(Builder builder, Int8Array data) {
        return createMetadataHeaderVector(
            builder,
            Js.<SessionInfoResponse.CreateMetadataHeaderVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createMetadataHeaderVector(Builder builder, JsArray<Double> data) {
        return createMetadataHeaderVector(
            builder,
            Js.<SessionInfoResponse.CreateMetadataHeaderVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createMetadataHeaderVector(Builder builder, Uint8Array data) {
        return createMetadataHeaderVector(
            builder,
            Js.<SessionInfoResponse.CreateMetadataHeaderVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createMetadataHeaderVector(Builder builder, double[] data) {
        return createMetadataHeaderVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double createSessionInfoResponse(
        Builder builder,
        double metadataHeaderOffset,
        double sessionTokenOffset,
        Long tokenRefreshDeadlineMs);

    @Deprecated
    public static native double createSessionTokenVector(
        Builder builder, SessionInfoResponse.CreateSessionTokenVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createSessionTokenVector(Builder builder, Int8Array data) {
        return createSessionTokenVector(
            builder,
            Js.<SessionInfoResponse.CreateSessionTokenVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createSessionTokenVector(Builder builder, JsArray<Double> data) {
        return createSessionTokenVector(
            builder,
            Js.<SessionInfoResponse.CreateSessionTokenVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createSessionTokenVector(Builder builder, Uint8Array data) {
        return createSessionTokenVector(
            builder,
            Js.<SessionInfoResponse.CreateSessionTokenVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createSessionTokenVector(Builder builder, double[] data) {
        return createSessionTokenVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double endSessionInfoResponse(Builder builder);

    public static native SessionInfoResponse getRootAsSessionInfoResponse(
        ByteBuffer bb, SessionInfoResponse obj);

    public static native SessionInfoResponse getRootAsSessionInfoResponse(ByteBuffer bb);

    public static native SessionInfoResponse getSizePrefixedRootAsSessionInfoResponse(
        ByteBuffer bb, SessionInfoResponse obj);

    public static native SessionInfoResponse getSizePrefixedRootAsSessionInfoResponse(
        ByteBuffer bb);

    public static native void startMetadataHeaderVector(Builder builder, double numElems);

    public static native void startSessionInfoResponse(Builder builder);

    public static native void startSessionTokenVector(Builder builder, double numElems);

    public ByteBuffer bb;
    public double bb_pos;

    public native SessionInfoResponse __init(double i, ByteBuffer bb);

    public native double metadataHeader(double index);

    public native Int8Array metadataHeaderArray();

    public native double metadataHeaderLength();

    public native double sessionToken(double index);

    public native Int8Array sessionTokenArray();

    public native double sessionTokenLength();

    public native Long tokenRefreshDeadlineMs();
}
