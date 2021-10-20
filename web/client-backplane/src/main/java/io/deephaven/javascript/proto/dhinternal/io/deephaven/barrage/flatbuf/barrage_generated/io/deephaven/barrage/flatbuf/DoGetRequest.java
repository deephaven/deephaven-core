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
        name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.DoGetRequest",
        namespace = JsPackage.GLOBAL)
public class DoGetRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateColumnsVectorDataUnionType {
        @JsOverlay
        static DoGetRequest.CreateColumnsVectorDataUnionType of(Object o) {
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
    public interface CreateTicketVectorDataUnionType {
        @JsOverlay
        static DoGetRequest.CreateTicketVectorDataUnionType of(Object o) {
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
    public interface CreateViewportVectorDataUnionType {
        @JsOverlay
        static DoGetRequest.CreateViewportVectorDataUnionType of(Object o) {
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

    public static native void addColumns(Builder builder, double columnsOffset);

    public static native void addSubscriptionOptions(
            Builder builder, double subscriptionOptionsOffset);

    public static native void addTicket(Builder builder, double ticketOffset);

    public static native void addViewport(Builder builder, double viewportOffset);

    @Deprecated
    public static native double createColumnsVector(
            Builder builder, DoGetRequest.CreateColumnsVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createColumnsVector(Builder builder, Int8Array data) {
        return createColumnsVector(
                builder, Js.<DoGetRequest.CreateColumnsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createColumnsVector(Builder builder, JsArray<Double> data) {
        return createColumnsVector(
                builder, Js.<DoGetRequest.CreateColumnsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createColumnsVector(Builder builder, Uint8Array data) {
        return createColumnsVector(
                builder, Js.<DoGetRequest.CreateColumnsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createColumnsVector(Builder builder, double[] data) {
        return createColumnsVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    @Deprecated
    public static native double createTicketVector(
            Builder builder, DoGetRequest.CreateTicketVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, Int8Array data) {
        return createTicketVector(
                builder, Js.<DoGetRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, JsArray<Double> data) {
        return createTicketVector(
                builder, Js.<DoGetRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, Uint8Array data) {
        return createTicketVector(
                builder, Js.<DoGetRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, double[] data) {
        return createTicketVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    @Deprecated
    public static native double createViewportVector(
            Builder builder, DoGetRequest.CreateViewportVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createViewportVector(Builder builder, Int8Array data) {
        return createViewportVector(
                builder, Js.<DoGetRequest.CreateViewportVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createViewportVector(Builder builder, JsArray<Double> data) {
        return createViewportVector(
                builder, Js.<DoGetRequest.CreateViewportVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createViewportVector(Builder builder, Uint8Array data) {
        return createViewportVector(
                builder, Js.<DoGetRequest.CreateViewportVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createViewportVector(Builder builder, double[] data) {
        return createViewportVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double endDoGetRequest(Builder builder);

    public static native DoGetRequest getRootAsDoGetRequest(ByteBuffer bb, DoGetRequest obj);

    public static native DoGetRequest getRootAsDoGetRequest(ByteBuffer bb);

    public static native DoGetRequest getSizePrefixedRootAsDoGetRequest(
            ByteBuffer bb, DoGetRequest obj);

    public static native DoGetRequest getSizePrefixedRootAsDoGetRequest(ByteBuffer bb);

    public static native void startColumnsVector(Builder builder, double numElems);

    public static native void startDoGetRequest(Builder builder);

    public static native void startTicketVector(Builder builder, double numElems);

    public static native void startViewportVector(Builder builder, double numElems);

    public ByteBuffer bb;
    public double bb_pos;

    public native DoGetRequest __init(double i, ByteBuffer bb);

    public native double columns(double index);

    public native Int8Array columnsArray();

    public native double columnsLength();

    public native BarrageSubscriptionOptions subscriptionOptions();

    public native BarrageSubscriptionOptions subscriptionOptions(BarrageSubscriptionOptions obj);

    public native double ticket(double index);

    public native Int8Array ticketArray();

    public native double ticketLength();

    public native double viewport(double index);

    public native Int8Array viewportArray();

    public native double viewportLength();
}
