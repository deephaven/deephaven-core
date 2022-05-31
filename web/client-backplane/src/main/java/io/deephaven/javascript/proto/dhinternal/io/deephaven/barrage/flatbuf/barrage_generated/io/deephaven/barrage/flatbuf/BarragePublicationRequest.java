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
        name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarragePublicationRequest",
        namespace = JsPackage.GLOBAL)
public class BarragePublicationRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateTicketVectorDataUnionType {
        @JsOverlay
        static BarragePublicationRequest.CreateTicketVectorDataUnionType of(Object o) {
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

    public static native void addPublishOptions(Builder builder, double publishOptionsOffset);

    public static native void addTicket(Builder builder, double ticketOffset);

    @Deprecated
    public static native double createTicketVector(
            Builder builder, BarragePublicationRequest.CreateTicketVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, Int8Array data) {
        return createTicketVector(
                builder, Js.<BarragePublicationRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, JsArray<Double> data) {
        return createTicketVector(
                builder, Js.<BarragePublicationRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, Uint8Array data) {
        return createTicketVector(
                builder, Js.<BarragePublicationRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, double[] data) {
        return createTicketVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double endBarragePublicationRequest(Builder builder);

    public static native BarragePublicationRequest getRootAsBarragePublicationRequest(
            ByteBuffer bb, BarragePublicationRequest obj);

    public static native BarragePublicationRequest getRootAsBarragePublicationRequest(ByteBuffer bb);

    public static native BarragePublicationRequest getSizePrefixedRootAsBarragePublicationRequest(
            ByteBuffer bb, BarragePublicationRequest obj);

    public static native BarragePublicationRequest getSizePrefixedRootAsBarragePublicationRequest(
            ByteBuffer bb);

    public static native void startBarragePublicationRequest(Builder builder);

    public static native void startTicketVector(Builder builder, double numElems);

    public ByteBuffer bb;
    public double bb_pos;

    public native BarragePublicationRequest __init(double i, ByteBuffer bb);

    public native BarragePublicationOptions publishOptions();

    public native BarragePublicationOptions publishOptions(BarragePublicationOptions obj);

    public native double ticket(double index);

    public native Int8Array ticketArray();

    public native double ticketLength();
}
