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
        name = "dhinternal.io.deephaven.barrage.flatbuf.Barrage_generated.io.deephaven.barrage.flatbuf.BarrageSnapshotRequest",
        namespace = JsPackage.GLOBAL)
public class BarrageSnapshotRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateColumnsVectorDataUnionType {
        @JsOverlay
        static BarrageSnapshotRequest.CreateColumnsVectorDataUnionType of(Object o) {
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
        static BarrageSnapshotRequest.CreateTicketVectorDataUnionType of(Object o) {
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
        static BarrageSnapshotRequest.CreateViewportVectorDataUnionType of(Object o) {
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

    public static native void addReverseViewport(Builder builder, boolean reverseViewport);

    public static native void addSnapshotOptions(Builder builder, double snapshotOptionsOffset);

    public static native void addTicket(Builder builder, double ticketOffset);

    public static native void addViewport(Builder builder, double viewportOffset);

    @Deprecated
    public static native double createColumnsVector(
            Builder builder, BarrageSnapshotRequest.CreateColumnsVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createColumnsVector(Builder builder, Int8Array data) {
        return createColumnsVector(
                builder, Js.<BarrageSnapshotRequest.CreateColumnsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createColumnsVector(Builder builder, JsArray<Double> data) {
        return createColumnsVector(
                builder, Js.<BarrageSnapshotRequest.CreateColumnsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createColumnsVector(Builder builder, Uint8Array data) {
        return createColumnsVector(
                builder, Js.<BarrageSnapshotRequest.CreateColumnsVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createColumnsVector(Builder builder, double[] data) {
        return createColumnsVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    @Deprecated
    public static native double createTicketVector(
            Builder builder, BarrageSnapshotRequest.CreateTicketVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, Int8Array data) {
        return createTicketVector(
                builder, Js.<BarrageSnapshotRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, JsArray<Double> data) {
        return createTicketVector(
                builder, Js.<BarrageSnapshotRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, Uint8Array data) {
        return createTicketVector(
                builder, Js.<BarrageSnapshotRequest.CreateTicketVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createTicketVector(Builder builder, double[] data) {
        return createTicketVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    @Deprecated
    public static native double createViewportVector(
            Builder builder, BarrageSnapshotRequest.CreateViewportVectorDataUnionType data);

    @JsOverlay
    @Deprecated
    public static final double createViewportVector(Builder builder, Int8Array data) {
        return createViewportVector(
                builder, Js.<BarrageSnapshotRequest.CreateViewportVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createViewportVector(Builder builder, JsArray<Double> data) {
        return createViewportVector(
                builder, Js.<BarrageSnapshotRequest.CreateViewportVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createViewportVector(Builder builder, Uint8Array data) {
        return createViewportVector(
                builder, Js.<BarrageSnapshotRequest.CreateViewportVectorDataUnionType>uncheckedCast(data));
    }

    @JsOverlay
    @Deprecated
    public static final double createViewportVector(Builder builder, double[] data) {
        return createViewportVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double endBarrageSnapshotRequest(Builder builder);

    public static native BarrageSnapshotRequest getRootAsBarrageSnapshotRequest(
            ByteBuffer bb, BarrageSnapshotRequest obj);

    public static native BarrageSnapshotRequest getRootAsBarrageSnapshotRequest(ByteBuffer bb);

    public static native BarrageSnapshotRequest getSizePrefixedRootAsBarrageSnapshotRequest(
            ByteBuffer bb, BarrageSnapshotRequest obj);

    public static native BarrageSnapshotRequest getSizePrefixedRootAsBarrageSnapshotRequest(
            ByteBuffer bb);

    public static native void startBarrageSnapshotRequest(Builder builder);

    public static native void startColumnsVector(Builder builder, double numElems);

    public static native void startTicketVector(Builder builder, double numElems);

    public static native void startViewportVector(Builder builder, double numElems);

    public ByteBuffer bb;
    public double bb_pos;

    public native BarrageSnapshotRequest __init(double i, ByteBuffer bb);

    public native double columns(double index);

    public native Int8Array columnsArray();

    public native double columnsLength();

    public native boolean reverseViewport();

    public native BarrageSnapshotOptions snapshotOptions();

    public native BarrageSnapshotOptions snapshotOptions(BarrageSnapshotOptions obj);

    public native double ticket(double index);

    public native Int8Array ticketArray();

    public native double ticketLength();

    public native double viewport(double index);

    public native Int8Array viewportArray();

    public native double viewportLength();
}
