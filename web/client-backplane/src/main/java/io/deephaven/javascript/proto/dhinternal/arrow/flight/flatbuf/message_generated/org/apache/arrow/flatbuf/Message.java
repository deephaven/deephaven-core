package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.KeyValue;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
        isNative = true,
        name = "dhinternal.arrow.flight.flatbuf.Message_generated.org.apache.arrow.flatbuf.Message",
        namespace = JsPackage.GLOBAL)
public class Message {
    public static native void addBodyLength(Builder builder, Long bodyLength);

    public static native void addCustomMetadata(Builder builder, double customMetadataOffset);

    public static native void addHeader(Builder builder, double headerOffset);

    public static native void addHeaderType(Builder builder, int headerType);

    public static native void addVersion(Builder builder, int version);

    public static native double createCustomMetadataVector(Builder builder, JsArray<Double> data);

    @JsOverlay
    public static final double createCustomMetadataVector(Builder builder, double[] data) {
        return createCustomMetadataVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
    }

    public static native double createMessage(
            Builder builder,
            int version,
            int headerType,
            double headerOffset,
            Long bodyLength,
            double customMetadataOffset);

    public static native double endMessage(Builder builder);

    public static native void finishMessageBuffer(Builder builder, double offset);

    public static native void finishSizePrefixedMessageBuffer(Builder builder, double offset);

    public static native Message getRootAsMessage(ByteBuffer bb, Message obj);

    public static native Message getRootAsMessage(ByteBuffer bb);

    public static native Message getSizePrefixedRootAsMessage(ByteBuffer bb, Message obj);

    public static native Message getSizePrefixedRootAsMessage(ByteBuffer bb);

    public static native void startCustomMetadataVector(Builder builder, double numElems);

    public static native void startMessage(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native Message __init(double i, ByteBuffer bb);

    public native Long bodyLength();

    public native KeyValue customMetadata(double index, KeyValue obj);

    public native KeyValue customMetadata(double index);

    public native double customMetadataLength();

    public native <T> T header(T obj);

    public native int headerType();

    public native int version();
}
