package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.DictionaryEncoding",
    namespace = JsPackage.GLOBAL)
public class DictionaryEncoding {
    public static native void addDictionaryKind(Builder builder, int dictionaryKind);

    public static native void addId(Builder builder, Long id);

    public static native void addIndexType(Builder builder, double indexTypeOffset);

    public static native void addIsOrdered(Builder builder, boolean isOrdered);

    public static native double endDictionaryEncoding(Builder builder);

    public static native DictionaryEncoding getRootAsDictionaryEncoding(
        ByteBuffer bb, DictionaryEncoding obj);

    public static native DictionaryEncoding getRootAsDictionaryEncoding(ByteBuffer bb);

    public static native DictionaryEncoding getSizePrefixedRootAsDictionaryEncoding(
        ByteBuffer bb, DictionaryEncoding obj);

    public static native DictionaryEncoding getSizePrefixedRootAsDictionaryEncoding(ByteBuffer bb);

    public static native void startDictionaryEncoding(Builder builder);

    public ByteBuffer bb;
    public double bb_pos;

    public native DictionaryEncoding __init(double i, ByteBuffer bb);

    public native int dictionaryKind();

    public native Long id();

    public native Int indexType();

    public native Int indexType(Int obj);

    public native boolean isOrdered();
}
