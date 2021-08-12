package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf;

import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name =
        "dhinternal.arrow.flight.flatbuf.Message_generated.org.apache.arrow.flatbuf.DictionaryBatch",
    namespace = JsPackage.GLOBAL)
public class DictionaryBatch {
  public static native void addData(Builder builder, double dataOffset);

  public static native void addId(Builder builder, Long id);

  public static native void addIsDelta(Builder builder, boolean isDelta);

  public static native double endDictionaryBatch(Builder builder);

  public static native DictionaryBatch getRootAsDictionaryBatch(ByteBuffer bb, DictionaryBatch obj);

  public static native DictionaryBatch getRootAsDictionaryBatch(ByteBuffer bb);

  public static native DictionaryBatch getSizePrefixedRootAsDictionaryBatch(
      ByteBuffer bb, DictionaryBatch obj);

  public static native DictionaryBatch getSizePrefixedRootAsDictionaryBatch(ByteBuffer bb);

  public static native void startDictionaryBatch(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native DictionaryBatch __init(double i, ByteBuffer bb);

  public native RecordBatch data();

  public native RecordBatch data(RecordBatch obj);

  public native Long id();

  public native boolean isDelta();
}
