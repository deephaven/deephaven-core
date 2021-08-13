package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import elemental2.core.JsArray;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Long;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Schema",
    namespace = JsPackage.GLOBAL)
public class Schema {
  public static native void addCustomMetadata(Builder builder, double customMetadataOffset);

  public static native void addEndianness(Builder builder, int endianness);

  public static native void addFeatures(Builder builder, double featuresOffset);

  public static native void addFields(Builder builder, double fieldsOffset);

  public static native double createCustomMetadataVector(Builder builder, JsArray<Double> data);

  @JsOverlay
  public static final double createCustomMetadataVector(Builder builder, double[] data) {
    return createCustomMetadataVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  public static native double createFeaturesVector(Builder builder, JsArray<Object> data);

  @JsOverlay
  public static final double createFeaturesVector(Builder builder, Object[] data) {
    return createFeaturesVector(builder, Js.<JsArray<Object>>uncheckedCast(data));
  }

  public static native double createFieldsVector(Builder builder, JsArray<Double> data);

  @JsOverlay
  public static final double createFieldsVector(Builder builder, double[] data) {
    return createFieldsVector(builder, Js.<JsArray<Double>>uncheckedCast(data));
  }

  public static native double createSchema(
      Builder builder,
      int endianness,
      double fieldsOffset,
      double customMetadataOffset,
      double featuresOffset);

  public static native double endSchema(Builder builder);

  public static native void finishSchemaBuffer(Builder builder, double offset);

  public static native void finishSizePrefixedSchemaBuffer(Builder builder, double offset);

  public static native Schema getRootAsSchema(ByteBuffer bb, Schema obj);

  public static native Schema getRootAsSchema(ByteBuffer bb);

  public static native Schema getSizePrefixedRootAsSchema(ByteBuffer bb, Schema obj);

  public static native Schema getSizePrefixedRootAsSchema(ByteBuffer bb);

  public static native void startCustomMetadataVector(Builder builder, double numElems);

  public static native void startFeaturesVector(Builder builder, double numElems);

  public static native void startFieldsVector(Builder builder, double numElems);

  public static native void startSchema(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Schema __init(double i, ByteBuffer bb);

  public native KeyValue customMetadata(double index, KeyValue obj);

  public native KeyValue customMetadata(double index);

  public native double customMetadataLength();

  public native int endianness();

  public native Long features(double index);

  public native double featuresLength();

  public native Field fields(double index, Field obj);

  public native Field fields(double index);

  public native double fieldsLength();
}
