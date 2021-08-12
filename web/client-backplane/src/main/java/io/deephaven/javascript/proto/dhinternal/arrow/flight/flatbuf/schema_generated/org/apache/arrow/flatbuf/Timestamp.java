package io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Encoding;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.flatbuf.Schema_generated.org.apache.arrow.flatbuf.Timestamp",
    namespace = JsPackage.GLOBAL)
public class Timestamp {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface TimezoneUnionType {
    @JsOverlay
    static Timestamp.TimezoneUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default String asString() {
      return Js.asString(this);
    }

    @JsOverlay
    default Uint8Array asUint8Array() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isString() {
      return (Object) this instanceof String;
    }

    @JsOverlay
    default boolean isUint8Array() {
      return (Object) this instanceof Uint8Array;
    }
  }

  public static native void addTimezone(Builder builder, double timezoneOffset);

  public static native void addUnit(Builder builder, int unit);

  public static native double createTimestamp(
      Builder builder, int unit, double timezoneOffset);

  public static native double endTimestamp(Builder builder);

  public static native Timestamp getRootAsTimestamp(ByteBuffer bb, Timestamp obj);

  public static native Timestamp getRootAsTimestamp(ByteBuffer bb);

  public static native Timestamp getSizePrefixedRootAsTimestamp(ByteBuffer bb, Timestamp obj);

  public static native Timestamp getSizePrefixedRootAsTimestamp(ByteBuffer bb);

  public static native void startTimestamp(Builder builder);

  public ByteBuffer bb;
  public double bb_pos;

  public native Timestamp __init(double i, ByteBuffer bb);

  public native Timestamp.TimezoneUnionType timezone();

  public native Timestamp.TimezoneUnionType timezone(Encoding optionalEncoding);

  public native int unit();
}
