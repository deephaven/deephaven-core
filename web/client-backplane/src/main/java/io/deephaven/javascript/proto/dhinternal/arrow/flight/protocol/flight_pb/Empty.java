package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb.Empty",
    namespace = JsPackage.GLOBAL)
public class Empty {
  public static native Empty deserializeBinary(Uint8Array bytes);

  public static native Empty deserializeBinaryFromReader(Empty message, Object reader);

  public static native void serializeBinaryToWriter(Empty message, Object writer);

  public static native Object toObject(boolean includeInstance, Empty msg);

  public native Uint8Array serializeBinary();

  public native Object toObject();

  public native Object toObject(boolean includeInstance);
}
