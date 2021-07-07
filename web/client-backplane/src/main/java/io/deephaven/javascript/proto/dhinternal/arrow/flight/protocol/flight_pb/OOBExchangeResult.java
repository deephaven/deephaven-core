package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.flight_pb.OOBExchangeResult",
    namespace = JsPackage.GLOBAL)
public class OOBExchangeResult {
  public static native OOBExchangeResult deserializeBinary(Uint8Array bytes);

  public static native OOBExchangeResult deserializeBinaryFromReader(
      OOBExchangeResult message, Object reader);

  public static native void serializeBinaryToWriter(OOBExchangeResult message, Object writer);

  public static native Object toObject(boolean includeInstance, OOBExchangeResult msg);

  public native Uint8Array serializeBinary();

  public native Object toObject();

  public native Object toObject(boolean includeInstance);
}
