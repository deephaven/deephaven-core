package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.flight_pb.OOBPutResult",
    namespace = JsPackage.GLOBAL)
public class OOBPutResult {
  public static native OOBPutResult deserializeBinary(Uint8Array bytes);

  public static native OOBPutResult deserializeBinaryFromReader(
      OOBPutResult message, Object reader);

  public static native void serializeBinaryToWriter(OOBPutResult message, Object writer);

  public static native Object toObject(boolean includeInstance, OOBPutResult msg);

  public native Uint8Array serializeBinary();

  public native Object toObject();

  public native Object toObject(boolean includeInstance);
}
