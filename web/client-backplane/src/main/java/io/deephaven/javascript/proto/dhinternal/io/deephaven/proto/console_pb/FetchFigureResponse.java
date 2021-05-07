package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FetchFigureResponse",
    namespace = JsPackage.GLOBAL)
public class FetchFigureResponse {
  public static native FetchFigureResponse deserializeBinary(Uint8Array bytes);

  public static native FetchFigureResponse deserializeBinaryFromReader(
      FetchFigureResponse message, Object reader);

  public static native void serializeBinaryToWriter(FetchFigureResponse message, Object writer);

  public static native Object toObject(boolean includeInstance, FetchFigureResponse msg);

  public native Uint8Array serializeBinary();

  public native Object toObject();

  public native Object toObject(boolean includeInstance);
}
