package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.BusinessCalendarDescriptor",
    namespace = JsPackage.GLOBAL)
public class BusinessCalendarDescriptor {
  public static native BusinessCalendarDescriptor deserializeBinary(Uint8Array bytes);

  public static native BusinessCalendarDescriptor deserializeBinaryFromReader(
      BusinessCalendarDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(
      BusinessCalendarDescriptor message, Object writer);

  public static native Object toObject(boolean includeInstance, BusinessCalendarDescriptor msg);

  public native Uint8Array serializeBinary();

  public native Object toObject();

  public native Object toObject(boolean includeInstance);
}
