package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.literal.ValueCase;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.Literal",
    namespace = JsPackage.GLOBAL)
public class Literal {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static Literal.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getDoublevalue();

    @JsProperty
    double getLongvalue();

    @JsProperty
    double getNanotimevalue();

    @JsProperty
    String getStringvalue();

    @JsProperty
    boolean isBoolvalue();

    @JsProperty
    void setBoolvalue(boolean boolvalue);

    @JsProperty
    void setDoublevalue(double doublevalue);

    @JsProperty
    void setLongvalue(double longvalue);

    @JsProperty
    void setNanotimevalue(double nanotimevalue);

    @JsProperty
    void setStringvalue(String stringvalue);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static Literal.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getDoublevalue();

    @JsProperty
    double getLongvalue();

    @JsProperty
    double getNanotimevalue();

    @JsProperty
    String getStringvalue();

    @JsProperty
    boolean isBoolvalue();

    @JsProperty
    void setBoolvalue(boolean boolvalue);

    @JsProperty
    void setDoublevalue(double doublevalue);

    @JsProperty
    void setLongvalue(double longvalue);

    @JsProperty
    void setNanotimevalue(double nanotimevalue);

    @JsProperty
    void setStringvalue(String stringvalue);
  }

  public static native Literal deserializeBinary(Uint8Array bytes);

  public static native Literal deserializeBinaryFromReader(Literal message, Object reader);

  public static native void serializeBinaryToWriter(Literal message, Object writer);

  public static native Literal.ToObjectReturnType toObject(boolean includeInstance, Literal msg);

  public native void clearBoolvalue();

  public native void clearDoublevalue();

  public native void clearLongvalue();

  public native void clearNanotimevalue();

  public native void clearStringvalue();

  public native boolean getBoolvalue();

  public native double getDoublevalue();

  public native double getLongvalue();

  public native double getNanotimevalue();

  public native String getStringvalue();

  public native ValueCase getValueCase();

  public native boolean hasBoolvalue();

  public native boolean hasDoublevalue();

  public native boolean hasLongvalue();

  public native boolean hasNanotimevalue();

  public native boolean hasStringvalue();

  public native Uint8Array serializeBinary();

  public native void setBoolvalue(boolean value);

  public native void setDoublevalue(double value);

  public native void setLongvalue(double value);

  public native void setNanotimevalue(double value);

  public native void setStringvalue(String value);

  public native Literal.ToObjectReturnType0 toObject();

  public native Literal.ToObjectReturnType0 toObject(boolean includeInstance);
}
