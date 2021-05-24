package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.FloatMapWithDefault",
    namespace = JsPackage.GLOBAL)
public class FloatMapWithDefault {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static FloatMapWithDefault.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getDefaultfloat();

    @JsProperty
    JsArray<String> getKeysList();

    @JsProperty
    JsArray<Double> getValuesList();

    @JsProperty
    void setDefaultfloat(double defaultfloat);

    @JsProperty
    void setKeysList(JsArray<String> keysList);

    @JsOverlay
    default void setKeysList(String[] keysList) {
      setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
    }

    @JsProperty
    void setValuesList(JsArray<Double> valuesList);

    @JsOverlay
    default void setValuesList(double[] valuesList) {
      setValuesList(Js.<JsArray<Double>>uncheckedCast(valuesList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static FloatMapWithDefault.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getDefaultfloat();

    @JsProperty
    JsArray<String> getKeysList();

    @JsProperty
    JsArray<Double> getValuesList();

    @JsProperty
    void setDefaultfloat(double defaultfloat);

    @JsProperty
    void setKeysList(JsArray<String> keysList);

    @JsOverlay
    default void setKeysList(String[] keysList) {
      setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
    }

    @JsProperty
    void setValuesList(JsArray<Double> valuesList);

    @JsOverlay
    default void setValuesList(double[] valuesList) {
      setValuesList(Js.<JsArray<Double>>uncheckedCast(valuesList));
    }
  }

  public static native FloatMapWithDefault deserializeBinary(Uint8Array bytes);

  public static native FloatMapWithDefault deserializeBinaryFromReader(
      FloatMapWithDefault message, Object reader);

  public static native void serializeBinaryToWriter(FloatMapWithDefault message, Object writer);

  public static native FloatMapWithDefault.ToObjectReturnType toObject(
      boolean includeInstance, FloatMapWithDefault msg);

  public native String addKeys(String value, double index);

  public native String addKeys(String value);

  public native double addValues(double value, double index);

  public native double addValues(double value);

  public native void clearKeysList();

  public native void clearValuesList();

  public native double getDefaultfloat();

  public native JsArray<String> getKeysList();

  public native JsArray<Double> getValuesList();

  public native Uint8Array serializeBinary();

  public native void setDefaultfloat(double value);

  public native void setKeysList(JsArray<String> value);

  @JsOverlay
  public final void setKeysList(String[] value) {
    setKeysList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setValuesList(JsArray<Double> value);

  @JsOverlay
  public final void setValuesList(double[] value) {
    setValuesList(Js.<JsArray<Double>>uncheckedCast(value));
  }

  public native FloatMapWithDefault.ToObjectReturnType0 toObject();

  public native FloatMapWithDefault.ToObjectReturnType0 toObject(boolean includeInstance);
}
