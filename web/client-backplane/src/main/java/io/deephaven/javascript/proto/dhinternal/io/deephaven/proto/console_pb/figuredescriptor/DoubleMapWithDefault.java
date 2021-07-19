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
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.DoubleMapWithDefault",
    namespace = JsPackage.GLOBAL)
public class DoubleMapWithDefault {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static DoubleMapWithDefault.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getDefaultDouble();

    @JsProperty
    JsArray<String> getKeysList();

    @JsProperty
    JsArray<Double> getValuesList();

    @JsProperty
    void setDefaultDouble(double defaultDouble);

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
    static DoubleMapWithDefault.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getDefaultDouble();

    @JsProperty
    JsArray<String> getKeysList();

    @JsProperty
    JsArray<Double> getValuesList();

    @JsProperty
    void setDefaultDouble(double defaultDouble);

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

  public static native DoubleMapWithDefault deserializeBinary(Uint8Array bytes);

  public static native DoubleMapWithDefault deserializeBinaryFromReader(
      DoubleMapWithDefault message, Object reader);

  public static native void serializeBinaryToWriter(DoubleMapWithDefault message, Object writer);

  public static native DoubleMapWithDefault.ToObjectReturnType toObject(
      boolean includeInstance, DoubleMapWithDefault msg);

  public native String addKeys(String value, double index);

  public native String addKeys(String value);

  public native double addValues(double value, double index);

  public native double addValues(double value);

  public native void clearKeysList();

  public native void clearValuesList();

  public native double getDefaultDouble();

  public native JsArray<String> getKeysList();

  public native JsArray<Double> getValuesList();

  public native Uint8Array serializeBinary();

  public native void setDefaultDouble(double value);

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

  public native DoubleMapWithDefault.ToObjectReturnType0 toObject();

  public native DoubleMapWithDefault.ToObjectReturnType0 toObject(boolean includeInstance);
}
