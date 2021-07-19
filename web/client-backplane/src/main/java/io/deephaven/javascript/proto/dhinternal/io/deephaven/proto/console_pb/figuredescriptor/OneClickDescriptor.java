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
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.OneClickDescriptor",
    namespace = JsPackage.GLOBAL)
public class OneClickDescriptor {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static OneClickDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnTypesList();

    @JsProperty
    JsArray<String> getColumnsList();

    @JsProperty
    boolean isRequireAllFiltersToDisplay();

    @JsProperty
    void setColumnTypesList(JsArray<String> columnTypesList);

    @JsOverlay
    default void setColumnTypesList(String[] columnTypesList) {
      setColumnTypesList(Js.<JsArray<String>>uncheckedCast(columnTypesList));
    }

    @JsProperty
    void setColumnsList(JsArray<String> columnsList);

    @JsOverlay
    default void setColumnsList(String[] columnsList) {
      setColumnsList(Js.<JsArray<String>>uncheckedCast(columnsList));
    }

    @JsProperty
    void setRequireAllFiltersToDisplay(boolean requireAllFiltersToDisplay);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static OneClickDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnTypesList();

    @JsProperty
    JsArray<String> getColumnsList();

    @JsProperty
    boolean isRequireAllFiltersToDisplay();

    @JsProperty
    void setColumnTypesList(JsArray<String> columnTypesList);

    @JsOverlay
    default void setColumnTypesList(String[] columnTypesList) {
      setColumnTypesList(Js.<JsArray<String>>uncheckedCast(columnTypesList));
    }

    @JsProperty
    void setColumnsList(JsArray<String> columnsList);

    @JsOverlay
    default void setColumnsList(String[] columnsList) {
      setColumnsList(Js.<JsArray<String>>uncheckedCast(columnsList));
    }

    @JsProperty
    void setRequireAllFiltersToDisplay(boolean requireAllFiltersToDisplay);
  }

  public static native OneClickDescriptor deserializeBinary(Uint8Array bytes);

  public static native OneClickDescriptor deserializeBinaryFromReader(
      OneClickDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(OneClickDescriptor message, Object writer);

  public static native OneClickDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, OneClickDescriptor msg);

  public native String addColumnTypes(String value, double index);

  public native String addColumnTypes(String value);

  public native String addColumns(String value, double index);

  public native String addColumns(String value);

  public native void clearColumnTypesList();

  public native void clearColumnsList();

  public native JsArray<String> getColumnTypesList();

  public native JsArray<String> getColumnsList();

  public native boolean getRequireAllFiltersToDisplay();

  public native Uint8Array serializeBinary();

  public native void setColumnTypesList(JsArray<String> value);

  @JsOverlay
  public final void setColumnTypesList(String[] value) {
    setColumnTypesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setColumnsList(JsArray<String> value);

  @JsOverlay
  public final void setColumnsList(String[] value) {
    setColumnsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setRequireAllFiltersToDisplay(boolean value);

  public native OneClickDescriptor.ToObjectReturnType0 toObject();

  public native OneClickDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
