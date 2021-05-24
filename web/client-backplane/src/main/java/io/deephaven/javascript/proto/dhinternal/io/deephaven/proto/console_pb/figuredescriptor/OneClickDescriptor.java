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
    JsArray<String> getColumnsList();

    @JsProperty
    JsArray<String> getColumntypesList();

    @JsProperty
    boolean isRequireallfilterstodisplay();

    @JsProperty
    void setColumnsList(JsArray<String> columnsList);

    @JsOverlay
    default void setColumnsList(String[] columnsList) {
      setColumnsList(Js.<JsArray<String>>uncheckedCast(columnsList));
    }

    @JsProperty
    void setColumntypesList(JsArray<String> columntypesList);

    @JsOverlay
    default void setColumntypesList(String[] columntypesList) {
      setColumntypesList(Js.<JsArray<String>>uncheckedCast(columntypesList));
    }

    @JsProperty
    void setRequireallfilterstodisplay(boolean requireallfilterstodisplay);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static OneClickDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getColumnsList();

    @JsProperty
    JsArray<String> getColumntypesList();

    @JsProperty
    boolean isRequireallfilterstodisplay();

    @JsProperty
    void setColumnsList(JsArray<String> columnsList);

    @JsOverlay
    default void setColumnsList(String[] columnsList) {
      setColumnsList(Js.<JsArray<String>>uncheckedCast(columnsList));
    }

    @JsProperty
    void setColumntypesList(JsArray<String> columntypesList);

    @JsOverlay
    default void setColumntypesList(String[] columntypesList) {
      setColumntypesList(Js.<JsArray<String>>uncheckedCast(columntypesList));
    }

    @JsProperty
    void setRequireallfilterstodisplay(boolean requireallfilterstodisplay);
  }

  public static native OneClickDescriptor deserializeBinary(Uint8Array bytes);

  public static native OneClickDescriptor deserializeBinaryFromReader(
      OneClickDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(OneClickDescriptor message, Object writer);

  public static native OneClickDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, OneClickDescriptor msg);

  public native String addColumns(String value, double index);

  public native String addColumns(String value);

  public native String addColumntypes(String value, double index);

  public native String addColumntypes(String value);

  public native void clearColumnsList();

  public native void clearColumntypesList();

  public native JsArray<String> getColumnsList();

  public native JsArray<String> getColumntypesList();

  public native boolean getRequireallfilterstodisplay();

  public native Uint8Array serializeBinary();

  public native void setColumnsList(JsArray<String> value);

  @JsOverlay
  public final void setColumnsList(String[] value) {
    setColumnsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setColumntypesList(JsArray<String> value);

  @JsOverlay
  public final void setColumntypesList(String[] value) {
    setColumntypesList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setRequireallfilterstodisplay(boolean value);

  public native OneClickDescriptor.ToObjectReturnType0 toObject();

  public native OneClickDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
