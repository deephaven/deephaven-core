package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.filterdescription.OperationMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.FilterDescription",
    namespace = JsPackage.GLOBAL)
public class FilterDescription {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static FilterDescription.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Object> getChildrenList();

    @JsProperty
    double getDoublevalue();

    @JsProperty
    double getFilterop();

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
    void setChildrenList(JsArray<Object> childrenList);

    @JsOverlay
    default void setChildrenList(Object[] childrenList) {
      setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
    }

    @JsProperty
    void setDoublevalue(double doublevalue);

    @JsProperty
    void setFilterop(double filterop);

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
    static FilterDescription.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Object> getChildrenList();

    @JsProperty
    double getDoublevalue();

    @JsProperty
    double getFilterop();

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
    void setChildrenList(JsArray<Object> childrenList);

    @JsOverlay
    default void setChildrenList(Object[] childrenList) {
      setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
    }

    @JsProperty
    void setDoublevalue(double doublevalue);

    @JsProperty
    void setFilterop(double filterop);

    @JsProperty
    void setLongvalue(double longvalue);

    @JsProperty
    void setNanotimevalue(double nanotimevalue);

    @JsProperty
    void setStringvalue(String stringvalue);
  }

  public static OperationMap Operation;

  public static native FilterDescription deserializeBinary(Uint8Array bytes);

  public static native FilterDescription deserializeBinaryFromReader(
      FilterDescription message, Object reader);

  public static native void serializeBinaryToWriter(FilterDescription message, Object writer);

  public static native FilterDescription.ToObjectReturnType toObject(
      boolean includeInstance, FilterDescription msg);

  public native FilterDescription addChildren();

  public native FilterDescription addChildren(FilterDescription value, double index);

  public native FilterDescription addChildren(FilterDescription value);

  public native void clearBoolvalue();

  public native void clearChildrenList();

  public native void clearDoublevalue();

  public native void clearLongvalue();

  public native void clearNanotimevalue();

  public native void clearStringvalue();

  public native boolean getBoolvalue();

  public native JsArray<FilterDescription> getChildrenList();

  public native double getDoublevalue();

  public native double getFilterop();

  public native double getLongvalue();

  public native double getNanotimevalue();

  public native String getStringvalue();

  public native int getValueCase();

  public native boolean hasBoolvalue();

  public native boolean hasDoublevalue();

  public native boolean hasLongvalue();

  public native boolean hasNanotimevalue();

  public native boolean hasStringvalue();

  public native Uint8Array serializeBinary();

  public native void setBoolvalue(boolean value);

  @JsOverlay
  public final void setChildrenList(FilterDescription[] value) {
    setChildrenList(Js.<JsArray<FilterDescription>>uncheckedCast(value));
  }

  public native void setChildrenList(JsArray<FilterDescription> value);

  public native void setDoublevalue(double value);

  public native void setFilterop(double value);

  public native void setLongvalue(double value);

  public native void setNanotimevalue(double value);

  public native void setStringvalue(String value);

  public native FilterDescription.ToObjectReturnType0 toObject();

  public native FilterDescription.ToObjectReturnType0 toObject(boolean includeInstance);
}
