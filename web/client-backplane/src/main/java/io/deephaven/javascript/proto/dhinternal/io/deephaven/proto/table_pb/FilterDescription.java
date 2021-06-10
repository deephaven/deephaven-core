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
    double getDoubleValue();

    @JsProperty
    double getFilterOp();

    @JsProperty
    double getLongValue();

    @JsProperty
    double getNanoTimeValue();

    @JsProperty
    String getStringValue();

    @JsProperty
    boolean isBoolValue();

    @JsProperty
    void setBoolValue(boolean boolValue);

    @JsProperty
    void setChildrenList(JsArray<Object> childrenList);

    @JsOverlay
    default void setChildrenList(Object[] childrenList) {
      setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
    }

    @JsProperty
    void setDoubleValue(double doubleValue);

    @JsProperty
    void setFilterOp(double filterOp);

    @JsProperty
    void setLongValue(double longValue);

    @JsProperty
    void setNanoTimeValue(double nanoTimeValue);

    @JsProperty
    void setStringValue(String stringValue);
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
    double getDoubleValue();

    @JsProperty
    double getFilterOp();

    @JsProperty
    double getLongValue();

    @JsProperty
    double getNanoTimeValue();

    @JsProperty
    String getStringValue();

    @JsProperty
    boolean isBoolValue();

    @JsProperty
    void setBoolValue(boolean boolValue);

    @JsProperty
    void setChildrenList(JsArray<Object> childrenList);

    @JsOverlay
    default void setChildrenList(Object[] childrenList) {
      setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
    }

    @JsProperty
    void setDoubleValue(double doubleValue);

    @JsProperty
    void setFilterOp(double filterOp);

    @JsProperty
    void setLongValue(double longValue);

    @JsProperty
    void setNanoTimeValue(double nanoTimeValue);

    @JsProperty
    void setStringValue(String stringValue);
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

  public native void clearBoolValue();

  public native void clearChildrenList();

  public native void clearDoubleValue();

  public native void clearLongValue();

  public native void clearNanoTimeValue();

  public native void clearStringValue();

  public native boolean getBoolValue();

  public native JsArray<FilterDescription> getChildrenList();

  public native double getDoubleValue();

  public native double getFilterOp();

  public native double getLongValue();

  public native double getNanoTimeValue();

  public native String getStringValue();

  public native int getValueCase();

  public native boolean hasBoolValue();

  public native boolean hasDoubleValue();

  public native boolean hasLongValue();

  public native boolean hasNanoTimeValue();

  public native boolean hasStringValue();

  public native Uint8Array serializeBinary();

  public native void setBoolValue(boolean value);

  @JsOverlay
  public final void setChildrenList(FilterDescription[] value) {
    setChildrenList(Js.<JsArray<FilterDescription>>uncheckedCast(value));
  }

  public native void setChildrenList(JsArray<FilterDescription> value);

  public native void setDoubleValue(double value);

  public native void setFilterOp(double value);

  public native void setLongValue(double value);

  public native void setNanoTimeValue(double value);

  public native void setStringValue(String value);

  public native FilterDescription.ToObjectReturnType0 toObject();

  public native FilterDescription.ToObjectReturnType0 toObject(boolean includeInstance);
}
