package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor.AxisFormatTypeMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor.AxisPositionMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor.AxisTypeMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.AxisDescriptor",
    namespace = JsPackage.GLOBAL)
public class AxisDescriptor {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsOverlay
    static AxisDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Object getBusinesscalendardescriptor();

    @JsProperty
    String getColor();

    @JsProperty
    String getFormatpattern();

    @JsProperty
    double getFormattype();

    @JsProperty
    double getGapbetweenmajorticks();

    @JsProperty
    String getId();

    @JsProperty
    String getLabel();

    @JsProperty
    String getLabelfont();

    @JsProperty
    JsArray<Double> getMajorticklocationsList();

    @JsProperty
    double getMaxrange();

    @JsProperty
    double getMinortickcount();

    @JsProperty
    double getMinrange();

    @JsProperty
    double getPosition();

    @JsProperty
    double getTicklabelangle();

    @JsProperty
    String getTicksfont();

    @JsProperty
    double getType();

    @JsProperty
    boolean isInvert();

    @JsProperty
    boolean isLog();

    @JsProperty
    void setBusinesscalendardescriptor(Object businesscalendardescriptor);

    @JsProperty
    void setColor(String color);

    @JsProperty
    void setFormatpattern(String formatpattern);

    @JsProperty
    void setFormattype(double formattype);

    @JsProperty
    void setGapbetweenmajorticks(double gapbetweenmajorticks);

    @JsProperty
    void setId(String id);

    @JsProperty
    void setInvert(boolean invert);

    @JsProperty
    void setLabel(String label);

    @JsProperty
    void setLabelfont(String labelfont);

    @JsProperty
    void setLog(boolean log);

    @JsProperty
    void setMajorticklocationsList(JsArray<Double> majorticklocationsList);

    @JsOverlay
    default void setMajorticklocationsList(double[] majorticklocationsList) {
      setMajorticklocationsList(Js.<JsArray<Double>>uncheckedCast(majorticklocationsList));
    }

    @JsProperty
    void setMaxrange(double maxrange);

    @JsProperty
    void setMinortickcount(double minortickcount);

    @JsProperty
    void setMinrange(double minrange);

    @JsProperty
    void setPosition(double position);

    @JsProperty
    void setTicklabelangle(double ticklabelangle);

    @JsProperty
    void setTicksfont(String ticksfont);

    @JsProperty
    void setType(double type);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsOverlay
    static AxisDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    Object getBusinesscalendardescriptor();

    @JsProperty
    String getColor();

    @JsProperty
    String getFormatpattern();

    @JsProperty
    double getFormattype();

    @JsProperty
    double getGapbetweenmajorticks();

    @JsProperty
    String getId();

    @JsProperty
    String getLabel();

    @JsProperty
    String getLabelfont();

    @JsProperty
    JsArray<Double> getMajorticklocationsList();

    @JsProperty
    double getMaxrange();

    @JsProperty
    double getMinortickcount();

    @JsProperty
    double getMinrange();

    @JsProperty
    double getPosition();

    @JsProperty
    double getTicklabelangle();

    @JsProperty
    String getTicksfont();

    @JsProperty
    double getType();

    @JsProperty
    boolean isInvert();

    @JsProperty
    boolean isLog();

    @JsProperty
    void setBusinesscalendardescriptor(Object businesscalendardescriptor);

    @JsProperty
    void setColor(String color);

    @JsProperty
    void setFormatpattern(String formatpattern);

    @JsProperty
    void setFormattype(double formattype);

    @JsProperty
    void setGapbetweenmajorticks(double gapbetweenmajorticks);

    @JsProperty
    void setId(String id);

    @JsProperty
    void setInvert(boolean invert);

    @JsProperty
    void setLabel(String label);

    @JsProperty
    void setLabelfont(String labelfont);

    @JsProperty
    void setLog(boolean log);

    @JsProperty
    void setMajorticklocationsList(JsArray<Double> majorticklocationsList);

    @JsOverlay
    default void setMajorticklocationsList(double[] majorticklocationsList) {
      setMajorticklocationsList(Js.<JsArray<Double>>uncheckedCast(majorticklocationsList));
    }

    @JsProperty
    void setMaxrange(double maxrange);

    @JsProperty
    void setMinortickcount(double minortickcount);

    @JsProperty
    void setMinrange(double minrange);

    @JsProperty
    void setPosition(double position);

    @JsProperty
    void setTicklabelangle(double ticklabelangle);

    @JsProperty
    void setTicksfont(String ticksfont);

    @JsProperty
    void setType(double type);
  }

  public static AxisFormatTypeMap AxisFormatType;
  public static AxisPositionMap AxisPosition;
  public static AxisTypeMap AxisType;

  public static native AxisDescriptor deserializeBinary(Uint8Array bytes);

  public static native AxisDescriptor deserializeBinaryFromReader(
      AxisDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(AxisDescriptor message, Object writer);

  public static native AxisDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, AxisDescriptor msg);

  public native double addMajorticklocations(double value, double index);

  public native double addMajorticklocations(double value);

  public native void clearBusinesscalendardescriptor();

  public native void clearMajorticklocationsList();

  public native BusinessCalendarDescriptor getBusinesscalendardescriptor();

  public native String getColor();

  public native String getFormatpattern();

  public native double getFormattype();

  public native double getGapbetweenmajorticks();

  public native String getId();

  public native boolean getInvert();

  public native String getLabel();

  public native String getLabelfont();

  public native boolean getLog();

  public native JsArray<Double> getMajorticklocationsList();

  public native double getMaxrange();

  public native double getMinortickcount();

  public native double getMinrange();

  public native double getPosition();

  public native double getTicklabelangle();

  public native String getTicksfont();

  public native double getType();

  public native boolean hasBusinesscalendardescriptor();

  public native Uint8Array serializeBinary();

  public native void setBusinesscalendardescriptor();

  public native void setBusinesscalendardescriptor(BusinessCalendarDescriptor value);

  public native void setColor(String value);

  public native void setFormatpattern(String value);

  public native void setFormattype(double value);

  public native void setGapbetweenmajorticks(double value);

  public native void setId(String value);

  public native void setInvert(boolean value);

  public native void setLabel(String value);

  public native void setLabelfont(String value);

  public native void setLog(boolean value);

  public native void setMajorticklocationsList(JsArray<Double> value);

  @JsOverlay
  public final void setMajorticklocationsList(double[] value) {
    setMajorticklocationsList(Js.<JsArray<Double>>uncheckedCast(value));
  }

  public native void setMaxrange(double value);

  public native void setMinortickcount(double value);

  public native void setMinrange(double value);

  public native void setPosition(double value);

  public native void setTicklabelangle(double value);

  public native void setTicksfont(String value);

  public native void setType(double value);

  public native AxisDescriptor.ToObjectReturnType0 toObject();

  public native AxisDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
