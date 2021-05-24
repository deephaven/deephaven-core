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
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.MultiSeriesSourceDescriptor",
    namespace = JsPackage.GLOBAL)
public class MultiSeriesSourceDescriptor {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AxisFieldType {
      @JsOverlay
      static MultiSeriesSourceDescriptor.ToObjectReturnType.AxisFieldType create() {
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

    @JsOverlay
    static MultiSeriesSourceDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    MultiSeriesSourceDescriptor.ToObjectReturnType.AxisFieldType getAxis();

    @JsProperty
    String getColumnname();

    @JsProperty
    double getTablemapid();

    @JsProperty
    double getType();

    @JsProperty
    void setAxis(MultiSeriesSourceDescriptor.ToObjectReturnType.AxisFieldType axis);

    @JsProperty
    void setColumnname(String columnname);

    @JsProperty
    void setTablemapid(double tablemapid);

    @JsProperty
    void setType(double type);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AxisFieldType {
      @JsOverlay
      static MultiSeriesSourceDescriptor.ToObjectReturnType0.AxisFieldType create() {
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

    @JsOverlay
    static MultiSeriesSourceDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    MultiSeriesSourceDescriptor.ToObjectReturnType0.AxisFieldType getAxis();

    @JsProperty
    String getColumnname();

    @JsProperty
    double getTablemapid();

    @JsProperty
    double getType();

    @JsProperty
    void setAxis(MultiSeriesSourceDescriptor.ToObjectReturnType0.AxisFieldType axis);

    @JsProperty
    void setColumnname(String columnname);

    @JsProperty
    void setTablemapid(double tablemapid);

    @JsProperty
    void setType(double type);
  }

  public static native MultiSeriesSourceDescriptor deserializeBinary(Uint8Array bytes);

  public static native MultiSeriesSourceDescriptor deserializeBinaryFromReader(
      MultiSeriesSourceDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(
      MultiSeriesSourceDescriptor message, Object writer);

  public static native MultiSeriesSourceDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, MultiSeriesSourceDescriptor msg);

  public native void clearAxis();

  public native AxisDescriptor getAxis();

  public native String getColumnname();

  public native double getTablemapid();

  public native double getType();

  public native boolean hasAxis();

  public native Uint8Array serializeBinary();

  public native void setAxis();

  public native void setAxis(AxisDescriptor value);

  public native void setColumnname(String value);

  public native void setTablemapid(double value);

  public native void setType(double value);

  public native MultiSeriesSourceDescriptor.ToObjectReturnType0 toObject();

  public native MultiSeriesSourceDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
