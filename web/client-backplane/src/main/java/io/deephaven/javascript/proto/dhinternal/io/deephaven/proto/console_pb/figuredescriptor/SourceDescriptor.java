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
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.SourceDescriptor",
    namespace = JsPackage.GLOBAL)
public class SourceDescriptor {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AxisFieldType {
      @JsOverlay
      static SourceDescriptor.ToObjectReturnType.AxisFieldType create() {
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
    public interface OneclickFieldType {
      @JsOverlay
      static SourceDescriptor.ToObjectReturnType.OneclickFieldType create() {
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

    @JsOverlay
    static SourceDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SourceDescriptor.ToObjectReturnType.AxisFieldType getAxis();

    @JsProperty
    String getColumnname();

    @JsProperty
    String getColumntype();

    @JsProperty
    SourceDescriptor.ToObjectReturnType.OneclickFieldType getOneclick();

    @JsProperty
    double getTableid();

    @JsProperty
    double getTablemapid();

    @JsProperty
    double getType();

    @JsProperty
    void setAxis(SourceDescriptor.ToObjectReturnType.AxisFieldType axis);

    @JsProperty
    void setColumnname(String columnname);

    @JsProperty
    void setColumntype(String columntype);

    @JsProperty
    void setOneclick(SourceDescriptor.ToObjectReturnType.OneclickFieldType oneclick);

    @JsProperty
    void setTableid(double tableid);

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
      static SourceDescriptor.ToObjectReturnType0.AxisFieldType create() {
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
    public interface OneclickFieldType {
      @JsOverlay
      static SourceDescriptor.ToObjectReturnType0.OneclickFieldType create() {
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

    @JsOverlay
    static SourceDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SourceDescriptor.ToObjectReturnType0.AxisFieldType getAxis();

    @JsProperty
    String getColumnname();

    @JsProperty
    String getColumntype();

    @JsProperty
    SourceDescriptor.ToObjectReturnType0.OneclickFieldType getOneclick();

    @JsProperty
    double getTableid();

    @JsProperty
    double getTablemapid();

    @JsProperty
    double getType();

    @JsProperty
    void setAxis(SourceDescriptor.ToObjectReturnType0.AxisFieldType axis);

    @JsProperty
    void setColumnname(String columnname);

    @JsProperty
    void setColumntype(String columntype);

    @JsProperty
    void setOneclick(SourceDescriptor.ToObjectReturnType0.OneclickFieldType oneclick);

    @JsProperty
    void setTableid(double tableid);

    @JsProperty
    void setTablemapid(double tablemapid);

    @JsProperty
    void setType(double type);
  }

  public static native SourceDescriptor deserializeBinary(Uint8Array bytes);

  public static native SourceDescriptor deserializeBinaryFromReader(
      SourceDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(SourceDescriptor message, Object writer);

  public static native SourceDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, SourceDescriptor msg);

  public native void clearAxis();

  public native void clearOneclick();

  public native AxisDescriptor getAxis();

  public native String getColumnname();

  public native String getColumntype();

  public native OneClickDescriptor getOneclick();

  public native double getTableid();

  public native double getTablemapid();

  public native double getType();

  public native boolean hasAxis();

  public native boolean hasOneclick();

  public native Uint8Array serializeBinary();

  public native void setAxis();

  public native void setAxis(AxisDescriptor value);

  public native void setColumnname(String value);

  public native void setColumntype(String value);

  public native void setOneclick();

  public native void setOneclick(OneClickDescriptor value);

  public native void setTableid(double value);

  public native void setTablemapid(double value);

  public native void setType(double value);

  public native SourceDescriptor.ToObjectReturnType0 toObject();

  public native SourceDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
