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
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.SeriesDescriptor",
    namespace = JsPackage.GLOBAL)
public class SeriesDescriptor {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DatasourcesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface AxisFieldType {
        @JsOverlay
        static SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.AxisFieldType create() {
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
        static SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.OneclickFieldType
            create() {
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
      static SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.AxisFieldType getAxis();

      @JsProperty
      String getColumnname();

      @JsProperty
      String getColumntype();

      @JsProperty
      SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.OneclickFieldType getOneclick();

      @JsProperty
      double getTableid();

      @JsProperty
      double getTablemapid();

      @JsProperty
      double getType();

      @JsProperty
      void setAxis(SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.AxisFieldType axis);

      @JsProperty
      void setColumnname(String columnname);

      @JsProperty
      void setColumntype(String columntype);

      @JsProperty
      void setOneclick(
          SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.OneclickFieldType oneclick);

      @JsProperty
      void setTableid(double tableid);

      @JsProperty
      void setTablemapid(double tablemapid);

      @JsProperty
      void setType(double type);
    }

    @JsOverlay
    static SeriesDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType> getDatasourcesList();

    @JsProperty
    String getLinecolor();

    @JsProperty
    String getName();

    @JsProperty
    double getPlotstyle();

    @JsProperty
    String getPointlabelformat();

    @JsProperty
    String getShape();

    @JsProperty
    String getShapecolor();

    @JsProperty
    String getShapelabel();

    @JsProperty
    double getShapesize();

    @JsProperty
    String getXtooltippattern();

    @JsProperty
    String getYtooltippattern();

    @JsProperty
    boolean isGradientvisible();

    @JsProperty
    boolean isLinesvisible();

    @JsProperty
    boolean isShapesvisible();

    @JsOverlay
    default void setDatasourcesList(
        SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType[] datasourcesList) {
      setDatasourcesList(
          Js.<JsArray<SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType>>uncheckedCast(
              datasourcesList));
    }

    @JsProperty
    void setDatasourcesList(
        JsArray<SeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType> datasourcesList);

    @JsProperty
    void setGradientvisible(boolean gradientvisible);

    @JsProperty
    void setLinecolor(String linecolor);

    @JsProperty
    void setLinesvisible(boolean linesvisible);

    @JsProperty
    void setName(String name);

    @JsProperty
    void setPlotstyle(double plotstyle);

    @JsProperty
    void setPointlabelformat(String pointlabelformat);

    @JsProperty
    void setShape(String shape);

    @JsProperty
    void setShapecolor(String shapecolor);

    @JsProperty
    void setShapelabel(String shapelabel);

    @JsProperty
    void setShapesize(double shapesize);

    @JsProperty
    void setShapesvisible(boolean shapesvisible);

    @JsProperty
    void setXtooltippattern(String xtooltippattern);

    @JsProperty
    void setYtooltippattern(String ytooltippattern);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DatasourcesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface AxisFieldType {
        @JsOverlay
        static SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.AxisFieldType
            create() {
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
        static SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.OneclickFieldType
            create() {
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
      static SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.AxisFieldType getAxis();

      @JsProperty
      String getColumnname();

      @JsProperty
      String getColumntype();

      @JsProperty
      SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.OneclickFieldType getOneclick();

      @JsProperty
      double getTableid();

      @JsProperty
      double getTablemapid();

      @JsProperty
      double getType();

      @JsProperty
      void setAxis(
          SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.AxisFieldType axis);

      @JsProperty
      void setColumnname(String columnname);

      @JsProperty
      void setColumntype(String columntype);

      @JsProperty
      void setOneclick(
          SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.OneclickFieldType oneclick);

      @JsProperty
      void setTableid(double tableid);

      @JsProperty
      void setTablemapid(double tablemapid);

      @JsProperty
      void setType(double type);
    }

    @JsOverlay
    static SeriesDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType> getDatasourcesList();

    @JsProperty
    String getLinecolor();

    @JsProperty
    String getName();

    @JsProperty
    double getPlotstyle();

    @JsProperty
    String getPointlabelformat();

    @JsProperty
    String getShape();

    @JsProperty
    String getShapecolor();

    @JsProperty
    String getShapelabel();

    @JsProperty
    double getShapesize();

    @JsProperty
    String getXtooltippattern();

    @JsProperty
    String getYtooltippattern();

    @JsProperty
    boolean isGradientvisible();

    @JsProperty
    boolean isLinesvisible();

    @JsProperty
    boolean isShapesvisible();

    @JsOverlay
    default void setDatasourcesList(
        SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType[] datasourcesList) {
      setDatasourcesList(
          Js.<JsArray<SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType>>uncheckedCast(
              datasourcesList));
    }

    @JsProperty
    void setDatasourcesList(
        JsArray<SeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType> datasourcesList);

    @JsProperty
    void setGradientvisible(boolean gradientvisible);

    @JsProperty
    void setLinecolor(String linecolor);

    @JsProperty
    void setLinesvisible(boolean linesvisible);

    @JsProperty
    void setName(String name);

    @JsProperty
    void setPlotstyle(double plotstyle);

    @JsProperty
    void setPointlabelformat(String pointlabelformat);

    @JsProperty
    void setShape(String shape);

    @JsProperty
    void setShapecolor(String shapecolor);

    @JsProperty
    void setShapelabel(String shapelabel);

    @JsProperty
    void setShapesize(double shapesize);

    @JsProperty
    void setShapesvisible(boolean shapesvisible);

    @JsProperty
    void setXtooltippattern(String xtooltippattern);

    @JsProperty
    void setYtooltippattern(String ytooltippattern);
  }

  public static native SeriesDescriptor deserializeBinary(Uint8Array bytes);

  public static native SeriesDescriptor deserializeBinaryFromReader(
      SeriesDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(SeriesDescriptor message, Object writer);

  public static native SeriesDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, SeriesDescriptor msg);

  public native SourceDescriptor addDatasources();

  public native SourceDescriptor addDatasources(SourceDescriptor value, double index);

  public native SourceDescriptor addDatasources(SourceDescriptor value);

  public native void clearDatasourcesList();

  public native void clearLinesvisible();

  public native void clearShapesize();

  public native void clearShapesvisible();

  public native JsArray<SourceDescriptor> getDatasourcesList();

  public native boolean getGradientvisible();

  public native String getLinecolor();

  public native boolean getLinesvisible();

  public native String getName();

  public native double getPlotstyle();

  public native String getPointlabelformat();

  public native String getShape();

  public native String getShapecolor();

  public native String getShapelabel();

  public native double getShapesize();

  public native boolean getShapesvisible();

  public native String getXtooltippattern();

  public native String getYtooltippattern();

  public native boolean hasLinesvisible();

  public native boolean hasShapesize();

  public native boolean hasShapesvisible();

  public native Uint8Array serializeBinary();

  public native void setDatasourcesList(JsArray<SourceDescriptor> value);

  @JsOverlay
  public final void setDatasourcesList(SourceDescriptor[] value) {
    setDatasourcesList(Js.<JsArray<SourceDescriptor>>uncheckedCast(value));
  }

  public native void setGradientvisible(boolean value);

  public native void setLinecolor(String value);

  public native void setLinesvisible(boolean value);

  public native void setName(String value);

  public native void setPlotstyle(double value);

  public native void setPointlabelformat(String value);

  public native void setShape(String value);

  public native void setShapecolor(String value);

  public native void setShapelabel(String value);

  public native void setShapesize(double value);

  public native void setShapesvisible(boolean value);

  public native void setXtooltippattern(String value);

  public native void setYtooltippattern(String value);

  public native SeriesDescriptor.ToObjectReturnType0 toObject();

  public native SeriesDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
