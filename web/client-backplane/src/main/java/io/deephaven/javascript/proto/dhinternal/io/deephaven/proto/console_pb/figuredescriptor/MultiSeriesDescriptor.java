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
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.MultiSeriesDescriptor",
    namespace = JsPackage.GLOBAL)
public class MultiSeriesDescriptor {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DatasourcesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface AxisFieldType {
        @JsOverlay
        static MultiSeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.AxisFieldType
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

      @JsOverlay
      static MultiSeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      MultiSeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.AxisFieldType getAxis();

      @JsProperty
      String getColumnname();

      @JsProperty
      double getTablemapid();

      @JsProperty
      double getType();

      @JsProperty
      void setAxis(
          MultiSeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType.AxisFieldType axis);

      @JsProperty
      void setColumnname(String columnname);

      @JsProperty
      void setTablemapid(double tablemapid);

      @JsProperty
      void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LinecolorFieldType {
      @JsOverlay
      static MultiSeriesDescriptor.ToObjectReturnType.LinecolorFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getDefaultstring();

      @JsProperty
      JsArray<String> getKeysList();

      @JsProperty
      JsArray<String> getValuesList();

      @JsProperty
      void setDefaultstring(String defaultstring);

      @JsProperty
      void setKeysList(JsArray<String> keysList);

      @JsOverlay
      default void setKeysList(String[] keysList) {
        setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
      }

      @JsProperty
      void setValuesList(JsArray<String> valuesList);

      @JsOverlay
      default void setValuesList(String[] valuesList) {
        setValuesList(Js.<JsArray<String>>uncheckedCast(valuesList));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LinesvisibleFieldType {
      @JsOverlay
      static MultiSeriesDescriptor.ToObjectReturnType.LinesvisibleFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getKeysList();

      @JsProperty
      JsArray<Boolean> getValuesList();

      @JsProperty
      boolean isDefaultbool();

      @JsProperty
      void setDefaultbool(boolean defaultbool);

      @JsProperty
      void setKeysList(JsArray<String> keysList);

      @JsOverlay
      default void setKeysList(String[] keysList) {
        setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
      }

      @JsProperty
      void setValuesList(JsArray<Boolean> valuesList);

      @JsOverlay
      default void setValuesList(boolean[] valuesList) {
        setValuesList(Js.<JsArray<Boolean>>uncheckedCast(valuesList));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface PointsizeFieldType {
      @JsOverlay
      static MultiSeriesDescriptor.ToObjectReturnType.PointsizeFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getDefaultdouble();

      @JsProperty
      JsArray<String> getKeysList();

      @JsProperty
      JsArray<Double> getValuesList();

      @JsProperty
      void setDefaultdouble(double defaultdouble);

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

    @JsOverlay
    static MultiSeriesDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<MultiSeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType> getDatasourcesList();

    @JsProperty
    Object getGradientvisible();

    @JsProperty
    MultiSeriesDescriptor.ToObjectReturnType.LinecolorFieldType getLinecolor();

    @JsProperty
    MultiSeriesDescriptor.ToObjectReturnType.LinesvisibleFieldType getLinesvisible();

    @JsProperty
    String getName();

    @JsProperty
    double getPlotstyle();

    @JsProperty
    Object getPointcolor();

    @JsProperty
    Object getPointlabel();

    @JsProperty
    Object getPointlabelformat();

    @JsProperty
    Object getPointshape();

    @JsProperty
    MultiSeriesDescriptor.ToObjectReturnType.PointsizeFieldType getPointsize();

    @JsProperty
    Object getPointsvisible();

    @JsProperty
    Object getXtooltippattern();

    @JsProperty
    Object getYtooltippattern();

    @JsOverlay
    default void setDatasourcesList(
        MultiSeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType[] datasourcesList) {
      setDatasourcesList(
          Js
              .<JsArray<MultiSeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType>>
                  uncheckedCast(datasourcesList));
    }

    @JsProperty
    void setDatasourcesList(
        JsArray<MultiSeriesDescriptor.ToObjectReturnType.DatasourcesListFieldType> datasourcesList);

    @JsProperty
    void setGradientvisible(Object gradientvisible);

    @JsProperty
    void setLinecolor(MultiSeriesDescriptor.ToObjectReturnType.LinecolorFieldType linecolor);

    @JsProperty
    void setLinesvisible(
        MultiSeriesDescriptor.ToObjectReturnType.LinesvisibleFieldType linesvisible);

    @JsProperty
    void setName(String name);

    @JsProperty
    void setPlotstyle(double plotstyle);

    @JsProperty
    void setPointcolor(Object pointcolor);

    @JsProperty
    void setPointlabel(Object pointlabel);

    @JsProperty
    void setPointlabelformat(Object pointlabelformat);

    @JsProperty
    void setPointshape(Object pointshape);

    @JsProperty
    void setPointsize(MultiSeriesDescriptor.ToObjectReturnType.PointsizeFieldType pointsize);

    @JsProperty
    void setPointsvisible(Object pointsvisible);

    @JsProperty
    void setXtooltippattern(Object xtooltippattern);

    @JsProperty
    void setYtooltippattern(Object ytooltippattern);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DatasourcesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface AxisFieldType {
        @JsOverlay
        static MultiSeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.AxisFieldType
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

      @JsOverlay
      static MultiSeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      MultiSeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.AxisFieldType getAxis();

      @JsProperty
      String getColumnname();

      @JsProperty
      double getTablemapid();

      @JsProperty
      double getType();

      @JsProperty
      void setAxis(
          MultiSeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType.AxisFieldType axis);

      @JsProperty
      void setColumnname(String columnname);

      @JsProperty
      void setTablemapid(double tablemapid);

      @JsProperty
      void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LinecolorFieldType {
      @JsOverlay
      static MultiSeriesDescriptor.ToObjectReturnType0.LinecolorFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getDefaultstring();

      @JsProperty
      JsArray<String> getKeysList();

      @JsProperty
      JsArray<String> getValuesList();

      @JsProperty
      void setDefaultstring(String defaultstring);

      @JsProperty
      void setKeysList(JsArray<String> keysList);

      @JsOverlay
      default void setKeysList(String[] keysList) {
        setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
      }

      @JsProperty
      void setValuesList(JsArray<String> valuesList);

      @JsOverlay
      default void setValuesList(String[] valuesList) {
        setValuesList(Js.<JsArray<String>>uncheckedCast(valuesList));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LinesvisibleFieldType {
      @JsOverlay
      static MultiSeriesDescriptor.ToObjectReturnType0.LinesvisibleFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<String> getKeysList();

      @JsProperty
      JsArray<Boolean> getValuesList();

      @JsProperty
      boolean isDefaultbool();

      @JsProperty
      void setDefaultbool(boolean defaultbool);

      @JsProperty
      void setKeysList(JsArray<String> keysList);

      @JsOverlay
      default void setKeysList(String[] keysList) {
        setKeysList(Js.<JsArray<String>>uncheckedCast(keysList));
      }

      @JsProperty
      void setValuesList(JsArray<Boolean> valuesList);

      @JsOverlay
      default void setValuesList(boolean[] valuesList) {
        setValuesList(Js.<JsArray<Boolean>>uncheckedCast(valuesList));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface PointsizeFieldType {
      @JsOverlay
      static MultiSeriesDescriptor.ToObjectReturnType0.PointsizeFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getDefaultdouble();

      @JsProperty
      JsArray<String> getKeysList();

      @JsProperty
      JsArray<Double> getValuesList();

      @JsProperty
      void setDefaultdouble(double defaultdouble);

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

    @JsOverlay
    static MultiSeriesDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<MultiSeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType>
        getDatasourcesList();

    @JsProperty
    Object getGradientvisible();

    @JsProperty
    MultiSeriesDescriptor.ToObjectReturnType0.LinecolorFieldType getLinecolor();

    @JsProperty
    MultiSeriesDescriptor.ToObjectReturnType0.LinesvisibleFieldType getLinesvisible();

    @JsProperty
    String getName();

    @JsProperty
    double getPlotstyle();

    @JsProperty
    Object getPointcolor();

    @JsProperty
    Object getPointlabel();

    @JsProperty
    Object getPointlabelformat();

    @JsProperty
    Object getPointshape();

    @JsProperty
    MultiSeriesDescriptor.ToObjectReturnType0.PointsizeFieldType getPointsize();

    @JsProperty
    Object getPointsvisible();

    @JsProperty
    Object getXtooltippattern();

    @JsProperty
    Object getYtooltippattern();

    @JsOverlay
    default void setDatasourcesList(
        MultiSeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType[] datasourcesList) {
      setDatasourcesList(
          Js
              .<JsArray<MultiSeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType>>
                  uncheckedCast(datasourcesList));
    }

    @JsProperty
    void setDatasourcesList(
        JsArray<MultiSeriesDescriptor.ToObjectReturnType0.DatasourcesListFieldType>
            datasourcesList);

    @JsProperty
    void setGradientvisible(Object gradientvisible);

    @JsProperty
    void setLinecolor(MultiSeriesDescriptor.ToObjectReturnType0.LinecolorFieldType linecolor);

    @JsProperty
    void setLinesvisible(
        MultiSeriesDescriptor.ToObjectReturnType0.LinesvisibleFieldType linesvisible);

    @JsProperty
    void setName(String name);

    @JsProperty
    void setPlotstyle(double plotstyle);

    @JsProperty
    void setPointcolor(Object pointcolor);

    @JsProperty
    void setPointlabel(Object pointlabel);

    @JsProperty
    void setPointlabelformat(Object pointlabelformat);

    @JsProperty
    void setPointshape(Object pointshape);

    @JsProperty
    void setPointsize(MultiSeriesDescriptor.ToObjectReturnType0.PointsizeFieldType pointsize);

    @JsProperty
    void setPointsvisible(Object pointsvisible);

    @JsProperty
    void setXtooltippattern(Object xtooltippattern);

    @JsProperty
    void setYtooltippattern(Object ytooltippattern);
  }

  public static native MultiSeriesDescriptor deserializeBinary(Uint8Array bytes);

  public static native MultiSeriesDescriptor deserializeBinaryFromReader(
      MultiSeriesDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(MultiSeriesDescriptor message, Object writer);

  public static native MultiSeriesDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, MultiSeriesDescriptor msg);

  public native MultiSeriesSourceDescriptor addDatasources();

  public native MultiSeriesSourceDescriptor addDatasources(
      MultiSeriesSourceDescriptor value, double index);

  public native MultiSeriesSourceDescriptor addDatasources(MultiSeriesSourceDescriptor value);

  public native void clearDatasourcesList();

  public native void clearGradientvisible();

  public native void clearLinecolor();

  public native void clearLinesvisible();

  public native void clearPointcolor();

  public native void clearPointlabel();

  public native void clearPointlabelformat();

  public native void clearPointshape();

  public native void clearPointsize();

  public native void clearPointsvisible();

  public native void clearXtooltippattern();

  public native void clearYtooltippattern();

  public native JsArray<MultiSeriesSourceDescriptor> getDatasourcesList();

  public native BoolMapWithDefault getGradientvisible();

  public native StringMapWithDefault getLinecolor();

  public native BoolMapWithDefault getLinesvisible();

  public native String getName();

  public native double getPlotstyle();

  public native StringMapWithDefault getPointcolor();

  public native StringMapWithDefault getPointlabel();

  public native StringMapWithDefault getPointlabelformat();

  public native StringMapWithDefault getPointshape();

  public native DoubleMapWithDefault getPointsize();

  public native BoolMapWithDefault getPointsvisible();

  public native StringMapWithDefault getXtooltippattern();

  public native StringMapWithDefault getYtooltippattern();

  public native boolean hasGradientvisible();

  public native boolean hasLinecolor();

  public native boolean hasLinesvisible();

  public native boolean hasPointcolor();

  public native boolean hasPointlabel();

  public native boolean hasPointlabelformat();

  public native boolean hasPointshape();

  public native boolean hasPointsize();

  public native boolean hasPointsvisible();

  public native boolean hasXtooltippattern();

  public native boolean hasYtooltippattern();

  public native Uint8Array serializeBinary();

  public native void setDatasourcesList(JsArray<MultiSeriesSourceDescriptor> value);

  @JsOverlay
  public final void setDatasourcesList(MultiSeriesSourceDescriptor[] value) {
    setDatasourcesList(Js.<JsArray<MultiSeriesSourceDescriptor>>uncheckedCast(value));
  }

  public native void setGradientvisible();

  public native void setGradientvisible(BoolMapWithDefault value);

  public native void setLinecolor();

  public native void setLinecolor(StringMapWithDefault value);

  public native void setLinesvisible();

  public native void setLinesvisible(BoolMapWithDefault value);

  public native void setName(String value);

  public native void setPlotstyle(double value);

  public native void setPointcolor();

  public native void setPointcolor(StringMapWithDefault value);

  public native void setPointlabel();

  public native void setPointlabel(StringMapWithDefault value);

  public native void setPointlabelformat();

  public native void setPointlabelformat(StringMapWithDefault value);

  public native void setPointshape();

  public native void setPointshape(StringMapWithDefault value);

  public native void setPointsize();

  public native void setPointsize(DoubleMapWithDefault value);

  public native void setPointsvisible();

  public native void setPointsvisible(BoolMapWithDefault value);

  public native void setXtooltippattern();

  public native void setXtooltippattern(StringMapWithDefault value);

  public native void setYtooltippattern();

  public native void setYtooltippattern(StringMapWithDefault value);

  public native MultiSeriesDescriptor.ToObjectReturnType0 toObject();

  public native MultiSeriesDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
