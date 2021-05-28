package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.chartdescriptor.ChartTypeMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.ChartDescriptor",
    namespace = JsPackage.GLOBAL)
public class ChartDescriptor {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface MultiseriesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface DatasourcesListFieldType {
        @JsOverlay
        static ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.DatasourcesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getAxis();

        @JsProperty
        String getColumnname();

        @JsProperty
        double getTablemapid();

        @JsProperty
        double getType();

        @JsProperty
        void setAxis(Object axis);

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
        static ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.LinecolorFieldType
            create() {
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
        static ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.LinesvisibleFieldType
            create() {
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
        static ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.PointsizeFieldType
            create() {
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
      static ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.DatasourcesListFieldType>
          getDatasourcesList();

      @JsProperty
      Object getGradientvisible();

      @JsProperty
      ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.LinecolorFieldType getLinecolor();

      @JsProperty
      ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.LinesvisibleFieldType
          getLinesvisible();

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
      ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.PointsizeFieldType getPointsize();

      @JsProperty
      Object getPointsvisible();

      @JsProperty
      Object getXtooltippattern();

      @JsProperty
      Object getYtooltippattern();

      @JsOverlay
      default void setDatasourcesList(
          ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.DatasourcesListFieldType[]
              datasourcesList) {
        setDatasourcesList(
            Js
                .<JsArray<
                        ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType
                            .DatasourcesListFieldType>>
                    uncheckedCast(datasourcesList));
      }

      @JsProperty
      void setDatasourcesList(
          JsArray<
                  ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType
                      .DatasourcesListFieldType>
              datasourcesList);

      @JsProperty
      void setGradientvisible(Object gradientvisible);

      @JsProperty
      void setLinecolor(
          ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.LinecolorFieldType linecolor);

      @JsProperty
      void setLinesvisible(
          ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.LinesvisibleFieldType
              linesvisible);

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
      void setPointsize(
          ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType.PointsizeFieldType pointsize);

      @JsProperty
      void setPointsvisible(Object pointsvisible);

      @JsProperty
      void setXtooltippattern(Object xtooltippattern);

      @JsProperty
      void setYtooltippattern(Object ytooltippattern);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SeriesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface DatasourcesListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AxisFieldType {
          @JsOverlay
          static ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType
                  .AxisFieldType
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
          static ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType
                  .OneclickFieldType
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
        static ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType
                .AxisFieldType
            getAxis();

        @JsProperty
        String getColumnname();

        @JsProperty
        String getColumntype();

        @JsProperty
        ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType
                .OneclickFieldType
            getOneclick();

        @JsProperty
        double getTableid();

        @JsProperty
        double getTablemapid();

        @JsProperty
        double getType();

        @JsProperty
        void setAxis(
            ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType
                    .AxisFieldType
                axis);

        @JsProperty
        void setColumnname(String columnname);

        @JsProperty
        void setColumntype(String columntype);

        @JsProperty
        void setOneclick(
            ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType
                    .OneclickFieldType
                oneclick);

        @JsProperty
        void setTableid(double tableid);

        @JsProperty
        void setTablemapid(double tablemapid);

        @JsProperty
        void setType(double type);
      }

      @JsOverlay
      static ChartDescriptor.ToObjectReturnType.SeriesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType>
          getDatasourcesList();

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
          ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType[]
              datasourcesList) {
        setDatasourcesList(
            Js
                .<JsArray<
                        ChartDescriptor.ToObjectReturnType.SeriesListFieldType
                            .DatasourcesListFieldType>>
                    uncheckedCast(datasourcesList));
      }

      @JsProperty
      void setDatasourcesList(
          JsArray<ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DatasourcesListFieldType>
              datasourcesList);

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

    @JsOverlay
    static ChartDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Object> getAxesList();

    @JsProperty
    double getCharttype();

    @JsProperty
    double getColspan();

    @JsProperty
    String getLegendcolor();

    @JsProperty
    String getLegendfont();

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType> getMultiseriesList();

    @JsProperty
    double getRowspan();

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType.SeriesListFieldType> getSeriesList();

    @JsProperty
    String getTitle();

    @JsProperty
    String getTitlecolor();

    @JsProperty
    String getTitlefont();

    @JsProperty
    boolean isIs3d();

    @JsProperty
    boolean isShowlegend();

    @JsProperty
    void setAxesList(JsArray<Object> axesList);

    @JsOverlay
    default void setAxesList(Object[] axesList) {
      setAxesList(Js.<JsArray<Object>>uncheckedCast(axesList));
    }

    @JsProperty
    void setCharttype(double charttype);

    @JsProperty
    void setColspan(double colspan);

    @JsProperty
    void setIs3d(boolean is3d);

    @JsProperty
    void setLegendcolor(String legendcolor);

    @JsProperty
    void setLegendfont(String legendfont);

    @JsProperty
    void setMultiseriesList(
        JsArray<ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType> multiseriesList);

    @JsOverlay
    default void setMultiseriesList(
        ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType[] multiseriesList) {
      setMultiseriesList(
          Js.<JsArray<ChartDescriptor.ToObjectReturnType.MultiseriesListFieldType>>uncheckedCast(
              multiseriesList));
    }

    @JsProperty
    void setRowspan(double rowspan);

    @JsProperty
    void setSeriesList(JsArray<ChartDescriptor.ToObjectReturnType.SeriesListFieldType> seriesList);

    @JsOverlay
    default void setSeriesList(
        ChartDescriptor.ToObjectReturnType.SeriesListFieldType[] seriesList) {
      setSeriesList(
          Js.<JsArray<ChartDescriptor.ToObjectReturnType.SeriesListFieldType>>uncheckedCast(
              seriesList));
    }

    @JsProperty
    void setShowlegend(boolean showlegend);

    @JsProperty
    void setTitle(String title);

    @JsProperty
    void setTitlecolor(String titlecolor);

    @JsProperty
    void setTitlefont(String titlefont);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface MultiseriesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface DatasourcesListFieldType {
        @JsOverlay
        static ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.DatasourcesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getAxis();

        @JsProperty
        String getColumnname();

        @JsProperty
        double getTablemapid();

        @JsProperty
        double getType();

        @JsProperty
        void setAxis(Object axis);

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
        static ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.LinecolorFieldType
            create() {
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
        static ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.LinesvisibleFieldType
            create() {
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
        static ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.PointsizeFieldType
            create() {
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
      static ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.DatasourcesListFieldType>
          getDatasourcesList();

      @JsProperty
      Object getGradientvisible();

      @JsProperty
      ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.LinecolorFieldType
          getLinecolor();

      @JsProperty
      ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.LinesvisibleFieldType
          getLinesvisible();

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
      ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.PointsizeFieldType
          getPointsize();

      @JsProperty
      Object getPointsvisible();

      @JsProperty
      Object getXtooltippattern();

      @JsProperty
      Object getYtooltippattern();

      @JsOverlay
      default void setDatasourcesList(
          ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.DatasourcesListFieldType[]
              datasourcesList) {
        setDatasourcesList(
            Js
                .<JsArray<
                        ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType
                            .DatasourcesListFieldType>>
                    uncheckedCast(datasourcesList));
      }

      @JsProperty
      void setDatasourcesList(
          JsArray<
                  ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType
                      .DatasourcesListFieldType>
              datasourcesList);

      @JsProperty
      void setGradientvisible(Object gradientvisible);

      @JsProperty
      void setLinecolor(
          ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.LinecolorFieldType
              linecolor);

      @JsProperty
      void setLinesvisible(
          ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.LinesvisibleFieldType
              linesvisible);

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
      void setPointsize(
          ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType.PointsizeFieldType
              pointsize);

      @JsProperty
      void setPointsvisible(Object pointsvisible);

      @JsProperty
      void setXtooltippattern(Object xtooltippattern);

      @JsProperty
      void setYtooltippattern(Object ytooltippattern);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SeriesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface DatasourcesListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AxisFieldType {
          @JsOverlay
          static ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType
                  .AxisFieldType
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
          static ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType
                  .OneclickFieldType
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
        static ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType
                .AxisFieldType
            getAxis();

        @JsProperty
        String getColumnname();

        @JsProperty
        String getColumntype();

        @JsProperty
        ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType
                .OneclickFieldType
            getOneclick();

        @JsProperty
        double getTableid();

        @JsProperty
        double getTablemapid();

        @JsProperty
        double getType();

        @JsProperty
        void setAxis(
            ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType
                    .AxisFieldType
                axis);

        @JsProperty
        void setColumnname(String columnname);

        @JsProperty
        void setColumntype(String columntype);

        @JsProperty
        void setOneclick(
            ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType
                    .OneclickFieldType
                oneclick);

        @JsProperty
        void setTableid(double tableid);

        @JsProperty
        void setTablemapid(double tablemapid);

        @JsProperty
        void setType(double type);
      }

      @JsOverlay
      static ChartDescriptor.ToObjectReturnType0.SeriesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType>
          getDatasourcesList();

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
          ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType[]
              datasourcesList) {
        setDatasourcesList(
            Js
                .<JsArray<
                        ChartDescriptor.ToObjectReturnType0.SeriesListFieldType
                            .DatasourcesListFieldType>>
                    uncheckedCast(datasourcesList));
      }

      @JsProperty
      void setDatasourcesList(
          JsArray<ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DatasourcesListFieldType>
              datasourcesList);

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

    @JsOverlay
    static ChartDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Object> getAxesList();

    @JsProperty
    double getCharttype();

    @JsProperty
    double getColspan();

    @JsProperty
    String getLegendcolor();

    @JsProperty
    String getLegendfont();

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType> getMultiseriesList();

    @JsProperty
    double getRowspan();

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType0.SeriesListFieldType> getSeriesList();

    @JsProperty
    String getTitle();

    @JsProperty
    String getTitlecolor();

    @JsProperty
    String getTitlefont();

    @JsProperty
    boolean isIs3d();

    @JsProperty
    boolean isShowlegend();

    @JsProperty
    void setAxesList(JsArray<Object> axesList);

    @JsOverlay
    default void setAxesList(Object[] axesList) {
      setAxesList(Js.<JsArray<Object>>uncheckedCast(axesList));
    }

    @JsProperty
    void setCharttype(double charttype);

    @JsProperty
    void setColspan(double colspan);

    @JsProperty
    void setIs3d(boolean is3d);

    @JsProperty
    void setLegendcolor(String legendcolor);

    @JsProperty
    void setLegendfont(String legendfont);

    @JsProperty
    void setMultiseriesList(
        JsArray<ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType> multiseriesList);

    @JsOverlay
    default void setMultiseriesList(
        ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType[] multiseriesList) {
      setMultiseriesList(
          Js.<JsArray<ChartDescriptor.ToObjectReturnType0.MultiseriesListFieldType>>uncheckedCast(
              multiseriesList));
    }

    @JsProperty
    void setRowspan(double rowspan);

    @JsProperty
    void setSeriesList(JsArray<ChartDescriptor.ToObjectReturnType0.SeriesListFieldType> seriesList);

    @JsOverlay
    default void setSeriesList(
        ChartDescriptor.ToObjectReturnType0.SeriesListFieldType[] seriesList) {
      setSeriesList(
          Js.<JsArray<ChartDescriptor.ToObjectReturnType0.SeriesListFieldType>>uncheckedCast(
              seriesList));
    }

    @JsProperty
    void setShowlegend(boolean showlegend);

    @JsProperty
    void setTitle(String title);

    @JsProperty
    void setTitlecolor(String titlecolor);

    @JsProperty
    void setTitlefont(String titlefont);
  }

  public static ChartTypeMap ChartType;

  public static native ChartDescriptor deserializeBinary(Uint8Array bytes);

  public static native ChartDescriptor deserializeBinaryFromReader(
      ChartDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(ChartDescriptor message, Object writer);

  public static native ChartDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, ChartDescriptor msg);

  public native AxisDescriptor addAxes();

  public native AxisDescriptor addAxes(AxisDescriptor value, double index);

  public native AxisDescriptor addAxes(AxisDescriptor value);

  public native MultiSeriesDescriptor addMultiseries();

  public native MultiSeriesDescriptor addMultiseries(MultiSeriesDescriptor value, double index);

  public native MultiSeriesDescriptor addMultiseries(MultiSeriesDescriptor value);

  public native SeriesDescriptor addSeries();

  public native SeriesDescriptor addSeries(SeriesDescriptor value, double index);

  public native SeriesDescriptor addSeries(SeriesDescriptor value);

  public native void clearAxesList();

  public native void clearMultiseriesList();

  public native void clearSeriesList();

  public native JsArray<AxisDescriptor> getAxesList();

  public native double getCharttype();

  public native double getColspan();

  public native boolean getIs3d();

  public native String getLegendcolor();

  public native String getLegendfont();

  public native JsArray<MultiSeriesDescriptor> getMultiseriesList();

  public native double getRowspan();

  public native JsArray<SeriesDescriptor> getSeriesList();

  public native boolean getShowlegend();

  public native String getTitle();

  public native String getTitlecolor();

  public native String getTitlefont();

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setAxesList(AxisDescriptor[] value) {
    setAxesList(Js.<JsArray<AxisDescriptor>>uncheckedCast(value));
  }

  public native void setAxesList(JsArray<AxisDescriptor> value);

  public native void setCharttype(double value);

  public native void setColspan(double value);

  public native void setIs3d(boolean value);

  public native void setLegendcolor(String value);

  public native void setLegendfont(String value);

  public native void setMultiseriesList(JsArray<MultiSeriesDescriptor> value);

  @JsOverlay
  public final void setMultiseriesList(MultiSeriesDescriptor[] value) {
    setMultiseriesList(Js.<JsArray<MultiSeriesDescriptor>>uncheckedCast(value));
  }

  public native void setRowspan(double value);

  public native void setSeriesList(JsArray<SeriesDescriptor> value);

  @JsOverlay
  public final void setSeriesList(SeriesDescriptor[] value) {
    setSeriesList(Js.<JsArray<SeriesDescriptor>>uncheckedCast(value));
  }

  public native void setShowlegend(boolean value);

  public native void setTitle(String value);

  public native void setTitlecolor(String value);

  public native void setTitlefont(String value);

  public native ChartDescriptor.ToObjectReturnType0 toObject();

  public native ChartDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
