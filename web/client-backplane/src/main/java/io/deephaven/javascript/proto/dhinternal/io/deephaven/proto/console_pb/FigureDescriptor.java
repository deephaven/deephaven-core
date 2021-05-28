package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.ChartDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.SeriesPlotStyleMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.SourceTypeMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor",
    namespace = JsPackage.GLOBAL)
public class FigureDescriptor {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ChartsListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface MultiseriesListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DatasourcesListFieldType {
          @JsOverlay
          static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                  .DatasourcesListFieldType
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
          static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                  .LinecolorFieldType
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
          static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                  .LinesvisibleFieldType
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
          static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                  .PointsizeFieldType
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
        static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                    .DatasourcesListFieldType>
            getDatasourcesList();

        @JsProperty
        Object getGradientvisible();

        @JsProperty
        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                .LinecolorFieldType
            getLinecolor();

        @JsProperty
        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                .LinesvisibleFieldType
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
        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                .PointsizeFieldType
            getPointsize();

        @JsProperty
        Object getPointsvisible();

        @JsProperty
        Object getXtooltippattern();

        @JsProperty
        Object getYtooltippattern();

        @JsOverlay
        default void setDatasourcesList(
            FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                        .DatasourcesListFieldType
                    []
                datasourcesList) {
          setDatasourcesList(
              Js
                  .<JsArray<
                          FigureDescriptor.ToObjectReturnType.ChartsListFieldType
                              .MultiseriesListFieldType.DatasourcesListFieldType>>
                      uncheckedCast(datasourcesList));
        }

        @JsProperty
        void setDatasourcesList(
            JsArray<
                    FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                        .DatasourcesListFieldType>
                datasourcesList);

        @JsProperty
        void setGradientvisible(Object gradientvisible);

        @JsProperty
        void setLinecolor(
            FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                    .LinecolorFieldType
                linecolor);

        @JsProperty
        void setLinesvisible(
            FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                    .LinesvisibleFieldType
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
            FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType
                    .PointsizeFieldType
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
            static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                    .DatasourcesListFieldType.AxisFieldType
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
            static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                    .DatasourcesListFieldType.OneclickFieldType
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
          static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                  .DatasourcesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                  .DatasourcesListFieldType.AxisFieldType
              getAxis();

          @JsProperty
          String getColumnname();

          @JsProperty
          String getColumntype();

          @JsProperty
          FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                  .DatasourcesListFieldType.OneclickFieldType
              getOneclick();

          @JsProperty
          double getTableid();

          @JsProperty
          double getTablemapid();

          @JsProperty
          double getType();

          @JsProperty
          void setAxis(
              FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                      .DatasourcesListFieldType.AxisFieldType
                  axis);

          @JsProperty
          void setColumnname(String columnname);

          @JsProperty
          void setColumntype(String columntype);

          @JsProperty
          void setOneclick(
              FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                      .DatasourcesListFieldType.OneclickFieldType
                  oneclick);

          @JsProperty
          void setTableid(double tableid);

          @JsProperty
          void setTablemapid(double tablemapid);

          @JsProperty
          void setType(double type);
        }

        @JsOverlay
        static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                    .DatasourcesListFieldType>
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
            FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                        .DatasourcesListFieldType
                    []
                datasourcesList) {
          setDatasourcesList(
              Js
                  .<JsArray<
                          FigureDescriptor.ToObjectReturnType.ChartsListFieldType
                              .SeriesListFieldType.DatasourcesListFieldType>>
                      uncheckedCast(datasourcesList));
        }

        @JsProperty
        void setDatasourcesList(
            JsArray<
                    FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType
                        .DatasourcesListFieldType>
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
      static FigureDescriptor.ToObjectReturnType.ChartsListFieldType create() {
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
      JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType>
          getMultiseriesList();

      @JsProperty
      double getRowspan();

      @JsProperty
      JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType>
          getSeriesList();

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
          JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType>
              multiseriesList);

      @JsOverlay
      default void setMultiseriesList(
          FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiseriesListFieldType[]
              multiseriesList) {
        setMultiseriesList(
            Js
                .<JsArray<
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType
                            .MultiseriesListFieldType>>
                    uncheckedCast(multiseriesList));
      }

      @JsProperty
      void setRowspan(double rowspan);

      @JsProperty
      void setSeriesList(
          JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType>
              seriesList);

      @JsOverlay
      default void setSeriesList(
          FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType[]
              seriesList) {
        setSeriesList(
            Js
                .<JsArray<
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType
                            .SeriesListFieldType>>
                    uncheckedCast(seriesList));
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
    public interface TableidsListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FigureDescriptor.ToObjectReturnType.TableidsListFieldType.GetIdUnionType of(
            Object o) {
          return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
          return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
          return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
          return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
          return (Object) this instanceof Uint8Array;
        }
      }

      @JsOverlay
      static FigureDescriptor.ToObjectReturnType.TableidsListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FigureDescriptor.ToObjectReturnType.TableidsListFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FigureDescriptor.ToObjectReturnType.TableidsListFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FigureDescriptor.ToObjectReturnType.TableidsListFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FigureDescriptor.ToObjectReturnType.TableidsListFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static FigureDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType> getChartsList();

    @JsProperty
    double getCols();

    @JsProperty
    JsArray<String> getErrorsList();

    @JsProperty
    double getRows();

    @JsProperty
    JsArray<FigureDescriptor.ToObjectReturnType.TableidsListFieldType> getTableidsList();

    @JsProperty
    JsArray<Object> getTablemapsList();

    @JsProperty
    String getTitle();

    @JsProperty
    String getTitlecolor();

    @JsProperty
    String getTitlefont();

    @JsProperty
    double getUpdateinterval();

    @JsOverlay
    default void setChartsList(
        FigureDescriptor.ToObjectReturnType.ChartsListFieldType[] chartsList) {
      setChartsList(
          Js.<JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType>>uncheckedCast(
              chartsList));
    }

    @JsProperty
    void setChartsList(JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType> chartsList);

    @JsProperty
    void setCols(double cols);

    @JsProperty
    void setErrorsList(JsArray<String> errorsList);

    @JsOverlay
    default void setErrorsList(String[] errorsList) {
      setErrorsList(Js.<JsArray<String>>uncheckedCast(errorsList));
    }

    @JsProperty
    void setRows(double rows);

    @JsProperty
    void setTableidsList(
        JsArray<FigureDescriptor.ToObjectReturnType.TableidsListFieldType> tableidsList);

    @JsOverlay
    default void setTableidsList(
        FigureDescriptor.ToObjectReturnType.TableidsListFieldType[] tableidsList) {
      setTableidsList(
          Js.<JsArray<FigureDescriptor.ToObjectReturnType.TableidsListFieldType>>uncheckedCast(
              tableidsList));
    }

    @JsProperty
    void setTablemapsList(JsArray<Object> tablemapsList);

    @JsOverlay
    default void setTablemapsList(Object[] tablemapsList) {
      setTablemapsList(Js.<JsArray<Object>>uncheckedCast(tablemapsList));
    }

    @JsProperty
    void setTitle(String title);

    @JsProperty
    void setTitlecolor(String titlecolor);

    @JsProperty
    void setTitlefont(String titlefont);

    @JsProperty
    void setUpdateinterval(double updateinterval);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ChartsListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface MultiseriesListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface DatasourcesListFieldType {
          @JsOverlay
          static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                  .DatasourcesListFieldType
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
          static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                  .LinecolorFieldType
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
          static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                  .LinesvisibleFieldType
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
          static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                  .PointsizeFieldType
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
        static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                    .DatasourcesListFieldType>
            getDatasourcesList();

        @JsProperty
        Object getGradientvisible();

        @JsProperty
        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                .LinecolorFieldType
            getLinecolor();

        @JsProperty
        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                .LinesvisibleFieldType
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
        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                .PointsizeFieldType
            getPointsize();

        @JsProperty
        Object getPointsvisible();

        @JsProperty
        Object getXtooltippattern();

        @JsProperty
        Object getYtooltippattern();

        @JsOverlay
        default void setDatasourcesList(
            FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                        .DatasourcesListFieldType
                    []
                datasourcesList) {
          setDatasourcesList(
              Js
                  .<JsArray<
                          FigureDescriptor.ToObjectReturnType0.ChartsListFieldType
                              .MultiseriesListFieldType.DatasourcesListFieldType>>
                      uncheckedCast(datasourcesList));
        }

        @JsProperty
        void setDatasourcesList(
            JsArray<
                    FigureDescriptor.ToObjectReturnType0.ChartsListFieldType
                        .MultiseriesListFieldType.DatasourcesListFieldType>
                datasourcesList);

        @JsProperty
        void setGradientvisible(Object gradientvisible);

        @JsProperty
        void setLinecolor(
            FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                    .LinecolorFieldType
                linecolor);

        @JsProperty
        void setLinesvisible(
            FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                    .LinesvisibleFieldType
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
            FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType
                    .PointsizeFieldType
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
            static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                    .DatasourcesListFieldType.AxisFieldType
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
            static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                    .DatasourcesListFieldType.OneclickFieldType
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
          static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                  .DatasourcesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                  .DatasourcesListFieldType.AxisFieldType
              getAxis();

          @JsProperty
          String getColumnname();

          @JsProperty
          String getColumntype();

          @JsProperty
          FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                  .DatasourcesListFieldType.OneclickFieldType
              getOneclick();

          @JsProperty
          double getTableid();

          @JsProperty
          double getTablemapid();

          @JsProperty
          double getType();

          @JsProperty
          void setAxis(
              FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                      .DatasourcesListFieldType.AxisFieldType
                  axis);

          @JsProperty
          void setColumnname(String columnname);

          @JsProperty
          void setColumntype(String columntype);

          @JsProperty
          void setOneclick(
              FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                      .DatasourcesListFieldType.OneclickFieldType
                  oneclick);

          @JsProperty
          void setTableid(double tableid);

          @JsProperty
          void setTablemapid(double tablemapid);

          @JsProperty
          void setType(double type);
        }

        @JsOverlay
        static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                    .DatasourcesListFieldType>
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
            FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                        .DatasourcesListFieldType
                    []
                datasourcesList) {
          setDatasourcesList(
              Js
                  .<JsArray<
                          FigureDescriptor.ToObjectReturnType0.ChartsListFieldType
                              .SeriesListFieldType.DatasourcesListFieldType>>
                      uncheckedCast(datasourcesList));
        }

        @JsProperty
        void setDatasourcesList(
            JsArray<
                    FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType
                        .DatasourcesListFieldType>
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
      static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType create() {
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
      JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType>
          getMultiseriesList();

      @JsProperty
      double getRowspan();

      @JsProperty
      JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType>
          getSeriesList();

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
          JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType>
              multiseriesList);

      @JsOverlay
      default void setMultiseriesList(
          FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiseriesListFieldType[]
              multiseriesList) {
        setMultiseriesList(
            Js
                .<JsArray<
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType
                            .MultiseriesListFieldType>>
                    uncheckedCast(multiseriesList));
      }

      @JsProperty
      void setRowspan(double rowspan);

      @JsProperty
      void setSeriesList(
          JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType>
              seriesList);

      @JsOverlay
      default void setSeriesList(
          FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType[]
              seriesList) {
        setSeriesList(
            Js
                .<JsArray<
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType
                            .SeriesListFieldType>>
                    uncheckedCast(seriesList));
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
    public interface TableidsListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FigureDescriptor.ToObjectReturnType0.TableidsListFieldType.GetIdUnionType of(
            Object o) {
          return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
          return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
          return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
          return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
          return (Object) this instanceof Uint8Array;
        }
      }

      @JsOverlay
      static FigureDescriptor.ToObjectReturnType0.TableidsListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FigureDescriptor.ToObjectReturnType0.TableidsListFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FigureDescriptor.ToObjectReturnType0.TableidsListFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FigureDescriptor.ToObjectReturnType0.TableidsListFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FigureDescriptor.ToObjectReturnType0.TableidsListFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static FigureDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType> getChartsList();

    @JsProperty
    double getCols();

    @JsProperty
    JsArray<String> getErrorsList();

    @JsProperty
    double getRows();

    @JsProperty
    JsArray<FigureDescriptor.ToObjectReturnType0.TableidsListFieldType> getTableidsList();

    @JsProperty
    JsArray<Object> getTablemapsList();

    @JsProperty
    String getTitle();

    @JsProperty
    String getTitlecolor();

    @JsProperty
    String getTitlefont();

    @JsProperty
    double getUpdateinterval();

    @JsOverlay
    default void setChartsList(
        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType[] chartsList) {
      setChartsList(
          Js.<JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType>>uncheckedCast(
              chartsList));
    }

    @JsProperty
    void setChartsList(
        JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType> chartsList);

    @JsProperty
    void setCols(double cols);

    @JsProperty
    void setErrorsList(JsArray<String> errorsList);

    @JsOverlay
    default void setErrorsList(String[] errorsList) {
      setErrorsList(Js.<JsArray<String>>uncheckedCast(errorsList));
    }

    @JsProperty
    void setRows(double rows);

    @JsProperty
    void setTableidsList(
        JsArray<FigureDescriptor.ToObjectReturnType0.TableidsListFieldType> tableidsList);

    @JsOverlay
    default void setTableidsList(
        FigureDescriptor.ToObjectReturnType0.TableidsListFieldType[] tableidsList) {
      setTableidsList(
          Js.<JsArray<FigureDescriptor.ToObjectReturnType0.TableidsListFieldType>>uncheckedCast(
              tableidsList));
    }

    @JsProperty
    void setTablemapsList(JsArray<Object> tablemapsList);

    @JsOverlay
    default void setTablemapsList(Object[] tablemapsList) {
      setTablemapsList(Js.<JsArray<Object>>uncheckedCast(tablemapsList));
    }

    @JsProperty
    void setTitle(String title);

    @JsProperty
    void setTitlecolor(String titlecolor);

    @JsProperty
    void setTitlefont(String titlefont);

    @JsProperty
    void setUpdateinterval(double updateinterval);
  }

  public static SeriesPlotStyleMap SeriesPlotStyle;
  public static SourceTypeMap SourceType;

  public static native FigureDescriptor deserializeBinary(Uint8Array bytes);

  public static native FigureDescriptor deserializeBinaryFromReader(
      FigureDescriptor message, Object reader);

  public static native void serializeBinaryToWriter(FigureDescriptor message, Object writer);

  public static native FigureDescriptor.ToObjectReturnType toObject(
      boolean includeInstance, FigureDescriptor msg);

  public native ChartDescriptor addCharts();

  public native ChartDescriptor addCharts(ChartDescriptor value, double index);

  public native ChartDescriptor addCharts(ChartDescriptor value);

  public native String addErrors(String value, double index);

  public native String addErrors(String value);

  public native Ticket addTableids();

  public native Ticket addTableids(Ticket value, double index);

  public native Ticket addTableids(Ticket value);

  public native Ticket addTablemaps();

  public native Ticket addTablemaps(Ticket value, double index);

  public native Ticket addTablemaps(Ticket value);

  public native void clearChartsList();

  public native void clearErrorsList();

  public native void clearTableidsList();

  public native void clearTablemapsList();

  public native JsArray<ChartDescriptor> getChartsList();

  public native double getCols();

  public native JsArray<String> getErrorsList();

  public native double getRows();

  public native JsArray<Ticket> getTableidsList();

  public native JsArray<Ticket> getTablemapsList();

  public native String getTitle();

  public native String getTitlecolor();

  public native String getTitlefont();

  public native double getUpdateinterval();

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setChartsList(ChartDescriptor[] value) {
    setChartsList(Js.<JsArray<ChartDescriptor>>uncheckedCast(value));
  }

  public native void setChartsList(JsArray<ChartDescriptor> value);

  public native void setCols(double value);

  public native void setErrorsList(JsArray<String> value);

  @JsOverlay
  public final void setErrorsList(String[] value) {
    setErrorsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setRows(double value);

  public native void setTableidsList(JsArray<Ticket> value);

  @JsOverlay
  public final void setTableidsList(Ticket[] value) {
    setTableidsList(Js.<JsArray<Ticket>>uncheckedCast(value));
  }

  public native void setTablemapsList(JsArray<Ticket> value);

  @JsOverlay
  public final void setTablemapsList(Ticket[] value) {
    setTablemapsList(Js.<JsArray<Ticket>>uncheckedCast(value));
  }

  public native void setTitle(String value);

  public native void setTitlecolor(String value);

  public native void setTitlefont(String value);

  public native void setUpdateinterval(double value);

  public native FigureDescriptor.ToObjectReturnType0 toObject();

  public native FigureDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
