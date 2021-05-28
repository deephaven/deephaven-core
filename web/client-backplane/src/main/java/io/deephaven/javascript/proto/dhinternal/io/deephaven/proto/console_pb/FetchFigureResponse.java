package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

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
    name = "dhinternal.io.deephaven.proto.console_pb.FetchFigureResponse",
    namespace = JsPackage.GLOBAL)
public class FetchFigureResponse {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FiguredescriptorFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ChartsListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MultiseriesListFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface DatasourcesListFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType.DatasourcesListFieldType
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
            static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType.LinecolorFieldType
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
            static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType.LinesvisibleFieldType
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
            static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType.PointsizeFieldType
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
          static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                  .ChartsListFieldType.MultiseriesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<
                  FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                      .ChartsListFieldType.MultiseriesListFieldType.DatasourcesListFieldType>
              getDatasourcesList();

          @JsProperty
          Object getGradientvisible();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                  .MultiseriesListFieldType.LinecolorFieldType
              getLinecolor();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                  .MultiseriesListFieldType.LinesvisibleFieldType
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
          FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                  .MultiseriesListFieldType.PointsizeFieldType
              getPointsize();

          @JsProperty
          Object getPointsvisible();

          @JsProperty
          Object getXtooltippattern();

          @JsProperty
          Object getYtooltippattern();

          @JsOverlay
          default void setDatasourcesList(
              FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                          .MultiseriesListFieldType.DatasourcesListFieldType
                      []
                  datasourcesList) {
            setDatasourcesList(
                Js
                    .<JsArray<
                            FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                                .ChartsListFieldType.MultiseriesListFieldType
                                .DatasourcesListFieldType>>
                        uncheckedCast(datasourcesList));
          }

          @JsProperty
          void setDatasourcesList(
              JsArray<
                      FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                          .ChartsListFieldType.MultiseriesListFieldType.DatasourcesListFieldType>
                  datasourcesList);

          @JsProperty
          void setGradientvisible(Object gradientvisible);

          @JsProperty
          void setLinecolor(
              FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                      .MultiseriesListFieldType.LinecolorFieldType
                  linecolor);

          @JsProperty
          void setLinesvisible(
              FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                      .MultiseriesListFieldType.LinesvisibleFieldType
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
              FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                      .MultiseriesListFieldType.PointsizeFieldType
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
              static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType
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
                setMajorticklocationsList(
                    Js.<JsArray<Double>>uncheckedCast(majorticklocationsList));
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
              static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType
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
            static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                    .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                    .SeriesListFieldType.DatasourcesListFieldType.AxisFieldType
                getAxis();

            @JsProperty
            String getColumnname();

            @JsProperty
            String getColumntype();

            @JsProperty
            FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                    .SeriesListFieldType.DatasourcesListFieldType.OneclickFieldType
                getOneclick();

            @JsProperty
            double getTableid();

            @JsProperty
            double getTablemapid();

            @JsProperty
            double getType();

            @JsProperty
            void setAxis(
                FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                        .SeriesListFieldType.DatasourcesListFieldType.AxisFieldType
                    axis);

            @JsProperty
            void setColumnname(String columnname);

            @JsProperty
            void setColumntype(String columntype);

            @JsProperty
            void setOneclick(
                FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                        .SeriesListFieldType.DatasourcesListFieldType.OneclickFieldType
                    oneclick);

            @JsProperty
            void setTableid(double tableid);

            @JsProperty
            void setTablemapid(double tablemapid);

            @JsProperty
            void setType(double type);
          }

          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                  .ChartsListFieldType.SeriesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<
                  FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType>
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
              FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                          .SeriesListFieldType.DatasourcesListFieldType
                      []
                  datasourcesList) {
            setDatasourcesList(
                Js
                    .<JsArray<
                            FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                                .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType>>
                        uncheckedCast(datasourcesList));
          }

          @JsProperty
          void setDatasourcesList(
              JsArray<
                      FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                          .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType>
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
        static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
            create() {
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
        JsArray<
                FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                    .MultiseriesListFieldType>
            getMultiseriesList();

        @JsProperty
        double getRowspan();

        @JsProperty
        JsArray<
                FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                    .SeriesListFieldType>
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
            JsArray<
                    FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                        .ChartsListFieldType.MultiseriesListFieldType>
                multiseriesList);

        @JsOverlay
        default void setMultiseriesList(
            FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                        .MultiseriesListFieldType
                    []
                multiseriesList) {
          setMultiseriesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                              .ChartsListFieldType.MultiseriesListFieldType>>
                      uncheckedCast(multiseriesList));
        }

        @JsProperty
        void setRowspan(double rowspan);

        @JsProperty
        void setSeriesList(
            JsArray<
                    FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                        .ChartsListFieldType.SeriesListFieldType>
                seriesList);

        @JsOverlay
        default void setSeriesList(
            FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType
                        .SeriesListFieldType
                    []
                seriesList) {
          setSeriesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                              .ChartsListFieldType.SeriesListFieldType>>
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
          static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                  .TableidsListFieldType.GetIdUnionType
              of(Object o) {
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
        static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                .TableidsListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.TableidsListFieldType
                .GetIdUnionType
            getId();

        @JsProperty
        void setId(
            FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.TableidsListFieldType
                    .GetIdUnionType
                id);

        @JsOverlay
        default void setId(String id) {
          setId(
              Js
                  .<FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                          .TableidsListFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }

        @JsOverlay
        default void setId(Uint8Array id) {
          setId(
              Js
                  .<FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                          .TableidsListFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }
      }

      @JsOverlay
      static FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType>
          getChartsList();

      @JsProperty
      double getCols();

      @JsProperty
      JsArray<String> getErrorsList();

      @JsProperty
      double getRows();

      @JsProperty
      JsArray<
              FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                  .TableidsListFieldType>
          getTableidsList();

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
          FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.ChartsListFieldType[]
              chartsList) {
        setChartsList(
            Js
                .<JsArray<
                        FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                            .ChartsListFieldType>>
                    uncheckedCast(chartsList));
      }

      @JsProperty
      void setChartsList(
          JsArray<
                  FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                      .ChartsListFieldType>
              chartsList);

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
          JsArray<
                  FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                      .TableidsListFieldType>
              tableidsList);

      @JsOverlay
      default void setTableidsList(
          FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType.TableidsListFieldType[]
              tableidsList) {
        setTableidsList(
            Js
                .<JsArray<
                        FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType
                            .TableidsListFieldType>>
                    uncheckedCast(tableidsList));
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

    @JsOverlay
    static FetchFigureResponse.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType getFiguredescriptor();

    @JsProperty
    void setFiguredescriptor(
        FetchFigureResponse.ToObjectReturnType.FiguredescriptorFieldType figuredescriptor);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FiguredescriptorFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ChartsListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MultiseriesListFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface DatasourcesListFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType.DatasourcesListFieldType
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
            static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType.LinecolorFieldType
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
            static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType.LinesvisibleFieldType
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
            static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType.PointsizeFieldType
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
          static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                  .ChartsListFieldType.MultiseriesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<
                  FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                      .ChartsListFieldType.MultiseriesListFieldType.DatasourcesListFieldType>
              getDatasourcesList();

          @JsProperty
          Object getGradientvisible();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                  .MultiseriesListFieldType.LinecolorFieldType
              getLinecolor();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                  .MultiseriesListFieldType.LinesvisibleFieldType
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
          FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                  .MultiseriesListFieldType.PointsizeFieldType
              getPointsize();

          @JsProperty
          Object getPointsvisible();

          @JsProperty
          Object getXtooltippattern();

          @JsProperty
          Object getYtooltippattern();

          @JsOverlay
          default void setDatasourcesList(
              FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                          .MultiseriesListFieldType.DatasourcesListFieldType
                      []
                  datasourcesList) {
            setDatasourcesList(
                Js
                    .<JsArray<
                            FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                                .ChartsListFieldType.MultiseriesListFieldType
                                .DatasourcesListFieldType>>
                        uncheckedCast(datasourcesList));
          }

          @JsProperty
          void setDatasourcesList(
              JsArray<
                      FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                          .ChartsListFieldType.MultiseriesListFieldType.DatasourcesListFieldType>
                  datasourcesList);

          @JsProperty
          void setGradientvisible(Object gradientvisible);

          @JsProperty
          void setLinecolor(
              FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                      .MultiseriesListFieldType.LinecolorFieldType
                  linecolor);

          @JsProperty
          void setLinesvisible(
              FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                      .MultiseriesListFieldType.LinesvisibleFieldType
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
              FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                      .MultiseriesListFieldType.PointsizeFieldType
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
              static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType
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
                setMajorticklocationsList(
                    Js.<JsArray<Double>>uncheckedCast(majorticklocationsList));
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
              static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType
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
            static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                    .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                    .SeriesListFieldType.DatasourcesListFieldType.AxisFieldType
                getAxis();

            @JsProperty
            String getColumnname();

            @JsProperty
            String getColumntype();

            @JsProperty
            FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                    .SeriesListFieldType.DatasourcesListFieldType.OneclickFieldType
                getOneclick();

            @JsProperty
            double getTableid();

            @JsProperty
            double getTablemapid();

            @JsProperty
            double getType();

            @JsProperty
            void setAxis(
                FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                        .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType
                        .AxisFieldType
                    axis);

            @JsProperty
            void setColumnname(String columnname);

            @JsProperty
            void setColumntype(String columntype);

            @JsProperty
            void setOneclick(
                FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                        .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType
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
          static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                  .ChartsListFieldType.SeriesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<
                  FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType>
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
              FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                          .SeriesListFieldType.DatasourcesListFieldType
                      []
                  datasourcesList) {
            setDatasourcesList(
                Js
                    .<JsArray<
                            FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                                .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType>>
                        uncheckedCast(datasourcesList));
          }

          @JsProperty
          void setDatasourcesList(
              JsArray<
                      FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                          .ChartsListFieldType.SeriesListFieldType.DatasourcesListFieldType>
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
        static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
            create() {
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
        JsArray<
                FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                    .ChartsListFieldType.MultiseriesListFieldType>
            getMultiseriesList();

        @JsProperty
        double getRowspan();

        @JsProperty
        JsArray<
                FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                    .ChartsListFieldType.SeriesListFieldType>
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
            JsArray<
                    FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                        .ChartsListFieldType.MultiseriesListFieldType>
                multiseriesList);

        @JsOverlay
        default void setMultiseriesList(
            FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                        .MultiseriesListFieldType
                    []
                multiseriesList) {
          setMultiseriesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                              .ChartsListFieldType.MultiseriesListFieldType>>
                      uncheckedCast(multiseriesList));
        }

        @JsProperty
        void setRowspan(double rowspan);

        @JsProperty
        void setSeriesList(
            JsArray<
                    FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                        .ChartsListFieldType.SeriesListFieldType>
                seriesList);

        @JsOverlay
        default void setSeriesList(
            FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType
                        .SeriesListFieldType
                    []
                seriesList) {
          setSeriesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                              .ChartsListFieldType.SeriesListFieldType>>
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
          static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                  .TableidsListFieldType.GetIdUnionType
              of(Object o) {
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
        static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                .TableidsListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.TableidsListFieldType
                .GetIdUnionType
            getId();

        @JsProperty
        void setId(
            FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.TableidsListFieldType
                    .GetIdUnionType
                id);

        @JsOverlay
        default void setId(String id) {
          setId(
              Js
                  .<FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                          .TableidsListFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }

        @JsOverlay
        default void setId(Uint8Array id) {
          setId(
              Js
                  .<FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                          .TableidsListFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }
      }

      @JsOverlay
      static FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType>
          getChartsList();

      @JsProperty
      double getCols();

      @JsProperty
      JsArray<String> getErrorsList();

      @JsProperty
      double getRows();

      @JsProperty
      JsArray<
              FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                  .TableidsListFieldType>
          getTableidsList();

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
          FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.ChartsListFieldType[]
              chartsList) {
        setChartsList(
            Js
                .<JsArray<
                        FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                            .ChartsListFieldType>>
                    uncheckedCast(chartsList));
      }

      @JsProperty
      void setChartsList(
          JsArray<
                  FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                      .ChartsListFieldType>
              chartsList);

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
          JsArray<
                  FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                      .TableidsListFieldType>
              tableidsList);

      @JsOverlay
      default void setTableidsList(
          FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType.TableidsListFieldType[]
              tableidsList) {
        setTableidsList(
            Js
                .<JsArray<
                        FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType
                            .TableidsListFieldType>>
                    uncheckedCast(tableidsList));
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

    @JsOverlay
    static FetchFigureResponse.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType getFiguredescriptor();

    @JsProperty
    void setFiguredescriptor(
        FetchFigureResponse.ToObjectReturnType0.FiguredescriptorFieldType figuredescriptor);
  }

  public static native FetchFigureResponse deserializeBinary(Uint8Array bytes);

  public static native FetchFigureResponse deserializeBinaryFromReader(
      FetchFigureResponse message, Object reader);

  public static native void serializeBinaryToWriter(FetchFigureResponse message, Object writer);

  public static native FetchFigureResponse.ToObjectReturnType toObject(
      boolean includeInstance, FetchFigureResponse msg);

  public native void clearFiguredescriptor();

  public native FigureDescriptor getFiguredescriptor();

  public native boolean hasFiguredescriptor();

  public native Uint8Array serializeBinary();

  public native void setFiguredescriptor();

  public native void setFiguredescriptor(FigureDescriptor value);

  public native FetchFigureResponse.ToObjectReturnType0 toObject();

  public native FetchFigureResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
