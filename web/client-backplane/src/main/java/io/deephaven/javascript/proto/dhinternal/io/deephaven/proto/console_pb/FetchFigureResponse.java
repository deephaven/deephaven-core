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
    public interface FigureDescriptorFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ChartsListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AxesListFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface BusinessCalendarDescriptorFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface BusinessPeriodsListFieldType {
              @JsOverlay
              static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                      .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                      .BusinessPeriodsListFieldType
                  create() {
                return Js.uncheckedCast(JsPropertyMap.of());
              }

              @JsProperty
              String getClose();

              @JsProperty
              String getOpen();

              @JsProperty
              void setClose(String close);

              @JsProperty
              void setOpen(String open);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface HolidaysListFieldType {
              @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
              public interface DateFieldType {
                @JsOverlay
                static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                        .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                        .HolidaysListFieldType.DateFieldType
                    create() {
                  return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getDay();

                @JsProperty
                double getMonth();

                @JsProperty
                double getYear();

                @JsProperty
                void setDay(double day);

                @JsProperty
                void setMonth(double month);

                @JsProperty
                void setYear(double year);
              }

              @JsOverlay
              static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                      .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                      .HolidaysListFieldType
                  create() {
                return Js.uncheckedCast(JsPropertyMap.of());
              }

              @JsProperty
              JsArray<Object> getBusinessPeriodsList();

              @JsProperty
              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                      .AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType
                      .DateFieldType
                  getDate();

              @JsProperty
              void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

              @JsOverlay
              default void setBusinessPeriodsList(Object[] businessPeriodsList) {
                setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
              }

              @JsProperty
              void setDate(
                  FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                          .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                          .HolidaysListFieldType.DateFieldType
                      date);
            }

            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                    .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Double> getBusinessDaysList();

            @JsProperty
            JsArray<
                    FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                        .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                        .BusinessPeriodsListFieldType>
                getBusinessPeriodsList();

            @JsProperty
            JsArray<
                    FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                        .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                        .HolidaysListFieldType>
                getHolidaysList();

            @JsProperty
            String getName();

            @JsProperty
            String getTimeZone();

            @JsProperty
            void setBusinessDaysList(JsArray<Double> businessDaysList);

            @JsOverlay
            default void setBusinessDaysList(double[] businessDaysList) {
              setBusinessDaysList(Js.<JsArray<Double>>uncheckedCast(businessDaysList));
            }

            @JsOverlay
            default void setBusinessPeriodsList(
                FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                            .AxesListFieldType.BusinessCalendarDescriptorFieldType
                            .BusinessPeriodsListFieldType
                        []
                    businessPeriodsList) {
              setBusinessPeriodsList(
                  Js
                      .<JsArray<
                              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                                  .ChartsListFieldType.AxesListFieldType
                                  .BusinessCalendarDescriptorFieldType
                                  .BusinessPeriodsListFieldType>>
                          uncheckedCast(businessPeriodsList));
            }

            @JsProperty
            void setBusinessPeriodsList(
                JsArray<
                        FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                            .ChartsListFieldType.AxesListFieldType
                            .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>
                    businessPeriodsList);

            @JsOverlay
            default void setHolidaysList(
                FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                            .AxesListFieldType.BusinessCalendarDescriptorFieldType
                            .HolidaysListFieldType
                        []
                    holidaysList) {
              setHolidaysList(
                  Js
                      .<JsArray<
                              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                                  .ChartsListFieldType.AxesListFieldType
                                  .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>>
                          uncheckedCast(holidaysList));
            }

            @JsProperty
            void setHolidaysList(
                JsArray<
                        FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                            .ChartsListFieldType.AxesListFieldType
                            .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>
                    holidaysList);

            @JsProperty
            void setName(String name);

            @JsProperty
            void setTimeZone(String timeZone);
          }

          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                  .ChartsListFieldType.AxesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                  .AxesListFieldType.BusinessCalendarDescriptorFieldType
              getBusinessCalendarDescriptor();

          @JsProperty
          String getColor();

          @JsProperty
          String getFormatPattern();

          @JsProperty
          double getFormatType();

          @JsProperty
          double getGapBetweenMajorTicks();

          @JsProperty
          String getId();

          @JsProperty
          String getLabel();

          @JsProperty
          String getLabelFont();

          @JsProperty
          JsArray<Double> getMajorTickLocationsList();

          @JsProperty
          double getMaxRange();

          @JsProperty
          double getMinRange();

          @JsProperty
          double getMinorTickCount();

          @JsProperty
          double getPosition();

          @JsProperty
          double getTickLabelAngle();

          @JsProperty
          String getTicksFont();

          @JsProperty
          double getType();

          @JsProperty
          boolean isInvert();

          @JsProperty
          boolean isIsTimeAxis();

          @JsProperty
          boolean isLog();

          @JsProperty
          boolean isMajorTicksVisible();

          @JsProperty
          boolean isMinorTicksVisible();

          @JsProperty
          void setBusinessCalendarDescriptor(
              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                      .AxesListFieldType.BusinessCalendarDescriptorFieldType
                  businessCalendarDescriptor);

          @JsProperty
          void setColor(String color);

          @JsProperty
          void setFormatPattern(String formatPattern);

          @JsProperty
          void setFormatType(double formatType);

          @JsProperty
          void setGapBetweenMajorTicks(double gapBetweenMajorTicks);

          @JsProperty
          void setId(String id);

          @JsProperty
          void setInvert(boolean invert);

          @JsProperty
          void setIsTimeAxis(boolean isTimeAxis);

          @JsProperty
          void setLabel(String label);

          @JsProperty
          void setLabelFont(String labelFont);

          @JsProperty
          void setLog(boolean log);

          @JsProperty
          void setMajorTickLocationsList(JsArray<Double> majorTickLocationsList);

          @JsOverlay
          default void setMajorTickLocationsList(double[] majorTickLocationsList) {
            setMajorTickLocationsList(Js.<JsArray<Double>>uncheckedCast(majorTickLocationsList));
          }

          @JsProperty
          void setMajorTicksVisible(boolean majorTicksVisible);

          @JsProperty
          void setMaxRange(double maxRange);

          @JsProperty
          void setMinRange(double minRange);

          @JsProperty
          void setMinorTickCount(double minorTickCount);

          @JsProperty
          void setMinorTicksVisible(boolean minorTicksVisible);

          @JsProperty
          void setPosition(double position);

          @JsProperty
          void setTickLabelAngle(double tickLabelAngle);

          @JsProperty
          void setTicksFont(String ticksFont);

          @JsProperty
          void setType(double type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MultiSeriesListFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface DataSourcesListFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getAxisId();

            @JsProperty
            String getColumnName();

            @JsProperty
            double getTableMapId();

            @JsProperty
            double getType();

            @JsProperty
            void setAxisId(String axisId);

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setTableMapId(double tableMapId);

            @JsProperty
            void setType(double type);
          }

          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface LineColorFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType.LineColorFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getDefaultString();

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<String> getValuesList();

            @JsProperty
            void setDefaultString(String defaultString);

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
          public interface LinesVisibleFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType.LinesVisibleFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<Boolean> getValuesList();

            @JsProperty
            boolean isDefaultBool();

            @JsProperty
            void setDefaultBool(boolean defaultBool);

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
          public interface PointSizeFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType.PointSizeFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getDefaultDouble();

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<Double> getValuesList();

            @JsProperty
            void setDefaultDouble(double defaultDouble);

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
          static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                  .ChartsListFieldType.MultiSeriesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<
                  FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                      .ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType>
              getDataSourcesList();

          @JsProperty
          Object getGradientVisible();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                  .MultiSeriesListFieldType.LineColorFieldType
              getLineColor();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                  .MultiSeriesListFieldType.LinesVisibleFieldType
              getLinesVisible();

          @JsProperty
          String getName();

          @JsProperty
          double getPlotStyle();

          @JsProperty
          Object getPointColor();

          @JsProperty
          Object getPointLabel();

          @JsProperty
          Object getPointLabelFormat();

          @JsProperty
          Object getPointShape();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                  .MultiSeriesListFieldType.PointSizeFieldType
              getPointSize();

          @JsProperty
          Object getPointsVisible();

          @JsProperty
          Object getXToolTipPattern();

          @JsProperty
          Object getYToolTipPattern();

          @JsOverlay
          default void setDataSourcesList(
              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                          .MultiSeriesListFieldType.DataSourcesListFieldType
                      []
                  dataSourcesList) {
            setDataSourcesList(
                Js
                    .<JsArray<
                            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                                .ChartsListFieldType.MultiSeriesListFieldType
                                .DataSourcesListFieldType>>
                        uncheckedCast(dataSourcesList));
          }

          @JsProperty
          void setDataSourcesList(
              JsArray<
                      FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                          .ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType>
                  dataSourcesList);

          @JsProperty
          void setGradientVisible(Object gradientVisible);

          @JsProperty
          void setLineColor(
              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                      .MultiSeriesListFieldType.LineColorFieldType
                  lineColor);

          @JsProperty
          void setLinesVisible(
              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                      .MultiSeriesListFieldType.LinesVisibleFieldType
                  linesVisible);

          @JsProperty
          void setName(String name);

          @JsProperty
          void setPlotStyle(double plotStyle);

          @JsProperty
          void setPointColor(Object pointColor);

          @JsProperty
          void setPointLabel(Object pointLabel);

          @JsProperty
          void setPointLabelFormat(Object pointLabelFormat);

          @JsProperty
          void setPointShape(Object pointShape);

          @JsProperty
          void setPointSize(
              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                      .MultiSeriesListFieldType.PointSizeFieldType
                  pointSize);

          @JsProperty
          void setPointsVisible(Object pointsVisible);

          @JsProperty
          void setXToolTipPattern(Object xToolTipPattern);

          @JsProperty
          void setYToolTipPattern(Object yToolTipPattern);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SeriesListFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface DataSourcesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OneClickFieldType {
              @JsOverlay
              static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType
                      .OneClickFieldType
                  create() {
                return Js.uncheckedCast(JsPropertyMap.of());
              }

              @JsProperty
              JsArray<String> getColumnTypesList();

              @JsProperty
              JsArray<String> getColumnsList();

              @JsProperty
              boolean isRequireAllFiltersToDisplay();

              @JsProperty
              void setColumnTypesList(JsArray<String> columnTypesList);

              @JsOverlay
              default void setColumnTypesList(String[] columnTypesList) {
                setColumnTypesList(Js.<JsArray<String>>uncheckedCast(columnTypesList));
              }

              @JsProperty
              void setColumnsList(JsArray<String> columnsList);

              @JsOverlay
              default void setColumnsList(String[] columnsList) {
                setColumnsList(Js.<JsArray<String>>uncheckedCast(columnsList));
              }

              @JsProperty
              void setRequireAllFiltersToDisplay(boolean requireAllFiltersToDisplay);
            }

            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                    .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getAxisId();

            @JsProperty
            String getColumnName();

            @JsProperty
            String getColumnType();

            @JsProperty
            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                    .SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType
                getOneClick();

            @JsProperty
            double getTableId();

            @JsProperty
            double getTableMapId();

            @JsProperty
            double getType();

            @JsProperty
            void setAxisId(String axisId);

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setColumnType(String columnType);

            @JsProperty
            void setOneClick(
                FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                        .SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType
                    oneClick);

            @JsProperty
            void setTableId(double tableId);

            @JsProperty
            void setTableMapId(double tableMapId);

            @JsProperty
            void setType(double type);
          }

          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                  .ChartsListFieldType.SeriesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<
                  FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType>
              getDataSourcesList();

          @JsProperty
          String getLineColor();

          @JsProperty
          String getName();

          @JsProperty
          double getPlotStyle();

          @JsProperty
          String getPointLabelFormat();

          @JsProperty
          String getShape();

          @JsProperty
          String getShapeColor();

          @JsProperty
          String getShapeLabel();

          @JsProperty
          double getShapeSize();

          @JsProperty
          String getXToolTipPattern();

          @JsProperty
          String getYToolTipPattern();

          @JsProperty
          boolean isGradientVisible();

          @JsProperty
          boolean isLinesVisible();

          @JsProperty
          boolean isShapesVisible();

          @JsOverlay
          default void setDataSourcesList(
              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                          .SeriesListFieldType.DataSourcesListFieldType
                      []
                  dataSourcesList) {
            setDataSourcesList(
                Js
                    .<JsArray<
                            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                                .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType>>
                        uncheckedCast(dataSourcesList));
          }

          @JsProperty
          void setDataSourcesList(
              JsArray<
                      FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                          .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType>
                  dataSourcesList);

          @JsProperty
          void setGradientVisible(boolean gradientVisible);

          @JsProperty
          void setLineColor(String lineColor);

          @JsProperty
          void setLinesVisible(boolean linesVisible);

          @JsProperty
          void setName(String name);

          @JsProperty
          void setPlotStyle(double plotStyle);

          @JsProperty
          void setPointLabelFormat(String pointLabelFormat);

          @JsProperty
          void setShape(String shape);

          @JsProperty
          void setShapeColor(String shapeColor);

          @JsProperty
          void setShapeLabel(String shapeLabel);

          @JsProperty
          void setShapeSize(double shapeSize);

          @JsProperty
          void setShapesVisible(boolean shapesVisible);

          @JsProperty
          void setXToolTipPattern(String xToolTipPattern);

          @JsProperty
          void setYToolTipPattern(String yToolTipPattern);
        }

        @JsOverlay
        static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                    .AxesListFieldType>
            getAxesList();

        @JsProperty
        double getChartType();

        @JsProperty
        double getColspan();

        @JsProperty
        String getLegendColor();

        @JsProperty
        String getLegendFont();

        @JsProperty
        JsArray<
                FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                    .MultiSeriesListFieldType>
            getMultiSeriesList();

        @JsProperty
        double getRowspan();

        @JsProperty
        JsArray<
                FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                    .SeriesListFieldType>
            getSeriesList();

        @JsProperty
        String getTitle();

        @JsProperty
        String getTitleColor();

        @JsProperty
        String getTitleFont();

        @JsProperty
        boolean isIs3d();

        @JsProperty
        boolean isShowLegend();

        @JsOverlay
        default void setAxesList(
            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                        .AxesListFieldType
                    []
                axesList) {
          setAxesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                              .ChartsListFieldType.AxesListFieldType>>
                      uncheckedCast(axesList));
        }

        @JsProperty
        void setAxesList(
            JsArray<
                    FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                        .ChartsListFieldType.AxesListFieldType>
                axesList);

        @JsProperty
        void setChartType(double chartType);

        @JsProperty
        void setColspan(double colspan);

        @JsProperty
        void setIs3d(boolean is3d);

        @JsProperty
        void setLegendColor(String legendColor);

        @JsProperty
        void setLegendFont(String legendFont);

        @JsProperty
        void setMultiSeriesList(
            JsArray<
                    FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                        .ChartsListFieldType.MultiSeriesListFieldType>
                multiSeriesList);

        @JsOverlay
        default void setMultiSeriesList(
            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                        .MultiSeriesListFieldType
                    []
                multiSeriesList) {
          setMultiSeriesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                              .ChartsListFieldType.MultiSeriesListFieldType>>
                      uncheckedCast(multiSeriesList));
        }

        @JsProperty
        void setRowspan(double rowspan);

        @JsProperty
        void setSeriesList(
            JsArray<
                    FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                        .ChartsListFieldType.SeriesListFieldType>
                seriesList);

        @JsOverlay
        default void setSeriesList(
            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType
                        .SeriesListFieldType
                    []
                seriesList) {
          setSeriesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                              .ChartsListFieldType.SeriesListFieldType>>
                      uncheckedCast(seriesList));
        }

        @JsProperty
        void setShowLegend(boolean showLegend);

        @JsProperty
        void setTitle(String title);

        @JsProperty
        void setTitleColor(String titleColor);

        @JsProperty
        void setTitleFont(String titleFont);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface TablesListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetSchemaHeaderUnionType {
          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                  .TablesListFieldType.GetSchemaHeaderUnionType
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface TicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
              @JsOverlay
              static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                      .TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType
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
            static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                    .TablesListFieldType.ResultIdFieldType.TicketFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
                    .ResultIdFieldType.TicketFieldType.GetTicketUnionType
                getTicket();

            @JsProperty
            void setTicket(
                FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
                        .ResultIdFieldType.TicketFieldType.GetTicketUnionType
                    ticket);

            @JsOverlay
            default void setTicket(String ticket) {
              setTicket(
                  Js
                      .<FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                              .TablesListFieldType.ResultIdFieldType.TicketFieldType
                              .GetTicketUnionType>
                          uncheckedCast(ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
              setTicket(
                  Js
                      .<FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                              .TablesListFieldType.ResultIdFieldType.TicketFieldType
                              .GetTicketUnionType>
                          uncheckedCast(ticket));
            }
          }

          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                  .TablesListFieldType.ResultIdFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          double getBatchOffset();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
                  .ResultIdFieldType.TicketFieldType
              getTicket();

          @JsProperty
          void setBatchOffset(double batchOffset);

          @JsProperty
          void setTicket(
              FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
                      .ResultIdFieldType.TicketFieldType
                  ticket);
        }

        @JsOverlay
        static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getErrorInfo();

        @JsProperty
        FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
                .ResultIdFieldType
            getResultId();

        @JsProperty
        FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
                .GetSchemaHeaderUnionType
            getSchemaHeader();

        @JsProperty
        String getSize();

        @JsProperty
        boolean isIsStatic();

        @JsProperty
        boolean isSuccess();

        @JsProperty
        void setErrorInfo(String errorInfo);

        @JsProperty
        void setIsStatic(boolean isStatic);

        @JsProperty
        void setResultId(
            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
                    .ResultIdFieldType
                resultId);

        @JsProperty
        void setSchemaHeader(
            FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType
                    .GetSchemaHeaderUnionType
                schemaHeader);

        @JsOverlay
        default void setSchemaHeader(String schemaHeader) {
          setSchemaHeader(
              Js
                  .<FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                          .TablesListFieldType.GetSchemaHeaderUnionType>
                      uncheckedCast(schemaHeader));
        }

        @JsOverlay
        default void setSchemaHeader(Uint8Array schemaHeader) {
          setSchemaHeader(
              Js
                  .<FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                          .TablesListFieldType.GetSchemaHeaderUnionType>
                      uncheckedCast(schemaHeader));
        }

        @JsProperty
        void setSize(String size);

        @JsProperty
        void setSuccess(boolean success);
      }

      @JsOverlay
      static FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType>
          getChartsList();

      @JsProperty
      double getCols();

      @JsProperty
      JsArray<String> getErrorsList();

      @JsProperty
      double getRows();

      @JsProperty
      JsArray<FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType>
          getTablesList();

      @JsProperty
      String getTitle();

      @JsProperty
      String getTitleColor();

      @JsProperty
      String getTitleFont();

      @JsProperty
      double getUpdateInterval();

      @JsOverlay
      default void setChartsList(
          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.ChartsListFieldType[]
              chartsList) {
        setChartsList(
            Js
                .<JsArray<
                        FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                            .ChartsListFieldType>>
                    uncheckedCast(chartsList));
      }

      @JsProperty
      void setChartsList(
          JsArray<
                  FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
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
      void setTablesList(
          JsArray<
                  FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                      .TablesListFieldType>
              tablesList);

      @JsOverlay
      default void setTablesList(
          FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType.TablesListFieldType[]
              tablesList) {
        setTablesList(
            Js
                .<JsArray<
                        FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType
                            .TablesListFieldType>>
                    uncheckedCast(tablesList));
      }

      @JsProperty
      void setTitle(String title);

      @JsProperty
      void setTitleColor(String titleColor);

      @JsProperty
      void setTitleFont(String titleFont);

      @JsProperty
      void setUpdateInterval(double updateInterval);
    }

    @JsOverlay
    static FetchFigureResponse.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType getFigureDescriptor();

    @JsProperty
    void setFigureDescriptor(
        FetchFigureResponse.ToObjectReturnType.FigureDescriptorFieldType figureDescriptor);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FigureDescriptorFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ChartsListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface AxesListFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface BusinessCalendarDescriptorFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface BusinessPeriodsListFieldType {
              @JsOverlay
              static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                      .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                      .BusinessPeriodsListFieldType
                  create() {
                return Js.uncheckedCast(JsPropertyMap.of());
              }

              @JsProperty
              String getClose();

              @JsProperty
              String getOpen();

              @JsProperty
              void setClose(String close);

              @JsProperty
              void setOpen(String open);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface HolidaysListFieldType {
              @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
              public interface DateFieldType {
                @JsOverlay
                static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                        .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                        .HolidaysListFieldType.DateFieldType
                    create() {
                  return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getDay();

                @JsProperty
                double getMonth();

                @JsProperty
                double getYear();

                @JsProperty
                void setDay(double day);

                @JsProperty
                void setMonth(double month);

                @JsProperty
                void setYear(double year);
              }

              @JsOverlay
              static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                      .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                      .HolidaysListFieldType
                  create() {
                return Js.uncheckedCast(JsPropertyMap.of());
              }

              @JsProperty
              JsArray<Object> getBusinessPeriodsList();

              @JsProperty
              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                      .AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType
                      .DateFieldType
                  getDate();

              @JsProperty
              void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

              @JsOverlay
              default void setBusinessPeriodsList(Object[] businessPeriodsList) {
                setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
              }

              @JsProperty
              void setDate(
                  FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                          .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                          .HolidaysListFieldType.DateFieldType
                      date);
            }

            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<Double> getBusinessDaysList();

            @JsProperty
            JsArray<
                    FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                        .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                        .BusinessPeriodsListFieldType>
                getBusinessPeriodsList();

            @JsProperty
            JsArray<
                    FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                        .ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                        .HolidaysListFieldType>
                getHolidaysList();

            @JsProperty
            String getName();

            @JsProperty
            String getTimeZone();

            @JsProperty
            void setBusinessDaysList(JsArray<Double> businessDaysList);

            @JsOverlay
            default void setBusinessDaysList(double[] businessDaysList) {
              setBusinessDaysList(Js.<JsArray<Double>>uncheckedCast(businessDaysList));
            }

            @JsOverlay
            default void setBusinessPeriodsList(
                FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                            .ChartsListFieldType.AxesListFieldType
                            .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType
                        []
                    businessPeriodsList) {
              setBusinessPeriodsList(
                  Js
                      .<JsArray<
                              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                                  .ChartsListFieldType.AxesListFieldType
                                  .BusinessCalendarDescriptorFieldType
                                  .BusinessPeriodsListFieldType>>
                          uncheckedCast(businessPeriodsList));
            }

            @JsProperty
            void setBusinessPeriodsList(
                JsArray<
                        FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                            .ChartsListFieldType.AxesListFieldType
                            .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>
                    businessPeriodsList);

            @JsOverlay
            default void setHolidaysList(
                FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                            .ChartsListFieldType.AxesListFieldType
                            .BusinessCalendarDescriptorFieldType.HolidaysListFieldType
                        []
                    holidaysList) {
              setHolidaysList(
                  Js
                      .<JsArray<
                              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                                  .ChartsListFieldType.AxesListFieldType
                                  .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>>
                          uncheckedCast(holidaysList));
            }

            @JsProperty
            void setHolidaysList(
                JsArray<
                        FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                            .ChartsListFieldType.AxesListFieldType
                            .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>
                    holidaysList);

            @JsProperty
            void setName(String name);

            @JsProperty
            void setTimeZone(String timeZone);
          }

          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                  .ChartsListFieldType.AxesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                  .AxesListFieldType.BusinessCalendarDescriptorFieldType
              getBusinessCalendarDescriptor();

          @JsProperty
          String getColor();

          @JsProperty
          String getFormatPattern();

          @JsProperty
          double getFormatType();

          @JsProperty
          double getGapBetweenMajorTicks();

          @JsProperty
          String getId();

          @JsProperty
          String getLabel();

          @JsProperty
          String getLabelFont();

          @JsProperty
          JsArray<Double> getMajorTickLocationsList();

          @JsProperty
          double getMaxRange();

          @JsProperty
          double getMinRange();

          @JsProperty
          double getMinorTickCount();

          @JsProperty
          double getPosition();

          @JsProperty
          double getTickLabelAngle();

          @JsProperty
          String getTicksFont();

          @JsProperty
          double getType();

          @JsProperty
          boolean isInvert();

          @JsProperty
          boolean isIsTimeAxis();

          @JsProperty
          boolean isLog();

          @JsProperty
          boolean isMajorTicksVisible();

          @JsProperty
          boolean isMinorTicksVisible();

          @JsProperty
          void setBusinessCalendarDescriptor(
              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                      .AxesListFieldType.BusinessCalendarDescriptorFieldType
                  businessCalendarDescriptor);

          @JsProperty
          void setColor(String color);

          @JsProperty
          void setFormatPattern(String formatPattern);

          @JsProperty
          void setFormatType(double formatType);

          @JsProperty
          void setGapBetweenMajorTicks(double gapBetweenMajorTicks);

          @JsProperty
          void setId(String id);

          @JsProperty
          void setInvert(boolean invert);

          @JsProperty
          void setIsTimeAxis(boolean isTimeAxis);

          @JsProperty
          void setLabel(String label);

          @JsProperty
          void setLabelFont(String labelFont);

          @JsProperty
          void setLog(boolean log);

          @JsProperty
          void setMajorTickLocationsList(JsArray<Double> majorTickLocationsList);

          @JsOverlay
          default void setMajorTickLocationsList(double[] majorTickLocationsList) {
            setMajorTickLocationsList(Js.<JsArray<Double>>uncheckedCast(majorTickLocationsList));
          }

          @JsProperty
          void setMajorTicksVisible(boolean majorTicksVisible);

          @JsProperty
          void setMaxRange(double maxRange);

          @JsProperty
          void setMinRange(double minRange);

          @JsProperty
          void setMinorTickCount(double minorTickCount);

          @JsProperty
          void setMinorTicksVisible(boolean minorTicksVisible);

          @JsProperty
          void setPosition(double position);

          @JsProperty
          void setTickLabelAngle(double tickLabelAngle);

          @JsProperty
          void setTicksFont(String ticksFont);

          @JsProperty
          void setType(double type);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MultiSeriesListFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface DataSourcesListFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getAxisId();

            @JsProperty
            String getColumnName();

            @JsProperty
            double getTableMapId();

            @JsProperty
            double getType();

            @JsProperty
            void setAxisId(String axisId);

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setTableMapId(double tableMapId);

            @JsProperty
            void setType(double type);
          }

          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface LineColorFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType.LineColorFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getDefaultString();

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<String> getValuesList();

            @JsProperty
            void setDefaultString(String defaultString);

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
          public interface LinesVisibleFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType.LinesVisibleFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<Boolean> getValuesList();

            @JsProperty
            boolean isDefaultBool();

            @JsProperty
            void setDefaultBool(boolean defaultBool);

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
          public interface PointSizeFieldType {
            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType.PointSizeFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getDefaultDouble();

            @JsProperty
            JsArray<String> getKeysList();

            @JsProperty
            JsArray<Double> getValuesList();

            @JsProperty
            void setDefaultDouble(double defaultDouble);

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
          static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                  .ChartsListFieldType.MultiSeriesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<
                  FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                      .ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType>
              getDataSourcesList();

          @JsProperty
          Object getGradientVisible();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                  .MultiSeriesListFieldType.LineColorFieldType
              getLineColor();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                  .MultiSeriesListFieldType.LinesVisibleFieldType
              getLinesVisible();

          @JsProperty
          String getName();

          @JsProperty
          double getPlotStyle();

          @JsProperty
          Object getPointColor();

          @JsProperty
          Object getPointLabel();

          @JsProperty
          Object getPointLabelFormat();

          @JsProperty
          Object getPointShape();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                  .MultiSeriesListFieldType.PointSizeFieldType
              getPointSize();

          @JsProperty
          Object getPointsVisible();

          @JsProperty
          Object getXToolTipPattern();

          @JsProperty
          Object getYToolTipPattern();

          @JsOverlay
          default void setDataSourcesList(
              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                          .MultiSeriesListFieldType.DataSourcesListFieldType
                      []
                  dataSourcesList) {
            setDataSourcesList(
                Js
                    .<JsArray<
                            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                                .ChartsListFieldType.MultiSeriesListFieldType
                                .DataSourcesListFieldType>>
                        uncheckedCast(dataSourcesList));
          }

          @JsProperty
          void setDataSourcesList(
              JsArray<
                      FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                          .ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType>
                  dataSourcesList);

          @JsProperty
          void setGradientVisible(Object gradientVisible);

          @JsProperty
          void setLineColor(
              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                      .MultiSeriesListFieldType.LineColorFieldType
                  lineColor);

          @JsProperty
          void setLinesVisible(
              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                      .MultiSeriesListFieldType.LinesVisibleFieldType
                  linesVisible);

          @JsProperty
          void setName(String name);

          @JsProperty
          void setPlotStyle(double plotStyle);

          @JsProperty
          void setPointColor(Object pointColor);

          @JsProperty
          void setPointLabel(Object pointLabel);

          @JsProperty
          void setPointLabelFormat(Object pointLabelFormat);

          @JsProperty
          void setPointShape(Object pointShape);

          @JsProperty
          void setPointSize(
              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                      .MultiSeriesListFieldType.PointSizeFieldType
                  pointSize);

          @JsProperty
          void setPointsVisible(Object pointsVisible);

          @JsProperty
          void setXToolTipPattern(Object xToolTipPattern);

          @JsProperty
          void setYToolTipPattern(Object yToolTipPattern);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface SeriesListFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface DataSourcesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface OneClickFieldType {
              @JsOverlay
              static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType
                      .OneClickFieldType
                  create() {
                return Js.uncheckedCast(JsPropertyMap.of());
              }

              @JsProperty
              JsArray<String> getColumnTypesList();

              @JsProperty
              JsArray<String> getColumnsList();

              @JsProperty
              boolean isRequireAllFiltersToDisplay();

              @JsProperty
              void setColumnTypesList(JsArray<String> columnTypesList);

              @JsOverlay
              default void setColumnTypesList(String[] columnTypesList) {
                setColumnTypesList(Js.<JsArray<String>>uncheckedCast(columnTypesList));
              }

              @JsProperty
              void setColumnsList(JsArray<String> columnsList);

              @JsOverlay
              default void setColumnsList(String[] columnsList) {
                setColumnsList(Js.<JsArray<String>>uncheckedCast(columnsList));
              }

              @JsProperty
              void setRequireAllFiltersToDisplay(boolean requireAllFiltersToDisplay);
            }

            @JsOverlay
            static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getAxisId();

            @JsProperty
            String getColumnName();

            @JsProperty
            String getColumnType();

            @JsProperty
            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                    .SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType
                getOneClick();

            @JsProperty
            double getTableId();

            @JsProperty
            double getTableMapId();

            @JsProperty
            double getType();

            @JsProperty
            void setAxisId(String axisId);

            @JsProperty
            void setColumnName(String columnName);

            @JsProperty
            void setColumnType(String columnType);

            @JsProperty
            void setOneClick(
                FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                        .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType
                        .OneClickFieldType
                    oneClick);

            @JsProperty
            void setTableId(double tableId);

            @JsProperty
            void setTableMapId(double tableMapId);

            @JsProperty
            void setType(double type);
          }

          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                  .ChartsListFieldType.SeriesListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<
                  FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                      .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType>
              getDataSourcesList();

          @JsProperty
          String getLineColor();

          @JsProperty
          String getName();

          @JsProperty
          double getPlotStyle();

          @JsProperty
          String getPointLabelFormat();

          @JsProperty
          String getShape();

          @JsProperty
          String getShapeColor();

          @JsProperty
          String getShapeLabel();

          @JsProperty
          double getShapeSize();

          @JsProperty
          String getXToolTipPattern();

          @JsProperty
          String getYToolTipPattern();

          @JsProperty
          boolean isGradientVisible();

          @JsProperty
          boolean isLinesVisible();

          @JsProperty
          boolean isShapesVisible();

          @JsOverlay
          default void setDataSourcesList(
              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                          .SeriesListFieldType.DataSourcesListFieldType
                      []
                  dataSourcesList) {
            setDataSourcesList(
                Js
                    .<JsArray<
                            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                                .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType>>
                        uncheckedCast(dataSourcesList));
          }

          @JsProperty
          void setDataSourcesList(
              JsArray<
                      FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                          .ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType>
                  dataSourcesList);

          @JsProperty
          void setGradientVisible(boolean gradientVisible);

          @JsProperty
          void setLineColor(String lineColor);

          @JsProperty
          void setLinesVisible(boolean linesVisible);

          @JsProperty
          void setName(String name);

          @JsProperty
          void setPlotStyle(double plotStyle);

          @JsProperty
          void setPointLabelFormat(String pointLabelFormat);

          @JsProperty
          void setShape(String shape);

          @JsProperty
          void setShapeColor(String shapeColor);

          @JsProperty
          void setShapeLabel(String shapeLabel);

          @JsProperty
          void setShapeSize(double shapeSize);

          @JsProperty
          void setShapesVisible(boolean shapesVisible);

          @JsProperty
          void setXToolTipPattern(String xToolTipPattern);

          @JsProperty
          void setYToolTipPattern(String yToolTipPattern);
        }

        @JsOverlay
        static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<
                FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.AxesListFieldType>
            getAxesList();

        @JsProperty
        double getChartType();

        @JsProperty
        double getColspan();

        @JsProperty
        String getLegendColor();

        @JsProperty
        String getLegendFont();

        @JsProperty
        JsArray<
                FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.MultiSeriesListFieldType>
            getMultiSeriesList();

        @JsProperty
        double getRowspan();

        @JsProperty
        JsArray<
                FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .ChartsListFieldType.SeriesListFieldType>
            getSeriesList();

        @JsProperty
        String getTitle();

        @JsProperty
        String getTitleColor();

        @JsProperty
        String getTitleFont();

        @JsProperty
        boolean isIs3d();

        @JsProperty
        boolean isShowLegend();

        @JsOverlay
        default void setAxesList(
            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                        .AxesListFieldType
                    []
                axesList) {
          setAxesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                              .ChartsListFieldType.AxesListFieldType>>
                      uncheckedCast(axesList));
        }

        @JsProperty
        void setAxesList(
            JsArray<
                    FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                        .ChartsListFieldType.AxesListFieldType>
                axesList);

        @JsProperty
        void setChartType(double chartType);

        @JsProperty
        void setColspan(double colspan);

        @JsProperty
        void setIs3d(boolean is3d);

        @JsProperty
        void setLegendColor(String legendColor);

        @JsProperty
        void setLegendFont(String legendFont);

        @JsProperty
        void setMultiSeriesList(
            JsArray<
                    FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                        .ChartsListFieldType.MultiSeriesListFieldType>
                multiSeriesList);

        @JsOverlay
        default void setMultiSeriesList(
            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                        .MultiSeriesListFieldType
                    []
                multiSeriesList) {
          setMultiSeriesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                              .ChartsListFieldType.MultiSeriesListFieldType>>
                      uncheckedCast(multiSeriesList));
        }

        @JsProperty
        void setRowspan(double rowspan);

        @JsProperty
        void setSeriesList(
            JsArray<
                    FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                        .ChartsListFieldType.SeriesListFieldType>
                seriesList);

        @JsOverlay
        default void setSeriesList(
            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType
                        .SeriesListFieldType
                    []
                seriesList) {
          setSeriesList(
              Js
                  .<JsArray<
                          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                              .ChartsListFieldType.SeriesListFieldType>>
                      uncheckedCast(seriesList));
        }

        @JsProperty
        void setShowLegend(boolean showLegend);

        @JsProperty
        void setTitle(String title);

        @JsProperty
        void setTitleColor(String titleColor);

        @JsProperty
        void setTitleFont(String titleFont);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface TablesListFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetSchemaHeaderUnionType {
          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                  .TablesListFieldType.GetSchemaHeaderUnionType
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

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ResultIdFieldType {
          @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
          public interface TicketFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetTicketUnionType {
              @JsOverlay
              static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                      .TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType
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
            static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                    .TablesListFieldType.ResultIdFieldType.TicketFieldType
                create() {
              return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType
                    .ResultIdFieldType.TicketFieldType.GetTicketUnionType
                getTicket();

            @JsProperty
            void setTicket(
                FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                        .TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType
                    ticket);

            @JsOverlay
            default void setTicket(String ticket) {
              setTicket(
                  Js
                      .<FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                              .TablesListFieldType.ResultIdFieldType.TicketFieldType
                              .GetTicketUnionType>
                          uncheckedCast(ticket));
            }

            @JsOverlay
            default void setTicket(Uint8Array ticket) {
              setTicket(
                  Js
                      .<FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                              .TablesListFieldType.ResultIdFieldType.TicketFieldType
                              .GetTicketUnionType>
                          uncheckedCast(ticket));
            }
          }

          @JsOverlay
          static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                  .TablesListFieldType.ResultIdFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          double getBatchOffset();

          @JsProperty
          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType
                  .ResultIdFieldType.TicketFieldType
              getTicket();

          @JsProperty
          void setBatchOffset(double batchOffset);

          @JsProperty
          void setTicket(
              FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType
                      .ResultIdFieldType.TicketFieldType
                  ticket);
        }

        @JsOverlay
        static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getErrorInfo();

        @JsProperty
        FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType
                .ResultIdFieldType
            getResultId();

        @JsProperty
        FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType
                .GetSchemaHeaderUnionType
            getSchemaHeader();

        @JsProperty
        String getSize();

        @JsProperty
        boolean isIsStatic();

        @JsProperty
        boolean isSuccess();

        @JsProperty
        void setErrorInfo(String errorInfo);

        @JsProperty
        void setIsStatic(boolean isStatic);

        @JsProperty
        void setResultId(
            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType
                    .ResultIdFieldType
                resultId);

        @JsProperty
        void setSchemaHeader(
            FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType
                    .GetSchemaHeaderUnionType
                schemaHeader);

        @JsOverlay
        default void setSchemaHeader(String schemaHeader) {
          setSchemaHeader(
              Js
                  .<FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                          .TablesListFieldType.GetSchemaHeaderUnionType>
                      uncheckedCast(schemaHeader));
        }

        @JsOverlay
        default void setSchemaHeader(Uint8Array schemaHeader) {
          setSchemaHeader(
              Js
                  .<FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                          .TablesListFieldType.GetSchemaHeaderUnionType>
                      uncheckedCast(schemaHeader));
        }

        @JsProperty
        void setSize(String size);

        @JsProperty
        void setSuccess(boolean success);
      }

      @JsOverlay
      static FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType>
          getChartsList();

      @JsProperty
      double getCols();

      @JsProperty
      JsArray<String> getErrorsList();

      @JsProperty
      double getRows();

      @JsProperty
      JsArray<FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType>
          getTablesList();

      @JsProperty
      String getTitle();

      @JsProperty
      String getTitleColor();

      @JsProperty
      String getTitleFont();

      @JsProperty
      double getUpdateInterval();

      @JsOverlay
      default void setChartsList(
          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.ChartsListFieldType[]
              chartsList) {
        setChartsList(
            Js
                .<JsArray<
                        FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                            .ChartsListFieldType>>
                    uncheckedCast(chartsList));
      }

      @JsProperty
      void setChartsList(
          JsArray<
                  FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
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
      void setTablesList(
          JsArray<
                  FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                      .TablesListFieldType>
              tablesList);

      @JsOverlay
      default void setTablesList(
          FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType.TablesListFieldType[]
              tablesList) {
        setTablesList(
            Js
                .<JsArray<
                        FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType
                            .TablesListFieldType>>
                    uncheckedCast(tablesList));
      }

      @JsProperty
      void setTitle(String title);

      @JsProperty
      void setTitleColor(String titleColor);

      @JsProperty
      void setTitleFont(String titleFont);

      @JsProperty
      void setUpdateInterval(double updateInterval);
    }

    @JsOverlay
    static FetchFigureResponse.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType getFigureDescriptor();

    @JsProperty
    void setFigureDescriptor(
        FetchFigureResponse.ToObjectReturnType0.FigureDescriptorFieldType figureDescriptor);
  }

  public static native FetchFigureResponse deserializeBinary(Uint8Array bytes);

  public static native FetchFigureResponse deserializeBinaryFromReader(
      FetchFigureResponse message, Object reader);

  public static native void serializeBinaryToWriter(FetchFigureResponse message, Object writer);

  public static native FetchFigureResponse.ToObjectReturnType toObject(
      boolean includeInstance, FetchFigureResponse msg);

  public native void clearFigureDescriptor();

  public native FigureDescriptor getFigureDescriptor();

  public native boolean hasFigureDescriptor();

  public native Uint8Array serializeBinary();

  public native void setFigureDescriptor();

  public native void setFigureDescriptor(FigureDescriptor value);

  public native FetchFigureResponse.ToObjectReturnType0 toObject();

  public native FetchFigureResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
