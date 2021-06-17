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
    public interface AxesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface BusinessCalendarDescriptorFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BusinessPeriodsListFieldType {
          @JsOverlay
          static ChartDescriptor.ToObjectReturnType.AxesListFieldType
                  .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType
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
            static ChartDescriptor.ToObjectReturnType.AxesListFieldType
                    .BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType
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
          static ChartDescriptor.ToObjectReturnType.AxesListFieldType
                  .BusinessCalendarDescriptorFieldType.HolidaysListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<Object> getBusinessPeriodsList();

          @JsProperty
          ChartDescriptor.ToObjectReturnType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                  .HolidaysListFieldType.DateFieldType
              getDate();

          @JsProperty
          void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

          @JsOverlay
          default void setBusinessPeriodsList(Object[] businessPeriodsList) {
            setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
          }

          @JsProperty
          void setDate(
              ChartDescriptor.ToObjectReturnType.AxesListFieldType
                      .BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType
                  date);
        }

        @JsOverlay
        static ChartDescriptor.ToObjectReturnType.AxesListFieldType
                .BusinessCalendarDescriptorFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Double> getBusinessDaysList();

        @JsProperty
        JsArray<
                ChartDescriptor.ToObjectReturnType.AxesListFieldType
                    .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>
            getBusinessPeriodsList();

        @JsProperty
        JsArray<
                ChartDescriptor.ToObjectReturnType.AxesListFieldType
                    .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>
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
            ChartDescriptor.ToObjectReturnType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                        .BusinessPeriodsListFieldType
                    []
                businessPeriodsList) {
          setBusinessPeriodsList(
              Js
                  .<JsArray<
                          ChartDescriptor.ToObjectReturnType.AxesListFieldType
                              .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>>
                      uncheckedCast(businessPeriodsList));
        }

        @JsProperty
        void setBusinessPeriodsList(
            JsArray<
                    ChartDescriptor.ToObjectReturnType.AxesListFieldType
                        .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>
                businessPeriodsList);

        @JsOverlay
        default void setHolidaysList(
            ChartDescriptor.ToObjectReturnType.AxesListFieldType.BusinessCalendarDescriptorFieldType
                        .HolidaysListFieldType
                    []
                holidaysList) {
          setHolidaysList(
              Js
                  .<JsArray<
                          ChartDescriptor.ToObjectReturnType.AxesListFieldType
                              .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>>
                      uncheckedCast(holidaysList));
        }

        @JsProperty
        void setHolidaysList(
            JsArray<
                    ChartDescriptor.ToObjectReturnType.AxesListFieldType
                        .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>
                holidaysList);

        @JsProperty
        void setName(String name);

        @JsProperty
        void setTimeZone(String timeZone);
      }

      @JsOverlay
      static ChartDescriptor.ToObjectReturnType.AxesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ChartDescriptor.ToObjectReturnType.AxesListFieldType.BusinessCalendarDescriptorFieldType
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
          ChartDescriptor.ToObjectReturnType.AxesListFieldType.BusinessCalendarDescriptorFieldType
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
        static ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.DataSourcesListFieldType
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
        static ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.LineColorFieldType
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
        static ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.LinesVisibleFieldType
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
        static ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.PointSizeFieldType
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
      static ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.DataSourcesListFieldType>
          getDataSourcesList();

      @JsProperty
      Object getGradientVisible();

      @JsProperty
      ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.LineColorFieldType getLineColor();

      @JsProperty
      ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.LinesVisibleFieldType
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
      ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.PointSizeFieldType getPointSize();

      @JsProperty
      Object getPointsVisible();

      @JsProperty
      Object getXToolTipPattern();

      @JsProperty
      Object getYToolTipPattern();

      @JsOverlay
      default void setDataSourcesList(
          ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.DataSourcesListFieldType[]
              dataSourcesList) {
        setDataSourcesList(
            Js
                .<JsArray<
                        ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType
                            .DataSourcesListFieldType>>
                    uncheckedCast(dataSourcesList));
      }

      @JsProperty
      void setDataSourcesList(
          JsArray<
                  ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType
                      .DataSourcesListFieldType>
              dataSourcesList);

      @JsProperty
      void setGradientVisible(Object gradientVisible);

      @JsProperty
      void setLineColor(
          ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.LineColorFieldType lineColor);

      @JsProperty
      void setLinesVisible(
          ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.LinesVisibleFieldType
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
          ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType.PointSizeFieldType pointSize);

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
          static ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DataSourcesListFieldType
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
        static ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DataSourcesListFieldType
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
        ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DataSourcesListFieldType
                .OneClickFieldType
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
            ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DataSourcesListFieldType
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
      static ChartDescriptor.ToObjectReturnType.SeriesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DataSourcesListFieldType>
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
          ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DataSourcesListFieldType[]
              dataSourcesList) {
        setDataSourcesList(
            Js
                .<JsArray<
                        ChartDescriptor.ToObjectReturnType.SeriesListFieldType
                            .DataSourcesListFieldType>>
                    uncheckedCast(dataSourcesList));
      }

      @JsProperty
      void setDataSourcesList(
          JsArray<ChartDescriptor.ToObjectReturnType.SeriesListFieldType.DataSourcesListFieldType>
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
    static ChartDescriptor.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType.AxesListFieldType> getAxesList();

    @JsProperty
    double getChartType();

    @JsProperty
    double getColspan();

    @JsProperty
    String getLegendColor();

    @JsProperty
    String getLegendFont();

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType> getMultiSeriesList();

    @JsProperty
    double getRowspan();

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType.SeriesListFieldType> getSeriesList();

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
    default void setAxesList(ChartDescriptor.ToObjectReturnType.AxesListFieldType[] axesList) {
      setAxesList(
          Js.<JsArray<ChartDescriptor.ToObjectReturnType.AxesListFieldType>>uncheckedCast(
              axesList));
    }

    @JsProperty
    void setAxesList(JsArray<ChartDescriptor.ToObjectReturnType.AxesListFieldType> axesList);

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
        JsArray<ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType> multiSeriesList);

    @JsOverlay
    default void setMultiSeriesList(
        ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType[] multiSeriesList) {
      setMultiSeriesList(
          Js.<JsArray<ChartDescriptor.ToObjectReturnType.MultiSeriesListFieldType>>uncheckedCast(
              multiSeriesList));
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
    void setShowLegend(boolean showLegend);

    @JsProperty
    void setTitle(String title);

    @JsProperty
    void setTitleColor(String titleColor);

    @JsProperty
    void setTitleFont(String titleFont);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AxesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface BusinessCalendarDescriptorFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BusinessPeriodsListFieldType {
          @JsOverlay
          static ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                  .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType
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
            static ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                    .BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType
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
          static ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                  .BusinessCalendarDescriptorFieldType.HolidaysListFieldType
              create() {
            return Js.uncheckedCast(JsPropertyMap.of());
          }

          @JsProperty
          JsArray<Object> getBusinessPeriodsList();

          @JsProperty
          ChartDescriptor.ToObjectReturnType0.AxesListFieldType.BusinessCalendarDescriptorFieldType
                  .HolidaysListFieldType.DateFieldType
              getDate();

          @JsProperty
          void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

          @JsOverlay
          default void setBusinessPeriodsList(Object[] businessPeriodsList) {
            setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
          }

          @JsProperty
          void setDate(
              ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                      .BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType
                  date);
        }

        @JsOverlay
        static ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                .BusinessCalendarDescriptorFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<Double> getBusinessDaysList();

        @JsProperty
        JsArray<
                ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                    .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>
            getBusinessPeriodsList();

        @JsProperty
        JsArray<
                ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                    .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>
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
            ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                        .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType
                    []
                businessPeriodsList) {
          setBusinessPeriodsList(
              Js
                  .<JsArray<
                          ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                              .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>>
                      uncheckedCast(businessPeriodsList));
        }

        @JsProperty
        void setBusinessPeriodsList(
            JsArray<
                    ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                        .BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>
                businessPeriodsList);

        @JsOverlay
        default void setHolidaysList(
            ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                        .BusinessCalendarDescriptorFieldType.HolidaysListFieldType
                    []
                holidaysList) {
          setHolidaysList(
              Js
                  .<JsArray<
                          ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                              .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>>
                      uncheckedCast(holidaysList));
        }

        @JsProperty
        void setHolidaysList(
            JsArray<
                    ChartDescriptor.ToObjectReturnType0.AxesListFieldType
                        .BusinessCalendarDescriptorFieldType.HolidaysListFieldType>
                holidaysList);

        @JsProperty
        void setName(String name);

        @JsProperty
        void setTimeZone(String timeZone);
      }

      @JsOverlay
      static ChartDescriptor.ToObjectReturnType0.AxesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ChartDescriptor.ToObjectReturnType0.AxesListFieldType.BusinessCalendarDescriptorFieldType
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
          ChartDescriptor.ToObjectReturnType0.AxesListFieldType.BusinessCalendarDescriptorFieldType
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
        static ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.DataSourcesListFieldType
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
        static ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.LineColorFieldType
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
        static ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.LinesVisibleFieldType
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
        static ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.PointSizeFieldType
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
      static ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.DataSourcesListFieldType>
          getDataSourcesList();

      @JsProperty
      Object getGradientVisible();

      @JsProperty
      ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.LineColorFieldType
          getLineColor();

      @JsProperty
      ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.LinesVisibleFieldType
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
      ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.PointSizeFieldType
          getPointSize();

      @JsProperty
      Object getPointsVisible();

      @JsProperty
      Object getXToolTipPattern();

      @JsProperty
      Object getYToolTipPattern();

      @JsOverlay
      default void setDataSourcesList(
          ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.DataSourcesListFieldType[]
              dataSourcesList) {
        setDataSourcesList(
            Js
                .<JsArray<
                        ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType
                            .DataSourcesListFieldType>>
                    uncheckedCast(dataSourcesList));
      }

      @JsProperty
      void setDataSourcesList(
          JsArray<
                  ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType
                      .DataSourcesListFieldType>
              dataSourcesList);

      @JsProperty
      void setGradientVisible(Object gradientVisible);

      @JsProperty
      void setLineColor(
          ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.LineColorFieldType
              lineColor);

      @JsProperty
      void setLinesVisible(
          ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.LinesVisibleFieldType
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
          ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType.PointSizeFieldType
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
          static ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DataSourcesListFieldType
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
        static ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DataSourcesListFieldType
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
        ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DataSourcesListFieldType
                .OneClickFieldType
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
            ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DataSourcesListFieldType
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
      static ChartDescriptor.ToObjectReturnType0.SeriesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DataSourcesListFieldType>
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
          ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DataSourcesListFieldType[]
              dataSourcesList) {
        setDataSourcesList(
            Js
                .<JsArray<
                        ChartDescriptor.ToObjectReturnType0.SeriesListFieldType
                            .DataSourcesListFieldType>>
                    uncheckedCast(dataSourcesList));
      }

      @JsProperty
      void setDataSourcesList(
          JsArray<ChartDescriptor.ToObjectReturnType0.SeriesListFieldType.DataSourcesListFieldType>
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
    static ChartDescriptor.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType0.AxesListFieldType> getAxesList();

    @JsProperty
    double getChartType();

    @JsProperty
    double getColspan();

    @JsProperty
    String getLegendColor();

    @JsProperty
    String getLegendFont();

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType> getMultiSeriesList();

    @JsProperty
    double getRowspan();

    @JsProperty
    JsArray<ChartDescriptor.ToObjectReturnType0.SeriesListFieldType> getSeriesList();

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
    default void setAxesList(ChartDescriptor.ToObjectReturnType0.AxesListFieldType[] axesList) {
      setAxesList(
          Js.<JsArray<ChartDescriptor.ToObjectReturnType0.AxesListFieldType>>uncheckedCast(
              axesList));
    }

    @JsProperty
    void setAxesList(JsArray<ChartDescriptor.ToObjectReturnType0.AxesListFieldType> axesList);

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
        JsArray<ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType> multiSeriesList);

    @JsOverlay
    default void setMultiSeriesList(
        ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType[] multiSeriesList) {
      setMultiSeriesList(
          Js.<JsArray<ChartDescriptor.ToObjectReturnType0.MultiSeriesListFieldType>>uncheckedCast(
              multiSeriesList));
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
    void setShowLegend(boolean showLegend);

    @JsProperty
    void setTitle(String title);

    @JsProperty
    void setTitleColor(String titleColor);

    @JsProperty
    void setTitleFont(String titleFont);
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

  public native MultiSeriesDescriptor addMultiSeries();

  public native MultiSeriesDescriptor addMultiSeries(MultiSeriesDescriptor value, double index);

  public native MultiSeriesDescriptor addMultiSeries(MultiSeriesDescriptor value);

  public native SeriesDescriptor addSeries();

  public native SeriesDescriptor addSeries(SeriesDescriptor value, double index);

  public native SeriesDescriptor addSeries(SeriesDescriptor value);

  public native void clearAxesList();

  public native void clearMultiSeriesList();

  public native void clearSeriesList();

  public native void clearTitle();

  public native JsArray<AxisDescriptor> getAxesList();

  public native int getChartType();

  public native int getColspan();

  public native boolean getIs3d();

  public native String getLegendColor();

  public native String getLegendFont();

  public native JsArray<MultiSeriesDescriptor> getMultiSeriesList();

  public native int getRowspan();

  public native JsArray<SeriesDescriptor> getSeriesList();

  public native boolean getShowLegend();

  public native String getTitle();

  public native String getTitleColor();

  public native String getTitleFont();

  public native boolean hasTitle();

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setAxesList(AxisDescriptor[] value) {
    setAxesList(Js.<JsArray<AxisDescriptor>>uncheckedCast(value));
  }

  public native void setAxesList(JsArray<AxisDescriptor> value);

  public native void setChartType(int value);

  public native void setColspan(int value);

  public native void setIs3d(boolean value);

  public native void setLegendColor(String value);

  public native void setLegendFont(String value);

  public native void setMultiSeriesList(JsArray<MultiSeriesDescriptor> value);

  @JsOverlay
  public final void setMultiSeriesList(MultiSeriesDescriptor[] value) {
    setMultiSeriesList(Js.<JsArray<MultiSeriesDescriptor>>uncheckedCast(value));
  }

  public native void setRowspan(int value);

  public native void setSeriesList(JsArray<SeriesDescriptor> value);

  @JsOverlay
  public final void setSeriesList(SeriesDescriptor[] value) {
    setSeriesList(Js.<JsArray<SeriesDescriptor>>uncheckedCast(value));
  }

  public native void setShowLegend(boolean value);

  public native void setTitle(String value);

  public native void setTitleColor(String value);

  public native void setTitleFont(String value);

  public native ChartDescriptor.ToObjectReturnType0 toObject();

  public native ChartDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
