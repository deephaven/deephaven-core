package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.ChartDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.SeriesPlotStyleMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.SourceTypeMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
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
            public interface AxesListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface BusinessCalendarDescriptorFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface BusinessPeriodsListFieldType {
                        @JsOverlay
                        static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType create() {
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
                            static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType create() {
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
                        static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getBusinessPeriodsList();

                        @JsProperty
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType getDate();

                        @JsProperty
                        void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

                        @JsOverlay
                        default void setBusinessPeriodsList(Object[] businessPeriodsList) {
                            setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
                        }

                        @JsProperty
                        void setDate(
                                FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType date);
                    }

                    @JsOverlay
                    static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    JsArray<Double> getBusinessDaysList();

                    @JsProperty
                    JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType> getBusinessPeriodsList();

                    @JsProperty
                    JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType> getHolidaysList();

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
                            FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType[] businessPeriodsList) {
                        setBusinessPeriodsList(
                                Js.<JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>>uncheckedCast(
                                        businessPeriodsList));
                    }

                    @JsProperty
                    void setBusinessPeriodsList(
                            JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType> businessPeriodsList);

                    @JsOverlay
                    default void setHolidaysList(
                            FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType[] holidaysList) {
                        setHolidaysList(
                                Js.<JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType>>uncheckedCast(
                                        holidaysList));
                    }

                    @JsProperty
                    void setHolidaysList(
                            JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType> holidaysList);

                    @JsProperty
                    void setName(String name);

                    @JsProperty
                    void setTimeZone(String timeZone);
                }

                @JsOverlay
                static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType getBusinessCalendarDescriptor();

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
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType businessCalendarDescriptor);

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
                    static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType create() {
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
                    static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.LineColorFieldType create() {
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
                    static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.LinesVisibleFieldType create() {
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
                    static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.PointSizeFieldType create() {
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
                static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType> getDataSourcesList();

                @JsProperty
                Object getGradientVisible();

                @JsProperty
                FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.LineColorFieldType getLineColor();

                @JsProperty
                FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.LinesVisibleFieldType getLinesVisible();

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
                FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.PointSizeFieldType getPointSize();

                @JsProperty
                Object getPointsVisible();

                @JsProperty
                Object getXToolTipPattern();

                @JsProperty
                Object getYToolTipPattern();

                @JsOverlay
                default void setDataSourcesList(
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType[] dataSourcesList) {
                    setDataSourcesList(
                            Js.<JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType>>uncheckedCast(
                                    dataSourcesList));
                }

                @JsProperty
                void setDataSourcesList(
                        JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType> dataSourcesList);

                @JsProperty
                void setGradientVisible(Object gradientVisible);

                @JsProperty
                void setLineColor(
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.LineColorFieldType lineColor);

                @JsProperty
                void setLinesVisible(
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.LinesVisibleFieldType linesVisible);

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
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType.PointSizeFieldType pointSize);

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
                        static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType create() {
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
                    static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getAxisId();

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    String getColumnType();

                    @JsProperty
                    FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType getOneClick();

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
                            FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType oneClick);

                    @JsProperty
                    void setTableId(double tableId);

                    @JsProperty
                    void setTableMapId(double tableMapId);

                    @JsProperty
                    void setType(double type);
                }

                @JsOverlay
                static FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType> getDataSourcesList();

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
                        FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType[] dataSourcesList) {
                    setDataSourcesList(
                            Js.<JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType>>uncheckedCast(
                                    dataSourcesList));
                }

                @JsProperty
                void setDataSourcesList(
                        JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType> dataSourcesList);

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
            static FigureDescriptor.ToObjectReturnType.ChartsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType> getAxesList();

            @JsProperty
            double getChartType();

            @JsProperty
            double getColspan();

            @JsProperty
            String getLegendColor();

            @JsProperty
            String getLegendFont();

            @JsProperty
            JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType> getMultiSeriesList();

            @JsProperty
            double getRowspan();

            @JsProperty
            JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType> getSeriesList();

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
                    FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType[] axesList) {
                setAxesList(
                        Js.<JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType>>uncheckedCast(
                                axesList));
            }

            @JsProperty
            void setAxesList(
                    JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.AxesListFieldType> axesList);

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
                    JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType> multiSeriesList);

            @JsOverlay
            default void setMultiSeriesList(
                    FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType[] multiSeriesList) {
                setMultiSeriesList(
                        Js.<JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.MultiSeriesListFieldType>>uncheckedCast(
                                multiSeriesList));
            }

            @JsProperty
            void setRowspan(double rowspan);

            @JsProperty
            void setSeriesList(
                    JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType> seriesList);

            @JsOverlay
            default void setSeriesList(
                    FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType[] seriesList) {
                setSeriesList(
                        Js.<JsArray<FigureDescriptor.ToObjectReturnType.ChartsListFieldType.SeriesListFieldType>>uncheckedCast(
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
        public interface TablesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetSchemaHeaderUnionType {
                @JsOverlay
                static FigureDescriptor.ToObjectReturnType.TablesListFieldType.GetSchemaHeaderUnionType of(
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

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ResultIdFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType of(
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
                    static FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType.TicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getBatchOffset();

                @JsProperty
                FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType.TicketFieldType getTicket();

                @JsProperty
                void setBatchOffset(double batchOffset);

                @JsProperty
                void setTicket(
                        FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType.TicketFieldType ticket);
            }

            @JsOverlay
            static FigureDescriptor.ToObjectReturnType.TablesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getErrorInfo();

            @JsProperty
            FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType getResultId();

            @JsProperty
            FigureDescriptor.ToObjectReturnType.TablesListFieldType.GetSchemaHeaderUnionType getSchemaHeader();

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
                    FigureDescriptor.ToObjectReturnType.TablesListFieldType.ResultIdFieldType resultId);

            @JsProperty
            void setSchemaHeader(
                    FigureDescriptor.ToObjectReturnType.TablesListFieldType.GetSchemaHeaderUnionType schemaHeader);

            @JsOverlay
            default void setSchemaHeader(String schemaHeader) {
                setSchemaHeader(
                        Js.<FigureDescriptor.ToObjectReturnType.TablesListFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                schemaHeader));
            }

            @JsOverlay
            default void setSchemaHeader(Uint8Array schemaHeader) {
                setSchemaHeader(
                        Js.<FigureDescriptor.ToObjectReturnType.TablesListFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                schemaHeader));
            }

            @JsProperty
            void setSize(String size);

            @JsProperty
            void setSuccess(boolean success);
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
        JsArray<FigureDescriptor.ToObjectReturnType.TablesListFieldType> getTablesList();

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
        void setTablesList(JsArray<FigureDescriptor.ToObjectReturnType.TablesListFieldType> tablesList);

        @JsOverlay
        default void setTablesList(
                FigureDescriptor.ToObjectReturnType.TablesListFieldType[] tablesList) {
            setTablesList(
                    Js.<JsArray<FigureDescriptor.ToObjectReturnType.TablesListFieldType>>uncheckedCast(
                            tablesList));
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ChartsListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface AxesListFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface BusinessCalendarDescriptorFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface BusinessPeriodsListFieldType {
                        @JsOverlay
                        static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType create() {
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
                            static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType create() {
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
                        static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType create() {
                            return Js.uncheckedCast(JsPropertyMap.of());
                        }

                        @JsProperty
                        JsArray<Object> getBusinessPeriodsList();

                        @JsProperty
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType getDate();

                        @JsProperty
                        void setBusinessPeriodsList(JsArray<Object> businessPeriodsList);

                        @JsOverlay
                        default void setBusinessPeriodsList(Object[] businessPeriodsList) {
                            setBusinessPeriodsList(Js.<JsArray<Object>>uncheckedCast(businessPeriodsList));
                        }

                        @JsProperty
                        void setDate(
                                FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType.DateFieldType date);
                    }

                    @JsOverlay
                    static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    JsArray<Double> getBusinessDaysList();

                    @JsProperty
                    JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType> getBusinessPeriodsList();

                    @JsProperty
                    JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType> getHolidaysList();

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
                            FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType[] businessPeriodsList) {
                        setBusinessPeriodsList(
                                Js.<JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType>>uncheckedCast(
                                        businessPeriodsList));
                    }

                    @JsProperty
                    void setBusinessPeriodsList(
                            JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.BusinessPeriodsListFieldType> businessPeriodsList);

                    @JsOverlay
                    default void setHolidaysList(
                            FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType[] holidaysList) {
                        setHolidaysList(
                                Js.<JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType>>uncheckedCast(
                                        holidaysList));
                    }

                    @JsProperty
                    void setHolidaysList(
                            JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType.HolidaysListFieldType> holidaysList);

                    @JsProperty
                    void setName(String name);

                    @JsProperty
                    void setTimeZone(String timeZone);
                }

                @JsOverlay
                static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType getBusinessCalendarDescriptor();

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
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType.BusinessCalendarDescriptorFieldType businessCalendarDescriptor);

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
                    static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType create() {
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
                    static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.LineColorFieldType create() {
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
                    static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.LinesVisibleFieldType create() {
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
                    static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.PointSizeFieldType create() {
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
                static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType> getDataSourcesList();

                @JsProperty
                Object getGradientVisible();

                @JsProperty
                FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.LineColorFieldType getLineColor();

                @JsProperty
                FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.LinesVisibleFieldType getLinesVisible();

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
                FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.PointSizeFieldType getPointSize();

                @JsProperty
                Object getPointsVisible();

                @JsProperty
                Object getXToolTipPattern();

                @JsProperty
                Object getYToolTipPattern();

                @JsOverlay
                default void setDataSourcesList(
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType[] dataSourcesList) {
                    setDataSourcesList(
                            Js.<JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType>>uncheckedCast(
                                    dataSourcesList));
                }

                @JsProperty
                void setDataSourcesList(
                        JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.DataSourcesListFieldType> dataSourcesList);

                @JsProperty
                void setGradientVisible(Object gradientVisible);

                @JsProperty
                void setLineColor(
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.LineColorFieldType lineColor);

                @JsProperty
                void setLinesVisible(
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.LinesVisibleFieldType linesVisible);

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
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType.PointSizeFieldType pointSize);

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
                        static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType create() {
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
                    static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    String getAxisId();

                    @JsProperty
                    String getColumnName();

                    @JsProperty
                    String getColumnType();

                    @JsProperty
                    FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType getOneClick();

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
                            FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType.OneClickFieldType oneClick);

                    @JsProperty
                    void setTableId(double tableId);

                    @JsProperty
                    void setTableMapId(double tableMapId);

                    @JsProperty
                    void setType(double type);
                }

                @JsOverlay
                static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType> getDataSourcesList();

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
                        FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType[] dataSourcesList) {
                    setDataSourcesList(
                            Js.<JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType>>uncheckedCast(
                                    dataSourcesList));
                }

                @JsProperty
                void setDataSourcesList(
                        JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType.DataSourcesListFieldType> dataSourcesList);

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
            static FigureDescriptor.ToObjectReturnType0.ChartsListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType> getAxesList();

            @JsProperty
            double getChartType();

            @JsProperty
            double getColspan();

            @JsProperty
            String getLegendColor();

            @JsProperty
            String getLegendFont();

            @JsProperty
            JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType> getMultiSeriesList();

            @JsProperty
            double getRowspan();

            @JsProperty
            JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType> getSeriesList();

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
                    FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType[] axesList) {
                setAxesList(
                        Js.<JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType>>uncheckedCast(
                                axesList));
            }

            @JsProperty
            void setAxesList(
                    JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.AxesListFieldType> axesList);

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
                    JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType> multiSeriesList);

            @JsOverlay
            default void setMultiSeriesList(
                    FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType[] multiSeriesList) {
                setMultiSeriesList(
                        Js.<JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.MultiSeriesListFieldType>>uncheckedCast(
                                multiSeriesList));
            }

            @JsProperty
            void setRowspan(double rowspan);

            @JsProperty
            void setSeriesList(
                    JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType> seriesList);

            @JsOverlay
            default void setSeriesList(
                    FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType[] seriesList) {
                setSeriesList(
                        Js.<JsArray<FigureDescriptor.ToObjectReturnType0.ChartsListFieldType.SeriesListFieldType>>uncheckedCast(
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
        public interface TablesListFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface GetSchemaHeaderUnionType {
                @JsOverlay
                static FigureDescriptor.ToObjectReturnType0.TablesListFieldType.GetSchemaHeaderUnionType of(
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

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface ResultIdFieldType {
                @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                public interface TicketFieldType {
                    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
                    public interface GetTicketUnionType {
                        @JsOverlay
                        static FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType of(
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
                    static FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType.TicketFieldType create() {
                        return Js.uncheckedCast(JsPropertyMap.of());
                    }

                    @JsProperty
                    FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType getTicket();

                    @JsProperty
                    void setTicket(
                            FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType ticket);

                    @JsOverlay
                    default void setTicket(String ticket) {
                        setTicket(
                                Js.<FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }

                    @JsOverlay
                    default void setTicket(Uint8Array ticket) {
                        setTicket(
                                Js.<FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType.TicketFieldType.GetTicketUnionType>uncheckedCast(
                                        ticket));
                    }
                }

                @JsOverlay
                static FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getBatchOffset();

                @JsProperty
                FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType.TicketFieldType getTicket();

                @JsProperty
                void setBatchOffset(double batchOffset);

                @JsProperty
                void setTicket(
                        FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType.TicketFieldType ticket);
            }

            @JsOverlay
            static FigureDescriptor.ToObjectReturnType0.TablesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getErrorInfo();

            @JsProperty
            FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType getResultId();

            @JsProperty
            FigureDescriptor.ToObjectReturnType0.TablesListFieldType.GetSchemaHeaderUnionType getSchemaHeader();

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
                    FigureDescriptor.ToObjectReturnType0.TablesListFieldType.ResultIdFieldType resultId);

            @JsProperty
            void setSchemaHeader(
                    FigureDescriptor.ToObjectReturnType0.TablesListFieldType.GetSchemaHeaderUnionType schemaHeader);

            @JsOverlay
            default void setSchemaHeader(String schemaHeader) {
                setSchemaHeader(
                        Js.<FigureDescriptor.ToObjectReturnType0.TablesListFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                schemaHeader));
            }

            @JsOverlay
            default void setSchemaHeader(Uint8Array schemaHeader) {
                setSchemaHeader(
                        Js.<FigureDescriptor.ToObjectReturnType0.TablesListFieldType.GetSchemaHeaderUnionType>uncheckedCast(
                                schemaHeader));
            }

            @JsProperty
            void setSize(String size);

            @JsProperty
            void setSuccess(boolean success);
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
        JsArray<FigureDescriptor.ToObjectReturnType0.TablesListFieldType> getTablesList();

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
        void setTablesList(
                JsArray<FigureDescriptor.ToObjectReturnType0.TablesListFieldType> tablesList);

        @JsOverlay
        default void setTablesList(
                FigureDescriptor.ToObjectReturnType0.TablesListFieldType[] tablesList) {
            setTablesList(
                    Js.<JsArray<FigureDescriptor.ToObjectReturnType0.TablesListFieldType>>uncheckedCast(
                            tablesList));
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

    public native ExportedTableCreationResponse addTables();

    public native ExportedTableCreationResponse addTables(
            ExportedTableCreationResponse value, double index);

    public native ExportedTableCreationResponse addTables(ExportedTableCreationResponse value);

    public native void clearChartsList();

    public native void clearErrorsList();

    public native void clearTablesList();

    public native void clearTitle();

    public native JsArray<ChartDescriptor> getChartsList();

    public native int getCols();

    public native JsArray<String> getErrorsList();

    public native int getRows();

    public native JsArray<ExportedTableCreationResponse> getTablesList();

    public native String getTitle();

    public native String getTitleColor();

    public native String getTitleFont();

    public native double getUpdateInterval();

    public native boolean hasTitle();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setChartsList(ChartDescriptor[] value) {
        setChartsList(Js.<JsArray<ChartDescriptor>>uncheckedCast(value));
    }

    public native void setChartsList(JsArray<ChartDescriptor> value);

    public native void setCols(int value);

    public native void setErrorsList(JsArray<String> value);

    @JsOverlay
    public final void setErrorsList(String[] value) {
        setErrorsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native void setRows(int value);

    @JsOverlay
    public final void setTablesList(ExportedTableCreationResponse[] value) {
        setTablesList(Js.<JsArray<ExportedTableCreationResponse>>uncheckedCast(value));
    }

    public native void setTablesList(JsArray<ExportedTableCreationResponse> value);

    public native void setTitle(String value);

    public native void setTitleColor(String value);

    public native void setTitleFont(String value);

    public native void setUpdateInterval(double value);

    public native FigureDescriptor.ToObjectReturnType0 toObject();

    public native FigureDescriptor.ToObjectReturnType0 toObject(boolean includeInstance);
}
