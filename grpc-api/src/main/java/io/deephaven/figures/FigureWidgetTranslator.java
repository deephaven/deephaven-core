package io.deephaven.figures;

import io.deephaven.api.Selectable;
import io.deephaven.db.plot.*;
import io.deephaven.db.plot.Font;
import io.deephaven.db.plot.axistransformations.AxisTransform;
import io.deephaven.db.plot.axistransformations.AxisTransformBusinessCalendar;
import io.deephaven.db.plot.datasets.AbstractDataSeries;
import io.deephaven.db.plot.datasets.category.AbstractCategoryDataSeries;
import io.deephaven.db.plot.datasets.category.CategoryDataSeriesMap;
import io.deephaven.db.plot.datasets.category.CategoryDataSeriesSwappableTableMap;
import io.deephaven.db.plot.datasets.category.CategoryDataSeriesTableMap;
import io.deephaven.db.plot.datasets.data.IndexableNumericData;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataSwappableTable;
import io.deephaven.db.plot.datasets.data.IndexableNumericDataTable;
import io.deephaven.db.plot.datasets.interval.IntervalXYDataSeriesArray;
import io.deephaven.db.plot.datasets.multiseries.AbstractMultiSeries;
import io.deephaven.db.plot.datasets.multiseries.AbstractTableMapHandleMultiSeries;
import io.deephaven.db.plot.datasets.multiseries.MultiCatSeries;
import io.deephaven.db.plot.datasets.multiseries.MultiXYSeries;
import io.deephaven.db.plot.datasets.ohlc.OHLCDataSeriesArray;
import io.deephaven.db.plot.datasets.xy.AbstractXYDataSeries;
import io.deephaven.db.plot.datasets.xy.XYDataSeriesArray;
import io.deephaven.db.plot.datasets.xyerrorbar.XYErrorBarDataSeriesArray;
import io.deephaven.db.plot.util.PlotUtils;
import io.deephaven.db.plot.util.tables.*;
import io.deephaven.db.plot.util.tables.TableHandle;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.table.TableServiceGrpcImpl;
import io.deephaven.gui.shape.JShapes;
import io.deephaven.gui.shape.NamedShape;
import io.deephaven.gui.shape.Shape;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor.*;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.BusinessPeriod;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.Holiday;
import io.deephaven.proto.backplane.script.grpc.FigureDescriptor.BusinessCalendarDescriptor.LocalDate;
import io.deephaven.util.calendar.BusinessCalendar;
import org.jetbrains.annotations.NotNull;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.awt.*;
import java.time.DayOfWeek;
import java.util.List;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

public class FigureWidgetTranslator {
    private static final DateTimeFormatter HOLIDAY_TIME_FORMAT = DateTimeFormat.forPattern("HH:mm");

    private final List<String> errorList = new ArrayList<>();
    private final Map<TableHandle, Integer> tablePositionMap = new HashMap<>();
    private final Map<TableMapHandle, Integer> tableMapPositionMap = new HashMap<>();

    private FigureWidgetTranslator() {
    }

    public static FigureDescriptor translate(FigureWidget figure, SessionState sessionState) {
        return new FigureWidgetTranslator().translateFigure(figure, sessionState);
    }
    private FigureDescriptor translateFigure(FigureWidget f, SessionState sessionState) {
        FigureDescriptor.Builder clientFigure = FigureDescriptor.newBuilder();
        BaseFigureImpl figure = f.getFigure();

        // translate tables first, so we can use them to look up tables as needed
        int i = 0;
        for (Map.Entry<Table, List<TableHandle>> entry : figure.getTableHandles().stream().collect(Collectors.groupingBy(TableHandle::getTable)).entrySet()) {
            Set<String> relevantColumns = entry.getValue().stream().map(TableHandle::getColumns).flatMap(Set::stream).collect(Collectors.toSet());
            Table table = entry.getKey().view(Selectable.from(relevantColumns));

            for (TableHandle handle : entry.getValue()) {
                tablePositionMap.put(handle, i);
            }
            i++;

            SessionState.ExportObject<Table> tableExportObject = sessionState.newServerSideExport(table);
            clientFigure.addTables(TableServiceGrpcImpl.buildTableCreationResponse(TableReference.newBuilder().setTicket(tableExportObject.getExportId()).build(), table));
        }

        // TODO (deephaven-core#62) implement once tablemaps are ready
//        i = 0;
//        for (Map.Entry<TableMap, List<TableMapHandle>> entry : figure.getTableMapHandles().stream().collect(Collectors.groupingBy(TableMapHandle::getTableMap)).entrySet()) {
//            Set<String> relevantColumns = entry.getValue().stream().map(TableMapHandle::getColumns).flatMap(Set::stream).collect(Collectors.toSet());
//            TableMap tableMap = new TableMapSupplier(entry.getKey(), Collections.singletonList(t -> t.view(relevantColumns)));
//
//            for (TableMapHandle handle : entry.getValue()) {
//                tableMapPositionMap.put(handle, i);
//            }
//            i++;
//
//            SessionState.ExportObject<TableMap> tableExportObject = sessionState.newServerSideExport(tableMap);
//            clientFigure.addTableMap(...)
//        }

        assignOptionalField(figure.getTitle(), clientFigure::setTitle, clientFigure::clearTitle);
        assignOptionalField(toCssColorString(figure.getTitleColor()), clientFigure::setTitleColor, clientFigure::clearTitleColor);
        assignOptionalField(toCssFont(figure.getTitleFont()), clientFigure::setTitleFont, clientFigure::clearTitleFont);

        List<ChartImpl> charts = figure.getCharts().getCharts();

        for (ChartImpl chart : charts) {
            clientFigure.addCharts(translate(chart));
        }

        clientFigure.setCols(figure.getWidth());
        clientFigure.setRows(figure.getHeight());

        clientFigure.setUpdateInterval(figure.getUpdateInterval());

        clientFigure.addAllErrors(errorList);

        return clientFigure.build();
    }

    private static <T> void assignOptionalField(T value, Consumer<T> setter, Runnable clear) {
        if (value != null) {
            setter.accept(value);
        } else {
            clear.run();
        }
    }

    private FigureDescriptor.ChartDescriptor translate(ChartImpl chart) {
        assert chart.dimension() == 2 : "Only dim=2 supported";
        FigureDescriptor.ChartDescriptor.Builder clientChart = FigureDescriptor.ChartDescriptor.newBuilder();

        boolean swappedPositions = chart.getPlotOrientation() != ChartImpl.PlotOrientation.VERTICAL;
        Map<String, AxisDescriptor> axes = new HashMap<>();

        //x=0, y=1, z=2, unless swapped

        // The first X axis is on the bottom, later instances should be on the top. Likewise, the first Y axis
        // is on the left, and later instances appear on the right.
        AxisDescriptor.Builder firstX = null;
        AxisDescriptor.Builder firstY = null;

        for (int i = 0; i < chart.getAxis().length; i++) {
            final AxisDescriptor.AxisType type;
            if ((i == 0 && !swappedPositions) || (i == 1 && swappedPositions)) {
                type = AxisDescriptor.AxisType.X;
            } else {
                assert i == 0 || i == 1;
                type = AxisDescriptor.AxisType.Y;
            }
            List<AxisImpl> currentPositionAxes = chart.getAxis()[i];
            for (AxisImpl axis : currentPositionAxes) {
                if (axis.getType() == null) {
                    // apparently unused, yet still in the collection - skip adding it to our list
                    // so we don't consider it later
                    continue;
                }
                AxisDescriptor.Builder clientAxis = AxisDescriptor.newBuilder();
                clientAxis.setId(type.name() + axis.id());
                clientAxis.setFormatType(AxisDescriptor.AxisFormatType.valueOf(axis.getType().name()));
                clientAxis.setLog(axis.isLog());
                assignOptionalField(axis.getLabel(), clientAxis::setLabel, clientAxis::clearLabel);
                assignOptionalField(toCssFont(axis.getLabelFont()), clientAxis::setLabelFont, clientAxis::clearLabelFont);
//                clientAxis.setFormat(axis.getFormat().toString());
                assignOptionalField(axis.getFormatPattern(), clientAxis::setFormatPattern, clientAxis::clearFormatPattern);
                assignOptionalField(toCssColorString(axis.getColor()), clientAxis::setColor, clientAxis::clearColor);
                clientAxis.setMinRange(axis.getMinRange());
                clientAxis.setMaxRange(axis.getMaxRange());
                clientAxis.setMinorTicksVisible(axis.isMinorTicksVisible());
                clientAxis.setMajorTicksVisible(axis.isMajorTicksVisible());
                clientAxis.setMinorTickCount(axis.getMinorTickCount());
                clientAxis.setGapBetweenMajorTicks(axis.getGapBetweenMajorTicks());
                assignOptionalField(axis.getMajorTickLocations(), arr -> DoubleStream.of(arr).forEach(clientAxis::addMajorTickLocations), clientAxis::clearMajorTickLocations);
//                clientAxis.setAxisTransform(axis.getAxisTransform().toString());
                clientAxis.setTickLabelAngle(axis.getTickLabelAngle());
                clientAxis.setInvert(axis.getInvert());
                clientAxis.setIsTimeAxis(axis.isTimeAxis());

                final AxisTransform axisTransform = axis.getAxisTransform();
                if (axisTransform instanceof AxisTransformBusinessCalendar) {
                    clientAxis.setBusinessCalendarDescriptor(translateBusinessCalendar((AxisTransformBusinessCalendar) axisTransform));
                }

                clientAxis.setType(type);
                if (type == AxisDescriptor.AxisType.X) {
                    if (firstX == null) {
                        firstX = clientAxis;
                        clientAxis.setPosition(AxisDescriptor.AxisPosition.BOTTOM);
                    } else {
                        clientAxis.setPosition(AxisDescriptor.AxisPosition.TOP);
                    }
                } else if (type == AxisDescriptor.AxisType.Y) {
                    if (firstY == null) {
                        firstY = clientAxis;
                        clientAxis.setPosition(AxisDescriptor.AxisPosition.LEFT);
                    } else {
                        clientAxis.setPosition(AxisDescriptor.AxisPosition.RIGHT);
                    }
                }

                axes.put(type.name() + axis.id(), clientAxis.build());
            }
        }
        clientChart.addAllAxes(axes.values());

        Stream.Builder<SeriesDescriptor> clientSeriesCollection = Stream.builder();
        Stream.Builder<FigureDescriptor.MultiSeriesDescriptor> clientMultiSeriesCollection = Stream.builder();

        chart.getAxes().forEach(axesImpl -> {

            // assign some bookkeeping axis instances just once
            AxisDescriptor xAxis = axes.get("X" + axesImpl.xAxis().id());
            AxisDescriptor yAxis = axes.get("Y" + axesImpl.yAxis().id());

            // this bookkeeping is only for category based plots
            final AxisDescriptor catAxis;
            final AxisDescriptor numAxis;
            if (xAxis.getFormatType() == AxisDescriptor.AxisFormatType.CATEGORY) {
                assert yAxis.getFormatType() == AxisDescriptor.AxisFormatType.NUMBER;
                catAxis = xAxis;
                numAxis = yAxis;
            } else if (yAxis.getFormatType() == AxisDescriptor.AxisFormatType.CATEGORY) {
                assert xAxis.getFormatType() == AxisDescriptor.AxisFormatType.NUMBER;
                catAxis = yAxis;
                numAxis = xAxis;
            } else {
                // this plot is not category based, leave these blank, they are not needed
                catAxis = null;
                numAxis = null;
            }

            // use the description map since it is known to be ordered correctly
            axesImpl.dataSeries().getSeriesDescriptions().values().stream().map(SeriesCollection.SeriesDescription::getSeries).forEach(seriesInternal -> {
                if (seriesInternal instanceof AbstractDataSeries) {
                    SeriesDescriptor.Builder clientSeries = SeriesDescriptor.newBuilder();
                    clientSeries.setPlotStyle(FigureDescriptor.SeriesPlotStyle.valueOf(axesImpl.getPlotStyle().name()));
                    clientSeries.setName(String.valueOf(seriesInternal.name()));
                    Stream.Builder<SourceDescriptor> clientAxes = Stream.builder();

                    AbstractDataSeries s = (AbstractDataSeries) seriesInternal;

                    assignOptionalField(s.getLinesVisible(), clientSeries::setLinesVisible, clientSeries::clearLinesVisible);
                    assignOptionalField(s.getPointsVisible(), clientSeries::setShapesVisible, clientSeries::clearShapesVisible);
                    clientSeries.setGradientVisible(s.getGradientVisible());
                    assignOptionalField(toCssColorString(s.getLineColor()), clientSeries::setLineColor, clientSeries::clearLineColor);
//                            clientSeries.setLineStyle(s.getLineStyle().toString());
                    assignOptionalField(s.getPointLabelFormat(), clientSeries::setPointLabelFormat, clientSeries::clearPointLabelFormat);
                    assignOptionalField(s.getXToolTipPattern(), clientSeries::setXToolTipPattern, clientSeries::clearXToolTipPattern);
                    assignOptionalField(s.getYToolTipPattern(), clientSeries::setYToolTipPattern, clientSeries::clearYToolTipPattern);

                    // build the set of axes that the series is watching, and give each a type, starting
                    // with the x and y we have so far mapped to this

                    if (s instanceof AbstractXYDataSeries) {
                        if (s instanceof IntervalXYDataSeriesArray) {
                            //interval (aka histogram)
                            IntervalXYDataSeriesArray series = (IntervalXYDataSeriesArray) s;
                            clientAxes.add(makeSourceDescriptor(series.getX(), SourceType.X, xAxis));
                            clientAxes.add(makeSourceDescriptor(series.getStartX(), SourceType.X_LOW, xAxis));
                            clientAxes.add(makeSourceDescriptor(series.getEndX(), SourceType.X_HIGH, xAxis));
                            clientAxes.add(makeSourceDescriptor(series.getY(), SourceType.Y, yAxis));
                            clientAxes.add(makeSourceDescriptor(series.getStartY(), SourceType.Y_LOW, yAxis));
                            clientAxes.add(makeSourceDescriptor(series.getEndY(), SourceType.Y_HIGH, yAxis));
                        } else if (s instanceof XYErrorBarDataSeriesArray) {
                            //errorbar x, xy
                            XYErrorBarDataSeriesArray series = (XYErrorBarDataSeriesArray) s;
                            clientAxes.add(makeSourceDescriptor(series.getX(), SourceType.X, xAxis));
                            clientAxes.add(makeSourceDescriptor(series.getXLow(), SourceType.X_LOW, xAxis));
                            clientAxes.add(makeSourceDescriptor(series.getXHigh(), SourceType.X_HIGH, xAxis));
                            clientAxes.add(makeSourceDescriptor(series.getY(), SourceType.Y, yAxis));
                            clientAxes.add(makeSourceDescriptor(series.getYLow(), SourceType.Y_LOW, yAxis));
                            clientAxes.add(makeSourceDescriptor(series.getYHigh(), SourceType.Y_HIGH, yAxis));
                        } else if (s instanceof OHLCDataSeriesArray) {
                            OHLCDataSeriesArray series = (OHLCDataSeriesArray) s;
                            clientAxes.add(makeSourceDescriptor(series.getTime(), SourceType.TIME, xAxis));
                            clientAxes.add(makeSourceDescriptor(series.getOpen(), SourceType.OPEN, yAxis));
                            clientAxes.add(makeSourceDescriptor(series.getClose(), SourceType.CLOSE, yAxis));
                            clientAxes.add(makeSourceDescriptor(series.getHigh(), SourceType.HIGH, yAxis));
                            clientAxes.add(makeSourceDescriptor(series.getLow(), SourceType.LOW, yAxis));
                        } else if (s instanceof XYDataSeriesArray) {
                            //xy of some other kind
                            XYDataSeriesArray series = (XYDataSeriesArray) s;
                            clientAxes.add(makeSourceDescriptor(series.getX(), SourceType.X, xAxis));
                            clientAxes.add(makeSourceDescriptor(series.getY(), SourceType.Y, yAxis));
                        } else {
                            // warn about other unsupported series types
                            errorList.add("OpenAPI presently does not support series of type " + s.getClass());
                        }

                        //TODO color label size shape
                    } else if (s instanceof AbstractCategoryDataSeries) {
                        if (s instanceof CategoryDataSeriesTableMap) {//bar and pie from a table
                            CategoryDataSeriesTableMap series = (CategoryDataSeriesTableMap) s;
                            clientAxes.add(makeSourceDescriptor(series.getTableHandle(), series.getCategoryCol(), catAxis == xAxis ? SourceType.X : SourceType.Y, catAxis));
                            clientAxes.add(makeSourceDescriptor(series.getTableHandle(), series.getValueCol(), numAxis == xAxis ? SourceType.X : SourceType.Y, numAxis));
                        } else if (s instanceof CategoryDataSeriesSwappableTableMap) {
                            CategoryDataSeriesSwappableTableMap series = (CategoryDataSeriesSwappableTableMap) s;

                            clientAxes.add(makeSourceDescriptor(series.getSwappableTable(), series.getCategoryCol(), catAxis == xAxis ? SourceType.X : SourceType.Y, catAxis));
                            clientAxes.add(makeSourceDescriptor(series.getSwappableTable(), series.getNumericCol(), numAxis == xAxis ? SourceType.X : SourceType.Y, numAxis));

                        } else if (s instanceof CategoryDataSeriesMap) {//bar and plot from constant data
                            errorList.add("OpenAPI presently does not support series of type " + s.getClass());
                        }
                        //TODO color label size shape
                    }

                    clientSeries.addAllDataSources(clientAxes.build().collect(Collectors.toList()));
                    clientSeriesCollection.add(clientSeries.build());
                } else if (seriesInternal instanceof AbstractMultiSeries) {
                    AbstractMultiSeries multiSeries = (AbstractMultiSeries) seriesInternal;

                    MultiSeriesDescriptor.Builder clientSeries = MultiSeriesDescriptor.newBuilder();
                    clientSeries.setPlotStyle(SeriesPlotStyle.valueOf(axesImpl.getPlotStyle().name()));
                    clientSeries.setName(String.valueOf(seriesInternal.name()));

                    Stream.Builder<MultiSeriesSourceDescriptor> clientAxes = Stream.builder();


                    if (multiSeries instanceof AbstractTableMapHandleMultiSeries) {
                        AbstractTableMapHandleMultiSeries tableMapMultiSeries = (AbstractTableMapHandleMultiSeries) multiSeries;
                        int plotHandleId = tableMapMultiSeries.getTableMapHandle().id();

                        if (tableMapMultiSeries instanceof MultiXYSeries) {
                            MultiXYSeries multiXYSeries = (MultiXYSeries) tableMapMultiSeries;
                            clientAxes.add(makeTableMapSourceDescriptor(plotHandleId, multiXYSeries.getXCol(), SourceType.X, xAxis));
                            clientAxes.add(makeTableMapSourceDescriptor(plotHandleId, multiXYSeries.getYCol(), SourceType.Y, yAxis));
                            clientSeries.setLineColor(stringMapWithDefault(mergeColors(
                                    multiXYSeries.lineColorSeriesNameTointMap(),
                                    multiXYSeries.lineColorSeriesNameToStringMap(),
                                    multiXYSeries.lineColorSeriesNameToPaintMap()
                            )));
                            clientSeries.setPointColor(stringMapWithDefault(mergeColors(
                                    multiXYSeries.pointColorSeriesNameTointMap(),
                                    multiXYSeries.pointColorSeriesNameToStringMap(),
                                    multiXYSeries.pointColorSeriesNameToPaintMap()
                            )));
                            clientSeries.setLinesVisible(boolMapWithDefault(multiXYSeries.linesVisibleSeriesNameToBooleanMap()));
                            clientSeries.setPointsVisible(boolMapWithDefault(multiXYSeries.pointsVisibleSeriesNameToBooleanMap()));
                            clientSeries.setGradientVisible(boolMapWithDefault(multiXYSeries.gradientVisibleSeriesNameTobooleanMap()));
                            clientSeries.setPointLabelFormat(stringMapWithDefault(multiXYSeries.pointLabelFormatSeriesNameToStringMap()));
                            clientSeries.setXToolTipPattern(stringMapWithDefault(multiXYSeries.xToolTipPatternSeriesNameToStringMap()));
                            clientSeries.setYToolTipPattern(stringMapWithDefault(multiXYSeries.yToolTipPatternSeriesNameToStringMap()));
                            clientSeries.setPointLabel(stringMapWithDefault(multiXYSeries.pointColorSeriesNameToStringMap(), Objects::toString));
                            clientSeries.setPointSize(doubleMapWithDefault(
                                    multiXYSeries.pointSizeSeriesNameToNumberMap(),
                                    number -> number == null ? null : number.doubleValue()
                            ));

                            clientSeries.setPointShape(stringMapWithDefault(mergeShapes(
                                    multiXYSeries.pointShapeSeriesNameToStringMap(),
                                    multiXYSeries.pointShapeSeriesNameToShapeMap()
                            )));
                        } else if (tableMapMultiSeries instanceof MultiCatSeries) {
                            MultiCatSeries multiCatSeries = (MultiCatSeries) tableMapMultiSeries;
                            clientAxes.add(makeTableMapSourceDescriptor(plotHandleId, multiCatSeries.getCategoryCol(), catAxis == xAxis ? SourceType.X : SourceType.Y, catAxis));
                            clientAxes.add(makeTableMapSourceDescriptor(plotHandleId, multiCatSeries.getNumericCol(), numAxis == xAxis ? SourceType.X : SourceType.Y, numAxis));
                            clientSeries.setLineColor(stringMapWithDefault(mergeColors(
                                    multiCatSeries.lineColorSeriesNameTointMap(),
                                    multiCatSeries.lineColorSeriesNameToStringMap(),
                                    multiCatSeries.lineColorSeriesNameToPaintMap()
                            )));
                            clientSeries.setPointColor(stringMapWithDefault(mergeColors(
                                    multiCatSeries.pointColorSeriesNameTointMap(),
                                    multiCatSeries.pointColorSeriesNameToStringMap(),
                                    multiCatSeries.pointColorSeriesNameToPaintMap()
                            )));
                            clientSeries.setLinesVisible(boolMapWithDefault(multiCatSeries.linesVisibleSeriesNameToBooleanMap()));
                            clientSeries.setPointsVisible(boolMapWithDefault(multiCatSeries.pointsVisibleSeriesNameToBooleanMap()));
                            clientSeries.setGradientVisible(boolMapWithDefault(multiCatSeries.gradientVisibleSeriesNameTobooleanMap()));
                            clientSeries.setPointLabelFormat(stringMapWithDefault(multiCatSeries.pointLabelFormatSeriesNameToStringMap()));
                            clientSeries.setXToolTipPattern(stringMapWithDefault(multiCatSeries.xToolTipPatternSeriesNameToStringMap()));
                            clientSeries.setYToolTipPattern(stringMapWithDefault(multiCatSeries.yToolTipPatternSeriesNameToStringMap()));
                            clientSeries.setPointLabel(stringMapWithDefault(multiCatSeries.pointLabelSeriesNameToObjectMap(), Objects::toString));
                            clientSeries.setPointSize(doubleMapWithDefault(
                                    multiCatSeries.pointSizeSeriesNameToNumberMap(),
                                    number -> number == null ? null : number.doubleValue()
                            ));

                            clientSeries.setPointShape(stringMapWithDefault(mergeShapes(
                                    multiCatSeries.pointShapeSeriesNameToStringMap(),
                                    multiCatSeries.pointShapeSeriesNameToShapeMap()
                            )));
                        }
                    } else {
                        errorList.add("OpenAPI presently does not support series of type " + multiSeries.getClass());
                    }

                    clientSeries.addAllDataSources(clientAxes.build().collect(Collectors.toList()));

                    clientMultiSeriesCollection.add(clientSeries.build());
                } else {
                    errorList.add("OpenAPI presently does not support series of type " + seriesInternal.getClass());
                    //TODO handle multi-series, possibly transformed case?
                }
            });
        });

        clientChart.addAllSeries(clientSeriesCollection.build().collect(Collectors.toList()));
        clientChart.addAllMultiSeries(clientMultiSeriesCollection.build().collect(Collectors.toList()));

        clientChart.setChartType(FigureDescriptor.ChartDescriptor.ChartType.valueOf(chart.getChartType().name()));
        clientChart.setColspan(chart.colSpan());
        assignOptionalField(toCssColorString(chart.getLegendColor()), clientChart::setLegendColor, clientChart::clearLegendColor);
        assignOptionalField(toCssFont(chart.getLegendFont()), clientChart::setLegendFont, clientChart::clearLegendFont);
        clientChart.setRowspan(chart.rowSpan());
        clientChart.setShowLegend(chart.isShowLegend());
        assignOptionalField(chart.getTitle(), clientChart::setTitle, clientChart::clearTitle);
        assignOptionalField(toCssColorString(chart.getTitleColor()), clientChart::setTitleColor, clientChart::clearTitleColor);
        assignOptionalField(toCssFont(chart.getTitleFont()), clientChart::setTitleFont, clientChart::clearTitleFont);

        return clientChart.build();
    }

    @NotNull
    private BusinessCalendarDescriptor translateBusinessCalendar(AxisTransformBusinessCalendar axisTransform) {
        final BusinessCalendar businessCalendar = axisTransform.getBusinessCalendar();
        final BusinessCalendarDescriptor.Builder businessCalendarDescriptor = BusinessCalendarDescriptor.newBuilder();
        businessCalendarDescriptor.setName(businessCalendar.name());
        businessCalendarDescriptor.setTimeZone(businessCalendar.timeZone().getTimeZone().getID());
        Arrays.stream(BusinessCalendarDescriptor.DayOfWeek.values()).filter(dayOfWeek -> {
            final DayOfWeek day = DayOfWeek.valueOf(dayOfWeek.name());
            return businessCalendar.isBusinessDay(day);
        }).forEach(businessCalendarDescriptor::addBusinessDays);
        businessCalendar.getDefaultBusinessPeriods().stream().map(period ->{
            final String[] array = period.split(",");
            final BusinessPeriod.Builder businessPeriod = BusinessPeriod.newBuilder();
            businessPeriod.setOpen(array[0]);
            businessPeriod.setClose(array[1]);
            return businessPeriod;
        }).forEach(businessCalendarDescriptor::addBusinessPeriods);

        businessCalendar.getHolidays().entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).map(entry -> {
            final LocalDate.Builder localDate = LocalDate.newBuilder();
            localDate.setYear(entry.getKey().getYear());
            localDate.setMonth(entry.getKey().getMonthValue());
            localDate.setDay(entry.getKey().getDayOfMonth());
            final Holiday.Builder holiday = Holiday.newBuilder();
            Arrays.stream(entry.getValue().getBusinessPeriods()).map(bp -> {
                final String open = HOLIDAY_TIME_FORMAT.withZone(businessCalendar.timeZone().getTimeZone()).print(bp.getStartTime().getMillis());
                final String close = HOLIDAY_TIME_FORMAT.withZone(businessCalendar.timeZone().getTimeZone()).print(bp.getEndTime().getMillis());
                final BusinessPeriod.Builder businessPeriod = BusinessPeriod.newBuilder();
                businessPeriod.setOpen(open);
                businessPeriod.setClose(close);
                return businessPeriod;
            }).forEach(holiday::addBusinessPeriods);
            holiday.setDate(localDate);
            return holiday.build();
        }).forEach(businessCalendarDescriptor::addHolidays);

        return businessCalendarDescriptor.build();
    }

    private PlotUtils.HashMapWithDefault<String, String> mergeShapes(PlotUtils.HashMapWithDefault<String, String> strings, PlotUtils.HashMapWithDefault<String, Shape> shapes) {
        PlotUtils.HashMapWithDefault<String, String> result = Stream.of(strings.keySet(), shapes.keySet())
                .flatMap(Set::stream)
                .distinct()
                .collect(Collectors.toMap(
                        Comparable::toString,
                        key -> Objects.requireNonNull(
                                mergeShape(
                                        strings.get(key),
                                        shapes.get(key)
                                ),
                                "key " + key + " had nulls in both shape maps"
                        ),
                        (s, s2) -> {
                            if (!s.equals(s2)) {
                                throw new IllegalStateException("More than one value possible for a given key: " + s + " and " + s2);
                            }
                            return s;
                        },
                        PlotUtils.HashMapWithDefault::new
                ));
        result.setDefault(mergeShape(strings.getDefault(), shapes.getDefault()));
        return result;
    }

    private String mergeShape(String string, Shape shape) {
        if (string != null) {
            return string;
        }
        NamedShape named = null;
        if (shape instanceof NamedShape) {
            named = (NamedShape) shape;
        } else if (shape instanceof JShapes) {
            named = JShapes.shape((JShapes) shape);
        }

        if (named == null) {
            return null;
        }
        return named.name();
    }

    /**
     * Merges the three maps into one, using CSS color strings, including default values
     */
    private PlotUtils.HashMapWithDefault<String, String> mergeColors(PlotUtils.HashMapWithDefault<String, Integer> seriesNameTointMap, PlotUtils.HashMapWithDefault<String, String> seriesNameToStringMap, PlotUtils.HashMapWithDefault<String, io.deephaven.gui.color.Paint> seriesNameToPaintMap) {
        PlotUtils.HashMapWithDefault<String, String> result = Stream.of(seriesNameTointMap.keySet(), seriesNameToStringMap.keySet(), seriesNameToPaintMap.keySet())
                .flatMap(Set::stream)
                .distinct()
                .collect(Collectors.toMap(
                        Comparable::toString,
                        key -> Objects.requireNonNull(
                                mergeCssColor(
                                        seriesNameTointMap.get(key),
                                        seriesNameToStringMap.get(key),
                                        seriesNameToPaintMap.get(key)
                                ),
                                "key " + key + " had nulls in all three color maps"
                        ),
                        (s, s2) -> {
                            if (!s.equals(s2)) {
                                throw new IllegalStateException("More than one value possible for a given key: " + s + " and " + s2);
                            }
                            return s;
                        },
                        PlotUtils.HashMapWithDefault::new
                ));
        // if a "higher precedence" map has a default, it overrides the other defaults
        result.setDefault(mergeCssColor(seriesNameTointMap.getDefault(), seriesNameToStringMap.getDefault(), seriesNameToPaintMap.getDefault()));
        return result;
    }

    private String mergeCssColor(Integer intColor, String strColor, io.deephaven.gui.color.Paint paintColor) {
        if (paintColor != null)  {
            String candidate = toCssColorString(paintColor);
            if (candidate != null) {
                return candidate;
            } // otherwise failed to be translated, lets at least try the others
        }
        if (strColor != null) {
            // lean on Color's translation. We know toCssColorString won't fail us here, since we're explicitly passing in a Color
            return toCssColorString(new io.deephaven.gui.color.Color(strColor));
        }
        if (intColor != null) {
            return toCssColorString(PlotUtils.intToColor(null, intColor));
        }
        return null;
    }

    private StringMapWithDefault stringMapWithDefault(PlotUtils.HashMapWithDefault<? extends Comparable<?>, String> map) {
        return stringMapWithDefault(map, Function.identity());
    }
    private <T> StringMapWithDefault stringMapWithDefault(PlotUtils.HashMapWithDefault<? extends Comparable<?>, T> map, Function<T, String> mappingFunc) {
        StringMapWithDefault.Builder result = StringMapWithDefault.newBuilder();
        result.setDefaultString(mappingFunc.apply(map.getDefault()));
        LinkedHashMap<? extends Comparable<?>, T> ordered = new LinkedHashMap<>(map);
        result.addAllKeys(ordered.keySet().stream().map(Comparable::toString).collect(Collectors.toList()));
        result.addAllValues(ordered.values().stream().map(mappingFunc).collect(Collectors.toList()));
        return result.build();
    }
    private DoubleMapWithDefault doubleMapWithDefault(PlotUtils.HashMapWithDefault<? extends Comparable<?>, Double> map) {
        return doubleMapWithDefault(map, Function.identity());
    }
    private <T> DoubleMapWithDefault doubleMapWithDefault(PlotUtils.HashMapWithDefault<? extends Comparable<?>, T> map, Function<T, Double> mappingFunc) {
        DoubleMapWithDefault.Builder result = DoubleMapWithDefault.newBuilder();
        result.setDefaultDouble(mappingFunc.apply(map.getDefault()));
        LinkedHashMap<? extends Comparable<?>, T> ordered = new LinkedHashMap<>(map);
        result.addAllKeys(ordered.keySet().stream().map(Comparable::toString).collect(Collectors.toList()));
        result.addAllValues(ordered.values().stream().map(mappingFunc).collect(Collectors.toList()));
        return result.build();
    }
    private BoolMapWithDefault boolMapWithDefault(PlotUtils.HashMapWithDefault<? extends Comparable<?>, Boolean> map) {
        BoolMapWithDefault.Builder result = BoolMapWithDefault.newBuilder();
        LinkedHashMap<? extends Comparable<?>, Boolean> ordered = new LinkedHashMap<>(map);
        result.addAllKeys(ordered.keySet().stream().map(Comparable::toString).collect(Collectors.toList()));
        result.addAllValues(new ArrayList<>(ordered.values()));
        return result.build();
    }

    private MultiSeriesSourceDescriptor makeTableMapSourceDescriptor(int plotHandleId, String columnName, SourceType sourceType, AxisDescriptor axis) {
        MultiSeriesSourceDescriptor.Builder source = MultiSeriesSourceDescriptor.newBuilder();
        source.setAxisId(axis.getId());
        source.setType(sourceType);
        source.setTableMapId(plotHandleId);
        source.setColumnName(columnName);
        return source.build();
    }

    private SourceDescriptor makeSourceDescriptor(TableHandle tableHandle, String columnName, SourceType sourceType, AxisDescriptor axis) {
        SourceDescriptor.Builder source = SourceDescriptor.newBuilder();

        source.setColumnName(columnName);
        source.setTableId(tablePositionMap.get(tableHandle));
        source.setAxisId(axis.getId());
        source.setType(sourceType);

        return source.build();
    }

    private SourceDescriptor makeSourceDescriptor(SwappableTable swappableTable, String columnName, SourceType sourceType, AxisDescriptor axis) {
        SourceDescriptor.Builder source = SourceDescriptor.newBuilder();

        source.setAxisId(axis.getId());
        source.setType(sourceType);

        if (swappableTable instanceof SwappableTableOneClickAbstract) {
            SwappableTableOneClickAbstract oneClick = (SwappableTableOneClickAbstract) swappableTable;
            source.setColumnName(columnName);
            source.setColumnType(swappableTable.getTableDefinition().getColumn(columnName).getDataType().getCanonicalName());
            source.setTableMapId(oneClick.getTableMapHandle().id());
            source.setOneClick(makeOneClick(oneClick));

        } else {
            errorList.add("OpenAPI does not presently support swappable table of type " + swappableTable.getClass());
        }

        return source.build();
    }

    private SourceDescriptor makeSourceDescriptor(IndexableNumericData data, SourceType sourceType, AxisDescriptor axis) {
        SourceDescriptor.Builder source = SourceDescriptor.newBuilder();
        source.setAxisId(axis.getId());
        source.setType(sourceType);
        if (data instanceof IndexableNumericDataTable) {
            ColumnHandlerFactory.ColumnHandler columnHandler = ((IndexableNumericDataTable) data).getColumnHandler();

            source.setColumnName(columnHandler.getColumnName());
            source.setTableId(tablePositionMap.get(columnHandler.getTableHandle()));
        } else if (data instanceof IndexableNumericDataSwappableTable) {
            IndexableNumericDataSwappableTable swappableTable = (IndexableNumericDataSwappableTable) data;
            if (swappableTable.getSwappableTable() instanceof SwappableTableOneClickAbstract) {
                SwappableTableOneClickAbstract oneClick = (SwappableTableOneClickAbstract) swappableTable.getSwappableTable();
                if (oneClick instanceof SwappableTableOneClickMap && ((SwappableTableOneClickMap) oneClick).getTransform() != null) {
                    errorList.add("OpenAPI does not presently support swappable tables that also use transform functions");
                    return source.build();
                }
                source.setColumnName(swappableTable.getColumn());
                source.setColumnType(swappableTable.getSwappableTable().getTableDefinition().getColumn(swappableTable.getColumn()).getDataType().getCanonicalName());
                source.setTableMapId(oneClick.getTableMapHandle().id());
                source.setOneClick(makeOneClick(oneClick));
            } else {
                errorList.add("OpenAPI does not presently support swappable table of type " + swappableTable.getSwappableTable().getClass());
            }

        } else {
            //TODO read out the array for constant data
            errorList.add("OpenAPI does not presently support source of type " + data.getClass());
        }
        return source.build();
    }

    private OneClickDescriptor makeOneClick(SwappableTableOneClickAbstract swappableTable) {
        OneClickDescriptor.Builder oneClick = OneClickDescriptor.newBuilder();
        oneClick.addAllColumns(swappableTable.getByColumns());
        oneClick.addAllColumnTypes(swappableTable.getByColumns()
                .stream()
                .map(colName -> swappableTable.getTableMapHandle().getTableDefinition().getColumn(colName).getDataType().getCanonicalName())
                .collect(Collectors.toList())
        );
        oneClick.setRequireAllFiltersToDisplay(swappableTable.isRequireAllFiltersToDisplay());
        return oneClick.build();
    }

    private String toCssColorString(io.deephaven.gui.color.Paint color) {
        if (!(color instanceof io.deephaven.gui.color.Color)) {
            errorList.add("OpenAPI does not presently support paint of type " + color);
            return null;
        }
        Color paint = (Color) color.javaColor();
        return "#" + Integer.toHexString(paint.getRGB()).substring(2);
    }
    private String toCssFont(Font font) {
        if (font == null) {
            return null;
        }
        return font.javaFont().getName();
    }
}
