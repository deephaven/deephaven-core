package io.deephaven.db.plot;

import io.deephaven.datastructures.util.CollectionUtil;
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
import io.deephaven.gui.shape.JShapes;
import io.deephaven.gui.shape.NamedShape;
import io.deephaven.gui.shape.Shape;
import io.deephaven.web.shared.data.*;
import io.deephaven.web.shared.data.plot.*;
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
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FigureWidgetTranslator {

    private final List<String> errorList = new ArrayList<>();
    private static final DateTimeFormatter HOLIDAY_TIME_FORMAT = DateTimeFormat.forPattern("HH:mm");

    public FigureWidgetTranslator() {
    }

    public FigureDescriptor translate(DisplayableFigureDescriptor descriptor) {
        FigureDescriptor clientFigure = new FigureDescriptor();

        BaseFigureImpl figure = descriptor.getFigure().getFigure();
        clientFigure.setTitle(figure.getTitle());
        clientFigure.setTitleColor(toCssColorString(figure.getTitleColor()));
        clientFigure.setTitleFont(toCssFont(figure.getTitleFont()));

        List<ChartImpl> charts = figure.getCharts().getCharts();
        int size = charts.size();
        ChartDescriptor[] clientCharts = new ChartDescriptor[size];
        for (int i = 0; i < size; i++) {
            clientCharts[i] = translate(charts.get(i));
        }
        clientFigure.setCharts(clientCharts);

        clientFigure.setCols(figure.getWidth());
        clientFigure.setRows(figure.getHeight());

        clientFigure.setResizable(figure.isResizable());

        clientFigure.setUpdateInterval(figure.getUpdateInterval());

        // TODO (deephaven/deephaven-core/41): Update this to match new DisplayableFigureDescriptor and whatever the JS plotting API needs post-GRPC.
//        final int[] tableHandleIds = new int[descriptor.getDeflatedTables().size()];
//        for (int i = 0; i < descriptor.getDeflatedTables().size(); i++) {
//            final ExportedTableDescriptorMessage t = (ExportedTableDescriptorMessage) descriptor.getDeflatedTables().get(i);
//            final ExportedTableDescriptorMessage nonPreemptive = (ExportedTableDescriptorMessage) descriptor.getDeflatedNonPreemptiveTables().get(i);
//
//            if (nonPreemptive != null) {
//                tableHandleIds[i] = nonPreemptive.getId();
//                tablesToRelease.add(t.getId());
//            } else {
//                tableHandleIds[i] = t.getId();
//            }
//        }
//
//        final int[][] plotHandleIds = descriptor.getDeflatedIds().stream().map(set -> set.stream().mapToInt(Integer::intValue).toArray()).toArray(int[][]::new);
//        clientFigure.setTableIds(tableHandleIds);
//        clientFigure.setPlotHandleIds(plotHandleIds);
//
//        clientFigure.setTableMaps(descriptor.getDeflatedTableMaps().stream().map(etmd -> {
//            ExportedTableMapHandleManager.Descriptor tableMapDescriptor = (ExportedTableMapHandleManager.Descriptor) etmd;
//            return new TableMapHandle(tableMapDescriptor.handleId);
//        }).toArray(TableMapHandle[]::new));
//        clientFigure.setTableMapIds(descriptor.getDeflatedTableMapIds().stream().map(set -> set.stream().mapToInt(Integer::intValue).toArray()).toArray(int[][]::new));

        clientFigure.setErrors(errorList.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));

        return clientFigure;
    }

    private ChartDescriptor translate(ChartImpl chart) {
        assert chart.dimension() == 2 : "Only dim=2 supported";
        ChartDescriptor clientChart = new ChartDescriptor();

        boolean swappedPositions = chart.getPlotOrientation() != ChartImpl.PlotOrientation.VERTICAL;
        Map<String, AxisDescriptor> axes = new HashMap<>();

        //x=0, y=1, z=2, unless swapped

        // The first X axis is on the bottom, later instances should be on the top. Likewise, the first Y axis
        // is on the left, and later instances appear on the right.
        AxisDescriptor firstX = null;
        AxisDescriptor firstY = null;

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
                AxisDescriptor clientAxis = new AxisDescriptor();
                clientAxis.setId(type.name() + axis.id());
                clientAxis.setFormatType(AxisDescriptor.AxisFormatType.valueOf(axis.getType().name()));
                clientAxis.setLog(axis.isLog());
                clientAxis.setLabel(axis.getLabel());
                clientAxis.setLabelFont(toCssFont(axis.getLabelFont()));
//                clientAxis.setFormat(axis.getFormat().toString());
                clientAxis.setFormatPattern(axis.getFormatPattern());
                clientAxis.setColor(toCssColorString(axis.getColor()));
                clientAxis.setMinRange(axis.getMinRange());
                clientAxis.setMaxRange(axis.getMaxRange());
                clientAxis.setMinorTicksVisible(axis.isMinorTicksVisible());
                clientAxis.setMajorTicksVisible(axis.isMajorTicksVisible());
                clientAxis.setMinorTickCount(axis.getMinorTickCount());
                clientAxis.setGapBetweenMajorTicks(axis.getGapBetweenMajorTicks());
                clientAxis.setMajorTickLocations(axis.getMajorTickLocations());
//                clientAxis.setAxisTransform(axis.getAxisTransform().toString());
                clientAxis.setTickLabelAngle(axis.getTickLabelAngle());
                clientAxis.setInvert(axis.getInvert());
                clientAxis.setTimeAxis(axis.isTimeAxis());

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

                axes.put(type.name() + axis.id(), clientAxis);
            }
        }
        clientChart.setAxes(axes.values().toArray(new AxisDescriptor[0]));

        Stream.Builder<SeriesDescriptor> clientSeriesCollection = Stream.builder();
        Stream.Builder<MultiSeriesDescriptor> clientMultiSeriesCollection = Stream.builder();

        chart.getAxes().stream().forEach(axesImpl -> {

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
                    SeriesDescriptor clientSeries = new SeriesDescriptor();
                    clientSeries.setPlotStyle(SeriesPlotStyle.valueOf(axesImpl.getPlotStyle().name()));
                    clientSeries.setName(String.valueOf(seriesInternal.name()));
                    Stream.Builder<SourceDescriptor> clientAxes = Stream.builder();

                    AbstractDataSeries s = (AbstractDataSeries) seriesInternal;

                    clientSeries.setLinesVisible(s.getLinesVisible());
                    clientSeries.setShapesVisible(s.getPointsVisible());
                    clientSeries.setGradientVisible(s.getGradientVisible());
                    clientSeries.setLineColor(toCssColorString(s.getLineColor()));
//                            clientSeries.setLineStyle(s.getLineStyle().toString());
                    clientSeries.setPointLabelFormat(s.getPointLabelFormat());
                    clientSeries.setXToolTipPattern(s.getXToolTipPattern());
                    clientSeries.setYToolTipPattern(s.getYToolTipPattern());

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

                    clientSeries.setDataSources(clientAxes.build().toArray(SourceDescriptor[]::new));
                    clientSeriesCollection.add(clientSeries);
                } else if (seriesInternal instanceof AbstractMultiSeries) {
                    AbstractMultiSeries multiSeries = (AbstractMultiSeries) seriesInternal;

                    MultiSeriesDescriptor clientSeries = new MultiSeriesDescriptor();
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
                            assignMapWithDefaults(
                                    mergeColors(
                                            multiXYSeries.lineColorSeriesNameTointMap(),
                                            multiXYSeries.lineColorSeriesNameToStringMap(),
                                            multiXYSeries.lineColorSeriesNameToPaintMap()
                                    ),
                                    clientSeries::setLineColorDefault,
                                    clientSeries::setLineColorKeys,
                                    clientSeries::setLineColorValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    mergeColors(
                                            multiXYSeries.pointColorSeriesNameTointMap(),
                                            multiXYSeries.pointColorSeriesNameToStringMap(),
                                            multiXYSeries.pointColorSeriesNameToPaintMap()
                                    ),
                                    clientSeries::setPointColorDefault,
                                    clientSeries::setPointColorKeys,
                                    clientSeries::setPointColorValues,
                                    String[]::new
                            );
                            this.assignMapWithDefaults(
                                    multiXYSeries.linesVisibleSeriesNameToBooleanMap(),
                                    clientSeries::setLinesVisibleDefault,
                                    clientSeries::setLinesVisibleKeys,
                                    clientSeries::setLinesVisibleValues,
                                    Boolean[]::new
                            );
                            assignMapWithDefaults(
                                    multiXYSeries.pointsVisibleSeriesNameToBooleanMap(),
                                    clientSeries::setPointsVisibleDefault,
                                    clientSeries::setPointsVisibleKeys,
                                    clientSeries::setPointsVisibleValues,
                                    Boolean[]::new
                            );
                            assignMapWithDefaults(
                                    multiXYSeries.gradientVisibleSeriesNameTobooleanMap(),
                                    clientSeries::setGradientVisibleDefault,
                                    clientSeries::setGradientVisibleKeys,
                                    clientSeries::setGradientVisibleValues,
                                    Boolean[]::new
                            );
                            assignMapWithDefaults(
                                    multiXYSeries.pointLabelFormatSeriesNameToStringMap(),
                                    clientSeries::setPointLabelFormatDefault,
                                    clientSeries::setPointLabelFormatKeys,
                                    clientSeries::setPointLabelFormatValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiXYSeries.xToolTipPatternSeriesNameToStringMap(),
                                    clientSeries::setXToolTipPatternDefault,
                                    clientSeries::setXToolTipPatternKeys,
                                    clientSeries::setXToolTipPatternValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiXYSeries.yToolTipPatternSeriesNameToStringMap(),
                                    clientSeries::setYToolTipPatternDefault,
                                    clientSeries::setYToolTipPatternKeys,
                                    clientSeries::setYToolTipPatternValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiXYSeries.pointLabelSeriesNameToObjectMap(),
                                    Objects::toString,
                                    clientSeries::setPointLabelDefault,
                                    clientSeries::setPointLabelKeys,
                                    clientSeries::setPointLabelValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiXYSeries.pointSizeSeriesNameToNumberMap(),
                                    number -> number == null ? null : number.doubleValue(),
                                    clientSeries::setPointSizeDefault,
                                    clientSeries::setPointSizeKeys,
                                    clientSeries::setPointSizeValues,
                                    Double[]::new
                            );

                            assignMapWithDefaults(
                                    mergeShapes(
                                            multiXYSeries.pointShapeSeriesNameToStringMap(),
                                            multiXYSeries.pointShapeSeriesNameToShapeMap()
                                    ),
                                    clientSeries::setPointShapeDefault,
                                    clientSeries::setPointShapeKeys,
                                    clientSeries::setPointShapeValues,
                                    String[]::new
                            );

                        } else if (tableMapMultiSeries instanceof MultiCatSeries) {
                            MultiCatSeries multiCatSeries = (MultiCatSeries) tableMapMultiSeries;
                            clientAxes.add(makeTableMapSourceDescriptor(plotHandleId, multiCatSeries.getCategoryCol(), catAxis == xAxis ? SourceType.X : SourceType.Y, catAxis));
                            clientAxes.add(makeTableMapSourceDescriptor(plotHandleId, multiCatSeries.getNumericCol(), numAxis == xAxis ? SourceType.X : SourceType.Y, numAxis));
                            assignMapWithDefaults(
                                    mergeColors(
                                            multiCatSeries.lineColorSeriesNameTointMap(),
                                            multiCatSeries.lineColorSeriesNameToStringMap(),
                                            multiCatSeries.lineColorSeriesNameToPaintMap()
                                    ),
                                    clientSeries::setLineColorDefault,
                                    clientSeries::setLineColorKeys,
                                    clientSeries::setLineColorValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    mergeColors(
                                            multiCatSeries.pointColorSeriesNameTointMap(),
                                            multiCatSeries.pointColorSeriesNameToStringMap(),
                                            multiCatSeries.pointColorSeriesNameToPaintMap()
                                    ),
                                    clientSeries::setPointColorDefault,
                                    clientSeries::setPointColorKeys,
                                    clientSeries::setPointColorValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiCatSeries.linesVisibleSeriesNameToBooleanMap(),
                                    clientSeries::setLinesVisibleDefault,
                                    clientSeries::setLinesVisibleKeys,
                                    clientSeries::setLinesVisibleValues,
                                    Boolean[]::new
                            );
                            assignMapWithDefaults(
                                    multiCatSeries.pointsVisibleSeriesNameToBooleanMap(),
                                    clientSeries::setPointsVisibleDefault,
                                    clientSeries::setPointsVisibleKeys,
                                    clientSeries::setPointsVisibleValues,
                                    Boolean[]::new
                            );
                            assignMapWithDefaults(
                                    multiCatSeries.gradientVisibleSeriesNameTobooleanMap(),
                                    clientSeries::setGradientVisibleDefault,
                                    clientSeries::setGradientVisibleKeys,
                                    clientSeries::setGradientVisibleValues,
                                    Boolean[]::new
                            );
                            assignMapWithDefaults(
                                    multiCatSeries.pointLabelFormatSeriesNameToStringMap(),
                                    clientSeries::setPointLabelFormatDefault,
                                    clientSeries::setPointLabelFormatKeys,
                                    clientSeries::setPointLabelFormatValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiCatSeries.xToolTipPatternSeriesNameToStringMap(),
                                    clientSeries::setXToolTipPatternDefault,
                                    clientSeries::setXToolTipPatternKeys,
                                    clientSeries::setXToolTipPatternValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiCatSeries.yToolTipPatternSeriesNameToStringMap(),
                                    clientSeries::setYToolTipPatternDefault,
                                    clientSeries::setYToolTipPatternKeys,
                                    clientSeries::setYToolTipPatternValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiCatSeries.pointLabelSeriesNameToObjectMap(),
                                    Objects::toString,
                                    clientSeries::setPointLabelDefault,
                                    clientSeries::setPointLabelKeys,
                                    clientSeries::setPointLabelValues,
                                    String[]::new
                            );
                            assignMapWithDefaults(
                                    multiCatSeries.pointSizeSeriesNameToNumberMap(),
                                    number -> number == null ? null : number.doubleValue(),
                                    clientSeries::setPointSizeDefault,
                                    clientSeries::setPointSizeKeys,
                                    clientSeries::setPointSizeValues,
                                    Double[]::new
                            );

                            assignMapWithDefaults(
                                    mergeShapes(
                                            multiCatSeries.pointShapeSeriesNameToStringMap(),
                                            multiCatSeries.pointShapeSeriesNameToShapeMap()
                                    ),
                                    clientSeries::setPointShapeDefault,
                                    clientSeries::setPointShapeKeys,
                                    clientSeries::setPointShapeValues,
                                    String[]::new
                            );
                        }
                    } else {
                        errorList.add("OpenAPI presently does not support series of type " + multiSeries.getClass());
                    }

                    clientSeries.setDataSources(clientAxes.build().toArray(MultiSeriesSourceDescriptor[]::new));

                    clientMultiSeriesCollection.add(clientSeries);
                } else {
                    errorList.add("OpenAPI presently does not support series of type " + seriesInternal.getClass());
                    //TODO handle multi-series, possibly transformed case?
                }
            });
        });

        clientChart.setSeries(clientSeriesCollection.build().toArray(SeriesDescriptor[]::new));
        clientChart.setMultiSeries(clientMultiSeriesCollection.build().toArray(MultiSeriesDescriptor[]::new));

        clientChart.setChartType(ChartDescriptor.ChartType.valueOf(chart.getChartType().name()));
        clientChart.setColspan(chart.colSpan());
        clientChart.setLegendColor(toCssColorString(chart.getLegendColor()));
        clientChart.setLegendFont(toCssFont(chart.getLegendFont()));
        clientChart.setRowspan(chart.rowSpan());
        clientChart.setShowLegend(chart.isShowLegend());
        clientChart.setTitle(chart.getTitle());
        clientChart.setTitleColor(toCssColorString(chart.getTitleColor()));
        clientChart.setTitleFont(toCssFont(chart.getTitleFont()));

        return clientChart;
    }

    @NotNull
    private BusinessCalendarDescriptor translateBusinessCalendar(AxisTransformBusinessCalendar axisTransform) {
        final BusinessCalendar businessCalendar = axisTransform.getBusinessCalendar();
        final BusinessCalendarDescriptor businessCalendarDescriptor = new BusinessCalendarDescriptor();
        businessCalendarDescriptor.setName(businessCalendar.name());
        businessCalendarDescriptor.setTimeZone(businessCalendar.timeZone().getTimeZone().getID());
        final BusinessCalendarDescriptor.DayOfWeek[] businessDays = Arrays.stream(
                BusinessCalendarDescriptor.DayOfWeek.values()).filter(dayOfWeek -> {
            final DayOfWeek day = DayOfWeek.valueOf(dayOfWeek.name());
            return businessCalendar.isBusinessDay(day);
        }).toArray(BusinessCalendarDescriptor.DayOfWeek[]::new);
        businessCalendarDescriptor.setBusinessDays(businessDays);
        final BusinessPeriod[] businessPeriods = businessCalendar.getDefaultBusinessPeriods().stream().map(period ->{
            final String[] array = period.split(",");
            final BusinessPeriod businessPeriod = new BusinessPeriod();
            businessPeriod.setOpen(array[0]);
            businessPeriod.setClose(array[1]);
            return businessPeriod;
        }).toArray(BusinessPeriod[]::new);
        businessCalendarDescriptor.setBusinessPeriods(businessPeriods);

        final Holiday[] holidays = businessCalendar.getHolidays().entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).map(entry -> {
            final LocalDate localDate = new LocalDate();
            localDate.setYear(entry.getKey().getYear());
            localDate.setMonthValue((byte) entry.getKey().getMonthValue());
            localDate.setDayOfMonth((byte) entry.getKey().getDayOfMonth());
            final BusinessPeriod[] businessPeriodsHoliday = Arrays.stream(entry.getValue().getBusinessPeriods()).map(bp -> {
                final String open = HOLIDAY_TIME_FORMAT.withZone(businessCalendar.timeZone().getTimeZone()).print(bp.getStartTime().getMillis());
                final String close = HOLIDAY_TIME_FORMAT.withZone(businessCalendar.timeZone().getTimeZone()).print(bp.getEndTime().getMillis());
                final BusinessPeriod businessPeriod = new BusinessPeriod();
                businessPeriod.setOpen(open);
                businessPeriod.setClose(close);
                return businessPeriod;
            }).toArray(BusinessPeriod[]::new);
            final Holiday holiday = new Holiday();
            holiday.setDate(localDate);
            holiday.setBusinessPeriods(businessPeriodsHoliday);
            return holiday;
        }).toArray(Holiday[]::new);
        businessCalendarDescriptor.setHolidays(holidays);

        return businessCalendarDescriptor;
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

    private <V> void assignMapWithDefaults(PlotUtils.HashMapWithDefault<? extends Comparable<?>, V> map, Consumer<V> defaultSetter, Consumer<String[]> keySetter, Consumer<V[]> valueSetter, IntFunction<V[]> empty) {
        assignMapWithDefaults(map, Function.identity(), defaultSetter, keySetter, valueSetter, empty);
    }
    private <T, V> void assignMapWithDefaults(PlotUtils.HashMapWithDefault<? extends Comparable<?>, T> map, Function<T, V> mapFunc, Consumer<V> defaultSetter, Consumer<String[]> keySetter, Consumer<V[]> valueSetter, IntFunction<V[]> empty) {
        defaultSetter.accept(mapFunc.apply(map.getDefault()));
        LinkedHashMap<? extends Comparable<?>, T> ordered = new LinkedHashMap<>(map);
        keySetter.accept(ordered.keySet().stream().map(Comparable::toString).toArray(String[]::new));
        valueSetter.accept(ordered.values().stream().map(mapFunc).toArray(empty));
    }

    private MultiSeriesSourceDescriptor makeTableMapSourceDescriptor(int plotHandleId, String columnName, SourceType sourceType, AxisDescriptor axis) {
        MultiSeriesSourceDescriptor source = new MultiSeriesSourceDescriptor();
        source.setAxis(axis);
        source.setType(sourceType);
        source.setTableMapId(plotHandleId);
        source.setColumnName(columnName);
        return source;
    }

    private SourceDescriptor makeSourceDescriptor(TableHandle tableHandle, String columnName, SourceType sourceType, AxisDescriptor axis) {
        SourceDescriptor source = new SourceDescriptor();

        source.setColumnName(columnName);
        source.setTableId(tableHandle.id());
        source.setAxis(axis);
        source.setType(sourceType);

        return source;
    }

    private SourceDescriptor makeSourceDescriptor(SwappableTable swappableTable, String columnName, SourceType sourceType, AxisDescriptor axis) {
        SourceDescriptor source = new SourceDescriptor();

        source.setAxis(axis);
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

        return source;
    }

    private SourceDescriptor makeSourceDescriptor(IndexableNumericData data, SourceType sourceType, AxisDescriptor axis) {
        SourceDescriptor source = new SourceDescriptor();
        source.setAxis(axis);
        source.setType(sourceType);
        if (data instanceof IndexableNumericDataTable) {
            ColumnHandlerFactory.ColumnHandler columnHandler = ((IndexableNumericDataTable) data).getColumnHandler();

            source.setColumnName(columnHandler.getColumnName());
            source.setTableId(columnHandler.getTableHandle().id());
        } else if (data instanceof IndexableNumericDataSwappableTable) {
            IndexableNumericDataSwappableTable swappableTable = (IndexableNumericDataSwappableTable) data;
            if (swappableTable.getSwappableTable() instanceof SwappableTableOneClickAbstract) {
                SwappableTableOneClickAbstract oneClick = (SwappableTableOneClickAbstract) swappableTable.getSwappableTable();
                if (oneClick instanceof SwappableTableOneClickMap && ((SwappableTableOneClickMap) oneClick).getTransform() != null) {
                    errorList.add("OpenAPI does not presently support swappable tables that also use transform functions");
                    return source;
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
        return source;
    }

    private OneClickDescriptor makeOneClick(SwappableTableOneClickAbstract swappableTable) {
        OneClickDescriptor oneClick = new OneClickDescriptor();
        oneClick.setColumns(swappableTable.getByColumns().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        oneClick.setColumnTypes(swappableTable.getByColumns()
                .stream()
                .map(colName -> swappableTable.getTableMapHandle().getTableDefinition().getColumn(colName).getDataType().getCanonicalName())
                .toArray(String[]::new)
        );
        oneClick.setRequireAllFiltersToDisplay(swappableTable.isRequireAllFiltersToDisplay());
        return oneClick;
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
