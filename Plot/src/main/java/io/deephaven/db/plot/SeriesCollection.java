package io.deephaven.db.plot;

import io.deephaven.db.plot.errors.PlotExceptionCause;
import io.deephaven.db.plot.errors.PlotIllegalArgumentException;
import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.SwappableTable;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A collection of data series.
 */
public class SeriesCollection implements Serializable, PlotExceptionCause {

    private static final long serialVersionUID = -8561681018661975444L;

    private final PlotInfo plotInfo;

    public SeriesCollection(final PlotInfo plotInfo) {
        this.plotInfo = plotInfo;
    }

    @Override
    public PlotInfo getPlotInfo() {
        return plotInfo;
    }

    /**
     * Type of data series.
     */
    public enum SeriesType {
        CATEGORY, XY, UNARY_FUNCTION, BINARY_FUNCTION, INTERVAL, OHLC
    }

    /**
     * Description of a data series.
     */
    public static class SeriesDescription implements Serializable {
        private static final long serialVersionUID = 1791578353902414235L;

        private final SeriesType type;
        private final boolean isMultiSeries;
        private final SeriesInternal series;

        private SeriesDescription(final SeriesType type, final boolean isMultiSeries, final SeriesInternal series) {
            this.type = type;
            this.isMultiSeries = isMultiSeries;
            this.series = series;
        }

        /**
         * Gets the type of data series.
         *
         * @return type of data series.
         */
        public SeriesType getType() {
            return type;
        }

        /**
         * True if the series is a multiseries. False otherwise.
         *
         * @return true if the series is a multiseries. False otherwise.
         */
        public boolean isMultiSeries() {
            return isMultiSeries;
        }

        /**
         * Gets the data series.
         *
         * @return data series.
         */
        public SeriesInternal getSeries() {
            return series;
        }
    }

    private final AtomicInteger seriesCounter = new AtomicInteger(-1);
    // using a linked hash map because ordering may matter for things like color selection
    private final Map<Comparable, SeriesDescription> seriesDescriptions = new LinkedHashMap<>();
    private final Map<Integer, SeriesInternal> index = new HashMap<>();

    /**
     * Creates a copy of this series collection on a new set of axes.
     *
     * @param axes axes to create a copy of this series on.
     * @return copy of the series collection on the new set of axes.
     */
    public synchronized SeriesCollection copy(final AxesImpl axes) {
        ArgumentValidations.assertNotNull(axes, "axes", plotInfo);

        final SeriesCollection result = new SeriesCollection(axes.getPlotInfo());
        result.seriesCounter.set(this.seriesCounter.get());

        for (SeriesDescription seriesDescriptions : seriesDescriptions.values()) {
            final SeriesInternal s = seriesDescriptions.series.copy(axes);
            result.add(seriesDescriptions.type, seriesDescriptions.isMultiSeries, s);
        }

        return result;
    }

    /**
     * Gets the descriptions of the series in the collection. The result is a map between series name and description.
     *
     * @return descriptions of the series in the collection
     */
    public synchronized Map<Comparable, SeriesDescription> getSeriesDescriptions() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(seriesDescriptions));
    }

    /**
     * Gets the next series id.
     *
     * @return next series id.
     */
    int nextId() {
        return seriesCounter.incrementAndGet();
    }

    /**
     * Gets the series with the given series ID.
     *
     * @param id series id
     * @return series
     */
    public synchronized SeriesInternal series(int id) {
        return index.get(id);
    }

    /**
     * Gets the series with the given series name.
     *
     * @param name series name
     * @return series
     */
    public synchronized SeriesInternal series(Comparable name) {
        final SeriesDescription seriesDescription = seriesDescriptions.get(name);
        if (seriesDescription == null) {
            throw new PlotIllegalArgumentException("Series " + name + " not found", this);
        }

        return seriesDescription.series;
    }

    public synchronized Set<SwappableTable> getSwappableTables() {
        final Set<SwappableTable> swappableTables = new HashSet<>();
        index.values().forEach(series -> swappableTables.addAll(series.getSwappableTables()));
        return swappableTables;
    }

    /**
     * Gets the last series added to the collection.
     *
     * @return last series added to the collection.
     */
    synchronized SeriesInternal lastSeries() {
        if (seriesDescriptions.isEmpty()) {
            return null;
        }

        SeriesDescription last = null;

        for (SeriesDescription seriesDescription : seriesDescriptions.values()) {
            last = seriesDescription;
        }

        return last.getSeries();
    }

    /**
     * Add a new Series.
     *
     * @param type type of series
     * @param isMultiSeries true for multi-series; false for standard mono-series.
     * @param series series
     */
    public synchronized void add(final SeriesType type, final boolean isMultiSeries, final SeriesInternal series) {
        if (seriesDescriptions.containsKey(series.name())) {
            throw new PlotUnsupportedOperationException(
                    "Series with the same name already exists in the collection.  name=" + series.name(), this);
        }

        seriesDescriptions.put(series.name(), new SeriesDescription(type, isMultiSeries, series));
        index.put(series.id(), series);
    }

    /**
     * Removes the series with the specified {@code names}.
     *
     * @param names series names
     */
    public synchronized void remove(final Comparable... names) {
        for (Comparable name : names) {
            final SeriesDescription v = seriesDescriptions.get(name);

            if (v != null) {
                seriesDescriptions.remove(name);
                index.remove(v.series.id());
            }
        }
    }
}
