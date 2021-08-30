package io.deephaven.db.plot;

import io.deephaven.db.plot.util.ArgumentValidations;
import io.deephaven.db.plot.util.tables.*;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;


/**
 * Abstract implementation of the base series that all data series inherit from.
 */
public abstract class AbstractSeriesInternal implements Series, SeriesInternal {
    private static final long serialVersionUID = 156282740742368526L;

    private final AxesImpl axes;
    private final int id;
    private final Comparable name;
    private final Set<TableHandle> tableHandles = new CopyOnWriteArraySet<>();
    private final Set<SwappableTable> swappableTables = new CopyOnWriteArraySet<>();
    private final Set<TableMapHandle> tableMapHandles = new CopyOnWriteArraySet<>();


    protected AbstractSeriesInternal(AxesImpl axes, int id, Comparable name) {
        ArgumentValidations.assertNotNull(axes, "Axes must not be null", null);
        ArgumentValidations.assertNotNull(name, "Series name must not be null", axes.getPlotInfo());
        this.axes = axes;
        this.id = id;
        this.name = name;
    }

    // copy constructor
    protected AbstractSeriesInternal(final AbstractSeriesInternal series, final AxesImpl axes) {
        this.axes = axes;
        this.id = series.id;
        this.name = series.name;
        this.tableHandles.addAll(series.tableHandles);
        this.tableMapHandles.addAll(series.tableMapHandles);
        this.swappableTables.addAll(series.swappableTables);
    }

    @Override
    public AxesImpl axes() {
        return axes;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public Comparable name() {
        return name;
    }

    @Override
    public void addTableHandle(TableHandle h) {
        tableHandles.add(h);
    }

    @Override
    public void removeTableHandle(TableHandle h) {
        tableHandles.remove(h);
    }

    @Override
    public Set<TableHandle> getTableHandles() {
        return tableHandles;
    }

    @Override
    public void addTableMapHandle(TableMapHandle map) {
        tableMapHandles.add(map);
    }

    @Override
    public Set<TableMapHandle> getTableMapHandles() {
        return tableMapHandles;
    }

    @Override
    public void addSwappableTable(SwappableTable st) {
        swappableTables.add(st);
        addTableMapHandle(st.getTableMapHandle());
    }

    @Override
    public Set<SwappableTable> getSwappableTables() {
        return swappableTables;
    }
}
