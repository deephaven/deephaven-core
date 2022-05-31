/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.plot.errors.PlotInfo;
import io.deephaven.plot.util.ArgumentValidations;

/**
 * {@link IndexableData} with {@link Double} values.
 */
public class IndexableDataDouble extends IndexableData<Double> {

    private static final long serialVersionUID = -7776388251959380558L;
    private final IndexableNumericData data;
    private final boolean mapNanToNull;

    /**
     * Creates an IndexableDataDouble instance.
     *
     * If mapNanToNull is true, Double.NaN values in {@code data} are mapped to null.
     *
     * @param data data
     * @param mapNanToNull if true, Double.NaN values are mapped to null
     */
    public IndexableDataDouble(IndexableNumericData data, boolean mapNanToNull, final PlotInfo plotInfo) {
        super(plotInfo);
        ArgumentValidations.assertNotNull(data, "data", getPlotInfo());

        this.data = data;
        this.mapNanToNull = mapNanToNull;
    }

    /**
     * Creates an IndexableDataDouble instance.
     *
     * If mapNanToNull is true, Double.NaN values in {@code values} are mapped to null.
     *
     * @param values data
     * @param mapNanToNull if true, Double.NaN values are mapped to null
     * @param <T> type of the data in {@code values}
     */
    public <T extends Number> IndexableDataDouble(T[] values, boolean mapNanToNull, final PlotInfo plotInfo) {
        this(new IndexableNumericDataArrayNumber<>(values, plotInfo), mapNanToNull, plotInfo);
    }

    /**
     * Creates an IndexableDataDouble instance.
     *
     * If mapNanToNull is true, Double.NaN values in {@code values} are mapped to null.
     *
     * @param values data
     * @param mapNanToNull if true, Double.NaN values are mapped to null
     */
    public IndexableDataDouble(short[] values, boolean mapNanToNull, final PlotInfo plotInfo) {
        this(new IndexableNumericDataArrayShort(values, plotInfo), mapNanToNull, plotInfo);
    }

    /**
     * Creates an IndexableDataDouble instance.
     *
     * If mapNanToNull is true, Double.NaN values in {@code values} are mapped to null.
     *
     * @param values data
     * @param mapNanToNull if true, Double.NaN values are mapped to null
     * @param plotInfo plot information
     */
    public IndexableDataDouble(int[] values, boolean mapNanToNull, final PlotInfo plotInfo) {
        this(new IndexableNumericDataArrayInt(values, plotInfo), mapNanToNull, plotInfo);
    }

    /**
     * Creates an IndexableDataDouble instance.
     *
     * If mapNanToNull is true, Double.NaN values in {@code values} are mapped to null.
     *
     * @param values data
     * @param mapNanToNull if true, Double.NaN values are mapped to null
     * @param plotInfo plot information
     */
    public IndexableDataDouble(long[] values, boolean mapNanToNull, final PlotInfo plotInfo) {
        this(new IndexableNumericDataArrayLong(values, plotInfo), mapNanToNull, plotInfo);
    }

    /**
     * Creates an IndexableDataDouble instance.
     *
     * If mapNanToNull is true, Double.NaN values in {@code values} are mapped to null.
     *
     * @param values data
     * @param mapNanToNull if true, Double.NaN values are mapped to null
     * @param plotInfo plot information
     */
    public IndexableDataDouble(float[] values, boolean mapNanToNull, final PlotInfo plotInfo) {
        this(new IndexableNumericDataArrayFloat(values, plotInfo), mapNanToNull, plotInfo);
    }

    /**
     * Creates an IndexableDataDouble instance.
     *
     * If mapNanToNull is true, Double.NaN values in {@code values} are mapped to null.
     *
     * @param values data
     * @param mapNanToNull if true, Double.NaN values are mapped to null
     * @param plotInfo plot information
     */
    public IndexableDataDouble(double[] values, boolean mapNanToNull, final PlotInfo plotInfo) {
        this(new IndexableNumericDataArrayDouble(values, plotInfo), mapNanToNull, plotInfo);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public Double get(int index) {
        final double v = data.get(index);
        return (mapNanToNull && Double.isNaN(v)) ? null : v;
    }

    public boolean getMapNanToNull() {
        return mapNanToNull;
    }
}
