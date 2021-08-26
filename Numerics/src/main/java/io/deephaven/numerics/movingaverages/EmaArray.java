/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.numerics.movingaverages;

import java.io.Serializable;

/**
 * An array of EMAs with different timescales.
 */
public class EmaArray implements Serializable {
    private static final long serialVersionUID = 2127279097359961192L;

    private final Ema[] emas;
    private long lastTimestamp = Long.MIN_VALUE;
    private double lastSample = Double.NaN;

    /**
     * Constructs a new array of EMAs which use the given timescales.
     *
     * @param type type of EMA
     * @param mode compute the ema by tick or time
     * @param timeScales timescales for the EMAs.
     */
    public EmaArray(Ema.Type type, Ema.Mode mode, double[] timeScales) {
        this.emas = new Ema[timeScales.length];

        for (int i = 0; i < timeScales.length; i++) {
            this.emas[i] = new Ema(type, mode, timeScales[i]);
        }
    }

    public void processDouble(long timestamp, double data) {
        for (Ema ema : emas) {
            ema.processDouble(timestamp, data);
        }

        lastTimestamp = timestamp;
        lastSample = data;
    }

    public void reset() {
        for (Ema ema : emas) {
            ema.reset();
        }
    }

    public double[] getCurrent() {
        double[] result = new double[emas.length];

        for (int i = 0; i < emas.length; i++) {
            result[i] = emas[i].getCurrent();
        }

        return result;
    }

    /**
     * Gets the size of the ema array.
     *
     * @return size of the ema array.
     */
    public int size() {
        return emas.length;
    }

    /**
     * Gets the last time the ema was updated.
     *
     * @return last time the ema was updated.
     */
    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public double getLastSample() {
        return lastSample;
    }
}
