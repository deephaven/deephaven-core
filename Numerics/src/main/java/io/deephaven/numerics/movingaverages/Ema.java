/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.numerics.movingaverages;

import java.io.Serializable;

/**
 * An EMA.
 */
public class Ema extends AbstractMa implements Serializable {
    private static final long serialVersionUID = 5997048575951664518L;

    private double emaState;
    private boolean initialized = false;

    /**
     * Constructs a new EMA which use the given timescale.
     *
     * @param type type of EMA
     * @param mode compute the ema by tick or time
     * @param timeScale timescale for the EMAs.
     */
    public Ema(Type type, Mode mode, double timeScale) {
        super(type, mode, timeScale);
        emaState = Double.NaN;
    }

    protected void processDoubleLocal(long timestamp, double data) {
        if (!initialized) {
            switch (type) {
                case LEVEL:
                    // initialize the values to the first input
                    emaState = data;
                    break;
                case DIFFERENCE:
                    // initialize the values to zero
                    emaState = 0;
                    break;
                default:
                    throw new UnsupportedOperationException("Ema type is not yet supported: " + type);
            }

            initialized = true;
        }

        final long dt;
        switch (mode) {
            case TICK:
                dt = 1;
                break;
            case TIME:
                dt = timestamp - lastTimestamp;
                break;
            default:
                throw new UnsupportedOperationException("Ema mode is not yet supported: " + mode);
        }

        double dtLimited = dt < 0 ? 0 : dt;
        double alpha = Math.exp(-dtLimited / timeScale);
        emaState = alpha * emaState + (1 - alpha) * data;
    }

    /**
     * Gets the current value of the ema.
     *
     * @return current value of the ema.
     */
    public double getCurrent() {
        return emaState;
    }

    /**
     * Sets the current value of the ema state.
     *
     * @param value new value of the ema state.
     */
    public void setCurrent(double value) {
        emaState = value;
    }

    public void reset() {
        initialized = false;
        emaState = Double.NaN;
        lastTimestamp = Long.MIN_VALUE;
    }

}
