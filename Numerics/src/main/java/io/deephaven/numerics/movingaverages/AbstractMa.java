/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.numerics.movingaverages;

import java.io.Serializable;

/**
 * Base class for computing moving averages.
 */
abstract public class AbstractMa implements Serializable {
    private static final long serialVersionUID = -4154939937570732234L;

    public enum Type {
        LEVEL, DIFFERENCE,
    }

    public enum Mode {
        TICK, TIME,
    }

    protected final Type type;
    protected final Mode mode;
    protected double timeScale;
    protected long lastTimestamp = Long.MIN_VALUE;
    protected double lastSample = Double.NaN;

    /**
     * empty constructor needed for externalizable
     */
    protected AbstractMa() {
        type = Type.LEVEL;
        mode = Mode.TICK;
    }

    /**
     * Constructs a new MA which use the given timescale.
     *
     * @param type type of EMA
     * @param mode compute the ema by tick or time
     * @param timeScale timescale for the EMAs.
     */
    public AbstractMa(Type type, Mode mode, double timeScale) {
        this.type = type;
        this.mode = mode;
        this.timeScale = timeScale;
    }

    public void processDouble(long timestamp, double data) {
        processDoubleLocal(timestamp, data);

        lastTimestamp = timestamp;
        lastSample = data;
    }

    abstract protected void processDoubleLocal(long timestamp, double data);

    /**
     * Gets the current value of the ema.
     *
     * @return current value of the ema.
     */
    abstract public double getCurrent();

    /**
     * Sets the current value of the ema state.
     *
     * @param value new value of the ema state.
     */
    abstract public void setCurrent(double value);

    public double getTimeScale() {
        return timeScale;
    }

    abstract public void reset();

    public void setTimeScale(double timeScale) {
        this.timeScale = timeScale;
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
