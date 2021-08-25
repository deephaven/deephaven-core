/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.numerics.movingaverages;

import java.io.Serializable;

/**
 * A sum where the values are decayed at an exponential rate to zero.
 */
public class ExponentiallyDecayedSum implements Serializable {
    private static final long serialVersionUID = -4569122361884129319L;

    private final double decayRate;
    private final boolean enableTimestepOutOfOrderException;
    private long lastTimestamp = Long.MIN_VALUE;
    private double value = 0;

    /**
     * Creates a new exponentially decayed sum.
     *
     * @param decayRate rate in milliseconds to decay the sum.
     * @param enableTimestepOutOfOrderException true to allow an exception to be thrown when timesteps are not
     *        sequential.
     */
    public ExponentiallyDecayedSum(double decayRate, boolean enableTimestepOutOfOrderException) {
        this.decayRate = decayRate;
        this.enableTimestepOutOfOrderException = enableTimestepOutOfOrderException;
    }

    public void processDouble(long timestamp, double data) {
        long dt = Math.max(timestamp - lastTimestamp, 0);

        if (enableTimestepOutOfOrderException && lastTimestamp != Long.MIN_VALUE && dt < 0) {
            throw new IllegalStateException(
                    "Timesteps are out of order: timestamps=" + lastTimestamp + "," + timestamp);
        }

        double weight = Math.exp(-dt / decayRate);

        if (lastTimestamp == Long.MIN_VALUE) {
            value = data;
        } else {
            value = data + weight * value;
        }

        lastTimestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public void reset() {
        lastTimestamp = Long.MIN_VALUE;
        value = 0;
    }
}
