package io.deephaven.plot.util;

import io.deephaven.base.verify.Require;

import java.io.Serializable;

/**
 * Continuous range.
 */
public class Range implements Serializable {
    private static final long serialVersionUID = 5886019742599081727L;
    private final double min;
    private final double max;
    private final boolean minOpen;
    private final boolean maxOpen;

    /**
     * Create a new range with a closed minimum interval and an open maximum interval.
     *
     * @param min minimum value for the range
     * @param max maximum value for the range
     */
    public Range(double min, double max) {
        this(min, max, false, true);
    }

    /**
     * Create a new range.
     *
     * @param min minimum value for the range
     * @param max maximum value for the range
     * @param minOpen true if the minimum is an open interval; false for a closed interval.
     * @param maxOpen true if the maximum is an open interval; false for a closed interval.
     */
    public Range(double min, double max, boolean minOpen, boolean maxOpen) {
        Require.geq(max, "max", min, "min");
        this.min = min;
        this.max = max;
        this.minOpen = minOpen;
        this.maxOpen = maxOpen;
    }

    /**
     * Determine if a value is in the range.
     *
     * @param d determine if this value is in the range
     * @return true if the value is in the range; false otherwise.
     */
    public boolean inRange(double d) {
        if (minOpen) {
            if (d <= min) {
                return false;
            }
        } else {
            if (d < min) {
                return false;
            }
        }

        if (maxOpen) {
            if (d >= max) {
                return false;
            }
        } else {
            if (d > max) {
                return false;
            }
        }

        return true;
    }

    /**
     * Gets the min value of the range
     *
     * @return min of the range
     */
    public double getMin() {
        return min;
    }

    /**
     * Gets the max value of the range
     *
     * @return max of the range
     */
    public double getMax() {
        return max;
    }
}
