/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.axisformatters;

import java.text.NumberFormat;

/**
 * Format for axis tick labels. For time values, this would be how the dates are formatted. For numerical values, this
 * would be the number of significant digits, etc.
 */
public interface AxisFormat {

    /**
     * Set the pattern used for formatting values.
     *
     * @param pattern string indicating how values should be formatted.
     */
    void setPattern(String pattern);

    /**
     * Gets the formatter for given pattern.
     * <p>
     * Note that as time values are expressed as numbers, a number formatter is still suitable for dates.
     * </p>
     *
     * @return formatter
     */
    NumberFormat getNumberFormatter();

}
