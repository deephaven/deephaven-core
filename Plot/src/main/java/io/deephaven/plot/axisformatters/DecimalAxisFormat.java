/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.axisformatters;

import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * A formatter for converting decimals into formatted strings.
 *
 * For details on the supported patterns see the javadoc for
 * <a href="https://docs.oracle.com/javase/7/docs/api/java/text/DecimalFormat.html">DecimalFormat</a>
 */
public class DecimalAxisFormat implements AxisFormat {
    private String pattern;

    @Override
    public void setPattern(final String pattern) {
        this.pattern = pattern;
    }

    @Override
    public NumberFormat getNumberFormatter() {
        return pattern == null ? new ScientificNumberFormatter() : new DecimalFormat(pattern);
    }
}
