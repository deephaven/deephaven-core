/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.axisformatters;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;

import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;

/**
 * {@link NumberFormat} which formats numbers in scientific notation if they are outside the given range.
 */
public class ScientificNumberFormatter extends NumberFormat {
    private static final int DEFAULT_NUMBER_OF_DECIMALS =
            Configuration.getInstance().getIntegerWithDefault("plot.axis.ticks.numdecimals", 3);
    private static final double DEFAULT_LOWER_LIMIT =
            Configuration.getInstance().getDoubleWithDefault("plot.axis.ticks.lowerlimit", 1e-7);
    private static final double DEFAULT_UPPER_LIMIT =
            Configuration.getInstance().getDoubleWithDefault("plot.axis.ticks.upperlimit", 1e7);

    private final DecimalFormat decimalFormat = new DecimalFormat();
    private final DecimalFormat scientificFormat;
    private final double lowerLimit;
    private final double upperLimit;

    /**
     * Creates a ScientificNumberFormatter instance with the default number of decimals, lower limit, and upper limit.
     */
    @SuppressWarnings("WeakerAccess")
    public ScientificNumberFormatter() {
        this(DEFAULT_NUMBER_OF_DECIMALS, DEFAULT_LOWER_LIMIT, DEFAULT_UPPER_LIMIT);
    }

    /**
     * Creates a ScientificNumberFormatter instance.
     *
     * @param numDecimals the max number of decimals to display
     * @param lowerLimit gives a range around 0 [-lowerLimit, lowerLimit] for which each number inside the range
     *        excluding 0 will be formatted with scientific notation
     * @param upperLimit gives a range around 0 [-upperLimit, upperLimit] for which each number outside the range will
     *        be formatted with scientific notation
     */
    @SuppressWarnings("WeakerAccess")
    public ScientificNumberFormatter(int numDecimals, double lowerLimit, double upperLimit) {
        Require.geq(numDecimals, "numDecimals", 0);
        this.lowerLimit = lowerLimit;
        this.upperLimit = upperLimit;

        String format = "0.";
        for (int i = 0; i < numDecimals; i++) {
            format += "#";
        }
        format += "E0";
        scientificFormat = new DecimalFormat(format);
    }

    @Override
    public StringBuffer format(double number, StringBuffer toAppendTo, FieldPosition pos) {
        return formatInternal(Math.abs(number), toAppendTo, pos, 0L, number, false);
    }

    @Override
    public StringBuffer format(long number, StringBuffer toAppendTo, FieldPosition pos) {
        return formatInternal(Math.abs(number), toAppendTo, pos, number, 0d, true);
    }

    @Override
    public Number parse(String source, ParsePosition parsePosition) {
        return decimalFormat.parse(source, parsePosition);
    }

    private StringBuffer formatInternal(final double abs, final StringBuffer toAppendTo, final FieldPosition pos,
            final long number1, final double number2, final boolean isLong) {
        if ((abs < lowerLimit && abs > 0) || abs > upperLimit) {
            if (isLong) {
                return scientificFormat.format(number1, toAppendTo, pos);
            }
            return scientificFormat.format(number2, toAppendTo, pos);
        }

        if (isLong) {
            return decimalFormat.format(number1, toAppendTo, pos);
        }
        return decimalFormat.format(number2, toAppendTo, pos);

    }
}
