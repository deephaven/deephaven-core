/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

public class Validate {
    public static int validatePositiveInteger(String name, String s) throws NumberFormatException {
        int i = 0;

        try {
            i = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(name + " is not a valid number");
        }

        if (i <= 0) {
            throw new NumberFormatException(name + " must be greater than zero");
        }

        return i;
    }

    public static int validateInteger(String name, String s) throws NumberFormatException {
        int i = 0;

        try {
            i = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(name + " is not a valid number");
        }

        return i;
    }

    public static double validatePositiveDouble(String name, String s)
        throws NumberFormatException {
        double d = 0;

        try {
            d = Double.parseDouble(s);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(name + " is not a valid number");
        }

        if (d <= 0) {
            throw new NumberFormatException(name + " must be greater than zero");
        }

        return d;
    }

    public static double validateDouble(String name, String s) throws NumberFormatException {
        double d = 0;

        try {
            d = Double.parseDouble(s);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(name + " is not a valid number");
        }

        return d;
    }

    public static void validate(boolean b, String errorMsg) throws Exception {
        if (!b) {
            throw new Exception(errorMsg);
        }
    }

    public static void validateDouble(String name, double value, double min, double max,
        boolean inclusiveMin, boolean inclusiveMax) throws Exception {
        if (Double.isNaN(value)) {
            throw new Exception(name + " may not be NaN");
        }

        if (inclusiveMin && value < min) {
            throw new Exception(name + " must be greater than or equal to " + min);
        }

        if (!inclusiveMin && value <= min) {
            throw new Exception(name + " must be greater than " + min);
        }

        if (inclusiveMax && value > max) {
            throw new Exception(name + " must be less than or equal to " + max);
        }

        if (!inclusiveMax && value >= max) {
            throw new Exception(name + " must be less than " + max);
        }
    }

    public static void validateInteger(String name, int value, int min, int max,
        boolean inclusiveMin, boolean inclusiveMax) throws Exception {
        if (inclusiveMin && value < min) {
            throw new Exception(name + " must be greater than or equal to " + min);
        }

        if (!inclusiveMin && value <= min) {
            throw new Exception(name + " must be greater than " + min);
        }

        if (inclusiveMax && value > max) {
            throw new Exception(name + " must be less than or equal to " + max);
        }

        if (!inclusiveMax && value >= max) {
            throw new Exception(name + " must be less than " + max);
        }
    }
}
