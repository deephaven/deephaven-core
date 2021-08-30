/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.numerics.interpolation;


import java.util.*;

import Jama.Matrix;
import io.deephaven.util.QueryConstants;

/**
 * A clean interface for performing interpolations.
 */
public class Interpolator {

    private Interpolator() {}

    public enum InterpolationAlgorithm {
        NEAREST, // nearest neighbor interpolation
        LINEAR, // linear interpolation
        PCHIP, // shape-preserving piecewise cubic interpolation
        SPLINE, // piecewise cubic spline interpolation
    }

    /**
     * Interpolates input data and evaluates the interpolation at a set of desired points.
     *
     * @param x input x data.
     * @param y input y data.
     * @param xi x points to evaluate the interpolation at.
     * @param method interpolation method to use.
     * @param extrapolate true if extrapolation is to be used for values outside of the x range;
     *        false if values outside the x range should return NaN.
     * @return y values interpolated at points xi if xi is in range(x), and y values extrapolated
     *         based on the extrapolate flag if xi is not in range(x).
     */
    public static double[] interpolate(double[] x, double[] y, double[] xi,
        InterpolationAlgorithm method, boolean extrapolate) {
        if (xi.length == 0) {
            return new double[0];
        }

        final int n = y.length;

        if (y.length != x.length) {
            throw new IllegalArgumentException(
                "X and Y are different lengths: " + x.length + "," + y.length);
        }

        if (containsInfinityOrNanOrNull(x)) {
            throw new IllegalArgumentException(
                "X contains Inf or NaNs or Nulls! " + Arrays.toString(x));
        }

        if (containsInfinityOrNanOrNull(y)) {
            throw new IllegalArgumentException(
                "Y contains Inf or NaNs or Nulls! " + Arrays.toString(y));
        }

        if (n < 2) {
            if (xi.length == 0) {
                return new double[0];
            } else {
                throw new IllegalArgumentException(
                    "At least 2 data points are needed to interpolate");
            }
        }

        // Start the algorithm
        double[] h = diff(x);

        if (containsNegative(h)) {
            double[][] sortTemp = sort(x, y);
            x = sortTemp[0];
            y = sortTemp[1];
            h = diff(x);
        }

        for (double hi : h) {
            if (hi == 0) {
                throw new IllegalArgumentException("The values of X should be distinct");
            }
        }

        // Interpolate
        double[] yi;
        int[] p = new int[xi.length];
        for (int i = 0; i < p.length; i++) {
            p[i] = i;
        }

        switch (method) {
            case SPLINE:
                yi = piecewiseCubicHermiteInterpolation(x, y, xi, new SplineDerivativeCalculator());
                break;
            case PCHIP:
                yi = piecewiseCubicHermiteInterpolation(x, y, xi, new PchipDerivativeCalculator());
                break;
            default:
                yi = new double[xi.length];

                if (containsNegative(diff(xi))) {
                    double[] pd = new double[xi.length];
                    for (int i = 0; i < pd.length; i++) {
                        pd[i] = i;
                    }

                    double[][] sortTemp = sort(xi, pd);
                    xi = sortTemp[0];

                    for (int i = 0; i < p.length; i++) {
                        p[i] = (int) sortTemp[1][i];
                    }
                } else {
                    for (int i = 0; i < p.length; i++) {
                        p[i] = i;
                    }
                }

                // Find indices of subintervals, x(k) <= u < x(k+1),
                // or u < x(1) or u >= x(m-1).
                int[] k = getBin(xi, x);

                for (int i = 0; i < xi.length; i++) {
                    if (xi[i] < x[0]) {
                        k[i] = 0;
                    }

                    if (xi[i] >= x[x.length - 1]) {
                        k[i] = n - 2;
                    }
                }

                switch (method) {
                    case NEAREST:
                        for (int i = 0; i < xi.length; i++) {
                            if (xi[i] >= (x[k[i]] + x[k[i] + 1]) / 2) {
                                k[i] = k[i] + 1;
                            }

                            yi[p[i]] = y[k[i]];
                        }

                        break;

                    case LINEAR:
                        for (int i = 0; i < xi.length; i++) {
                            double s = (xi[i] - x[k[i]]) / h[k[i]];
                            yi[p[i]] = y[k[i]] + s * (y[k[i] + 1] - y[k[i]]);
                        }
                        break;

                    default:
                        throw new UnsupportedOperationException(
                            "Interpolation method is not yet supported: " + method);
                }
        }

        // Override extrapolation
        if (!extrapolate) {
            for (int i = 0; i < xi.length; i++) {
                if (xi[i] < x[0] || xi[i] > x[n - 1]) {
                    yi[p[i]] = Double.NaN;
                }
            }
        }

        // Handle null xi
        for (int i = 0; i < xi.length; i++) {
            if (xi[i] == QueryConstants.NULL_DOUBLE) {
                yi[i] = QueryConstants.NULL_DOUBLE;
            }
        }

        return yi;
    }

    private static double[] diff(double[] v) {
        double[] result = new double[v.length - 1];

        for (int i = 0; i < result.length; i++) {
            result[i] = v[i + 1] - v[i];
        }

        return result;
    }

    private static double[][] sort(double[] x, double[] y) {
        if (x.length != y.length) {
            throw new IllegalArgumentException(
                "X and Y are not the same length: " + x.length + "," + y.length);
        }


        SortedMap<Double, Integer> map = new TreeMap<Double, Integer>();

        for (int i = 0; i < x.length; i++) {
            map.put(x[i], i);
        }

        if (map.size() != x.length) {
            throw new IllegalArgumentException("X values are repeated!");
        }

        double[][] result = new double[2][x.length];

        int index = 0;
        for (Map.Entry<Double, Integer> entry : map.entrySet()) {
            result[0][index] = entry.getKey();
            result[1][index] = y[entry.getValue()];
            index++;
        }

        return result;
    }

    private static boolean containsNegative(double[] x) {
        for (double xi : x) {
            if (xi < 0) {
                return true;
            }
        }

        return false;
    }

    private static double[] piecewiseCubicHermiteInterpolation(double[] x, double[] y, double[] xi,
        InterpolationDerivativeCalculator derivativeCalculator) {
        if (x.length != y.length) {
            throw new IllegalArgumentException(
                "X and Y are different lengths: " + x.length + "," + y.length);
        }

        if (x.length == 0) {
            throw new IllegalArgumentException("Inputs data is of zero length.");
        } else if (x.length == 1) {
            double[] result = new double[xi.length];

            for (int i = 0; i < result.length; i++) {
                result[i] = y[0];
            }

            return result;
        } else if (x.length == 2) {
            double m = (y[0] - y[1]) / (x[0] - x[1]);

            double[] result = new double[xi.length];

            for (int i = -0; i < result.length; i++) {
                result[i] = m * (xi[i] - x[0]) + y[0];
            }

            return result;
        }

        double[] h = diff(x);
        double[] delta = diff(y);

        for (int i = 0; i < delta.length; i++) {
            delta[i] = delta[i] / h[i];
        }

        double[] d = derivativeCalculator.computeDerivatives(x, y, h, delta);

        if (d.length != x.length) {
            throw new IllegalStateException("Hermite derivative vector is the wrong length: "
                + d.length + " != " + x.length + " " + derivativeCalculator.getClass());
        }

        int[] k = getBin(xi, x);
        double[] yi = new double[xi.length];

        for (int i = 0; i < xi.length; i++) {
            double yk = y[k[i]];
            double dk = d[k[i]];
            double ck = (3 * delta[k[i]] - 2 * d[k[i]] - d[k[i] + 1]) / h[k[i]];
            double bk = (d[k[i]] - 2 * delta[k[i]] + d[k[i] + 1]) / h[k[i]] / h[k[i]];
            double s = xi[i] - x[k[i]];
            yi[i] = yk + s * dk + s * s * ck + s * s * s * bk;
        }

        return yi;
    }

    /**
     * Interface for computing the derivatives at the nodes of the interpolation.
     */
    private interface InterpolationDerivativeCalculator {
        double[] computeDerivatives(double[] x, double[] y, double[] h, double[] delta);
    }

    private static class PchipDerivativeCalculator implements InterpolationDerivativeCalculator {

        public double[] computeDerivatives(double[] x, double[] y, double[] h, double[] delta) {
            double[] d = new double[x.length];

            for (int i = 1; i < x.length - 1; i++) {
                if (delta[i] * delta[i - 1] <= 0) {
                    d[i] = 0;
                } else {
                    double w1 = 2 * h[i] + h[i - 1];
                    double w2 = h[i] + 2 * h[i - 1];

                    double rhs = w1 / delta[i - 1] + w2 / delta[i];
                    double sum = w1 + w1;
                    d[i] = sum / rhs;
                }
            }

            d[0] = computeDeriviativeBoundaryValue(h[0], h[1], delta[0], delta[1]);
            d[d.length - 1] = computeDeriviativeBoundaryValue(h[h.length - 1], h[h.length - 2],
                delta[delta.length - 1], delta[delta.length - 2]);

            return d;
        }

        private double computeDeriviativeBoundaryValue(double h1, double h2, double delta1,
            double delta2) {
            double d = ((2 * h1 + h2) * delta1 - h1 * delta2) / (h1 + h2);

            if (d * delta1 < 0) {
                d = 0;
            } else if (delta1 * delta2 < 0 && Math.abs(d) > Math.abs(3 * delta1)) {
                d = 3 * delta1;
            }

            return d;
        }
    }

    private static class SplineDerivativeCalculator implements InterpolationDerivativeCalculator {

        public double[] computeDerivatives(double[] x, double[] y, double[] h, double[] delta) {

            double[][] a = new double[delta.length + 1][delta.length + 1];

            a[0][0] = h[1];
            a[0][1] = h[1] + h[0];

            a[delta.length][delta.length] = h[delta.length - 1];
            a[delta.length][delta.length - 1] = h[delta.length - 2] + h[delta.length - 1];

            for (int i = 1; i < delta.length; i++) {
                a[i][i - 1] = h[1];
                a[i][i] = 2 * (h[i - 1] + h[i]);
                a[i][i + 1] = h[i - 1];
            }

            double[][] r = new double[delta.length + 1][1];

            r[0][0] = ((h[0] + 2 * (h[0] + h[1])) * h[1] * delta[0] + h[0] * h[0] * delta[1])
                / (h[0] + h[1]);
            r[delta.length][0] = (h[h.length - 1] * h[h.length - 1] * delta[h.length - 2]
                + (2 * (h[h.length - 2] + h[h.length - 1]) + h[h.length - 1]) * h[h.length - 2]
                    * delta[h.length - 1])
                / (h[h.length - 2] + h[h.length - 1]);

            for (int i = 1; i < delta.length; i++) {
                r[i][0] = 3 * (h[i] * delta[i - 1] + h[i - 1] * delta[i]);
            }

            Matrix A = new Matrix(a);
            Matrix R = new Matrix(r);
            Matrix D = A.solve(R);

            double[] d = new double[D.getRowDimension()];

            for (int i = 0; i < d.length; i++) {
                d[i] = D.get(i, 0);
            }

            return d;
        }
    }

    /**
     * Returns the bin, determined by the nodes of x, that each element of xi belongs in. If the
     * sample is less than x[0] or greater than x[end], the sample is assigned to the first or last
     * bin. x is assumed to be sorted.
     *
     * @param xi elements being binned.
     * @param x nodes partitioning the space.
     * @return the bin, determined by the nodes of x, that each element of xi belongs in. If the
     *         sample is less than x[0] or greater than x[end], the sample is assigned to the first
     *         or last bin.
     */
    private static int[] getBin(double[] xi, double[] x) {
        int[] bin = new int[xi.length];

        for (int i = 0; i < xi.length; i++) {
            for (int k = 0; k < x.length - 1; k++) {
                if (x[k] <= xi[i] && xi[i] < x[k + 1]) {
                    bin[i] = k;
                }
            }

            if (x[x.length - 1] <= xi[i]) {
                bin[i] = x.length - 2;
            }

            if (x[0] > xi[i]) {
                bin[i] = 0;
            }
        }

        return bin;
    }

    /**
     * Returns true if any element is infinite or NaN or null.
     * 
     * @param v value
     * @return true if any element is infinite or NaN or null
     */
    private static boolean containsInfinityOrNanOrNull(final double[] v) {
        for (int i = 0; i < v.length; i++) {
            final double w = v[i];
            if (Double.isInfinite(w) || Double.isNaN(w) || isNull(w)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNull(double w) {
        return w == -Double.MAX_VALUE;
    }
}


