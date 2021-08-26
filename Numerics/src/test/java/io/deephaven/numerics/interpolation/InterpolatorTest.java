package io.deephaven.numerics.interpolation;

import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Test Interpolator.
 */
public class InterpolatorTest extends TestCase {

    public void testNearest() {
        double tol = 5e-2;
        final double extrapolateDistance = 0.01;
        final int nSteps = 95;
        final int nStepsInterp = 100;
        final Interpolator.InterpolationAlgorithm method =
            Interpolator.InterpolationAlgorithm.SPLINE;

        t1(tol, nSteps, nStepsInterp, extrapolateDistance, method);
    }

    public void testLinear() {
        double tol = 1e-2;
        final double extrapolateDistance = 0.1;
        final int nSteps = 40;
        final int nStepsInterp = 100;
        final Interpolator.InterpolationAlgorithm method =
            Interpolator.InterpolationAlgorithm.LINEAR;

        t1(tol, nSteps, nStepsInterp, extrapolateDistance, method);
    }

    public void testPchip() {
        double tol = 1e-2;
        final double extrapolateDistance = 0.1;
        final int nSteps = 35;
        final int nStepsInterp = 100;
        final Interpolator.InterpolationAlgorithm method =
            Interpolator.InterpolationAlgorithm.PCHIP;

        t1(tol, nSteps, nStepsInterp, extrapolateDistance, method);
    }

    public void testSpline() {
        double tol = 1e-2;
        final double extrapolateDistance = 0.1;
        final int nSteps = 20;
        final int nStepsInterp = 100;
        final Interpolator.InterpolationAlgorithm method =
            Interpolator.InterpolationAlgorithm.SPLINE;

        t1(tol, nSteps, nStepsInterp, extrapolateDistance, method);
    }

    private void t1(double tol, int nSteps, int nStepsInterp, double extrapolateDistance,
        Interpolator.InterpolationAlgorithm method) {
        final boolean extrapolate = true;

        final double xMin = extrapolateDistance;
        final double xMax = 3.141 * 2 - extrapolateDistance;

        final double xMinInterp = 0;
        final double xMaxInterp = 3.14 * 2;

        final double dx = (xMax - xMin) / (nSteps - 1);
        double[] x = new double[nSteps];
        double[] y = new double[nSteps];
        List<Integer> indices = new ArrayList<Integer>();

        for (int i = 0; i < nSteps; i++) {
            x[i] = xMin + i * dx;
            y[i] = Math.sin(x[i]) + 0.5 * Math.sin(2 * x[i]);
            indices.add(i);
        }

        Collections.shuffle(indices);
        double[] xshuffle = new double[x.length];
        double[] yshuffle = new double[y.length];

        for (int i = 0; i < indices.size(); i++) {
            xshuffle[i] = x[indices.get(i)];
            yshuffle[i] = y[indices.get(i)];
        }

        final double dxInterp = (xMaxInterp - xMinInterp) / (nStepsInterp - 1);
        double[] xi = new double[nStepsInterp];
        double[] yActual = new double[nStepsInterp];

        for (int i = 0; i < nStepsInterp; i++) {
            xi[i] = xMaxInterp - i * dxInterp;
            yActual[i] = Math.sin(xi[i]) + 0.5 * Math.sin(2 * xi[i]);
        }

        double[] yi = Interpolator.interpolate(xshuffle, yshuffle, xi, method, extrapolate);

        for (int i = 0; i < xi.length; i++) {
            assertEquals("Element: " + i, yActual[i], yi[i], tol);
        }
    }

    public void testNonExtrapolation() {
        double[] x = new double[] {1, 2, 3, 4, 5};
        double[] y = new double[] {2, 3, 2, 1, 1};
        double[] xi = new double[] {0, 2, 4, 6};
        double[] yi =
            Interpolator.interpolate(x, y, xi, Interpolator.InterpolationAlgorithm.PCHIP, false);
        assertTrue(Double.isNaN(yi[0]));
        assertTrue(Double.isNaN(yi[3]));
    }

    public void testNulls() {
        double[] x = new double[] {1, 2, 3, 4, 5};
        double[] y = new double[] {2, 3, 2, 1, 1};
        double[] xi = new double[] {1, 2, QueryConstants.NULL_DOUBLE, 5};
        double[] yi =
            Interpolator.interpolate(x, y, xi, Interpolator.InterpolationAlgorithm.PCHIP, false);
        assertFalse(yi[0] == QueryConstants.NULL_DOUBLE);
        assertFalse(yi[1] == QueryConstants.NULL_DOUBLE);
        assertTrue(yi[2] == QueryConstants.NULL_DOUBLE);
        assertFalse(yi[3] == QueryConstants.NULL_DOUBLE);
    }

}
