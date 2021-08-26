package io.deephaven.numerics.movingaverages;

import junit.framework.TestCase;

/**
 * Test EmaArray.
 */
public class EmaArrayTest extends TestCase {

    public void testLevelTick() {
        double dt = 1;
        double[] timeScales = {3, 4};
        double tol = 1e-10;

        double[] alphas = {Math.exp(-dt / timeScales[0]), Math.exp(-dt / timeScales[1])};

        EmaArray emas = new EmaArray(Ema.Type.LEVEL, Ema.Mode.TICK, timeScales);

        double x1 = 5;
        emas.processDouble(1, x1);
        assertEquals(1, emas.getLastTimestamp());
        assertEquals(2, emas.getCurrent().length);
        assertEquals(x1, emas.getCurrent()[0], tol);
        assertEquals(x1, emas.getCurrent()[1], tol);

        double x2 = 7;
        emas.processDouble(2, x2);
        assertEquals(2, emas.getLastTimestamp());
        assertEquals(2, emas.getCurrent().length);
        assertEquals(x1 * alphas[0] + x2 * (1 - alphas[0]), emas.getCurrent()[0], tol);
        assertEquals(x1 * alphas[1] + x2 * (1 - alphas[1]), emas.getCurrent()[1], tol);
    }

    public void testLevelTime() {
        double dt = 10;
        double[] timeScales = {3, 4};
        double tol = 1e-10;

        double[] alphas = {Math.exp(-dt / timeScales[0]), Math.exp(-dt / timeScales[1])};

        EmaArray emas = new EmaArray(Ema.Type.LEVEL, Ema.Mode.TIME, timeScales);

        double x1 = 5;
        emas.processDouble(10, x1);
        assertEquals(10, emas.getLastTimestamp());
        assertEquals(2, emas.getCurrent().length);
        assertEquals(x1, emas.getCurrent()[0], tol);
        assertEquals(x1, emas.getCurrent()[1], tol);

        double x2 = 7;
        emas.processDouble(20, x2);
        assertEquals(20, emas.getLastTimestamp());
        assertEquals(2, emas.getCurrent().length);
        assertEquals(x1 * alphas[0] + x2 * (1 - alphas[0]), emas.getCurrent()[0], tol);
        assertEquals(x1 * alphas[1] + x2 * (1 - alphas[1]), emas.getCurrent()[1], tol);
    }

    public void testDifferenceTick() {
        double dt = 1;
        double[] timeScales = {3, 4};
        double tol = 1e-10;

        double[] alphas = {Math.exp(-dt / timeScales[0]), Math.exp(-dt / timeScales[1])};

        EmaArray emas = new EmaArray(Ema.Type.DIFFERENCE, Ema.Mode.TICK, timeScales);

        double x1 = 5;
        emas.processDouble(1, x1);
        assertEquals(1, emas.getLastTimestamp());
        assertEquals(2, emas.getCurrent().length);
        assertEquals(x1 * (1 - alphas[0]), emas.getCurrent()[0], tol);
        assertEquals(x1 * (1 - alphas[1]), emas.getCurrent()[1], tol);

        double x2 = 7;
        emas.processDouble(2, x2);
        assertEquals(2, emas.getLastTimestamp());
        assertEquals(2, emas.getCurrent().length);
        assertEquals(x1 * alphas[0] * (1 - alphas[0]) + x2 * (1 - alphas[0]), emas.getCurrent()[0], tol);
        assertEquals(x1 * alphas[1] * (1 - alphas[1]) + x2 * (1 - alphas[1]), emas.getCurrent()[1], tol);
    }

    /**
     * Make sure that smaller timescales correspond to faster moving averages. The smallest double should correspond to
     * no averaging.
     */
    public void testTimescales() {
        double[] timeScales = {Double.MIN_VALUE};
        double tol = 1e-10;

        EmaArray emas = new EmaArray(Ema.Type.LEVEL, Ema.Mode.TIME, timeScales);

        double x1 = 5;
        emas.processDouble(1, x1);
        assertEquals(1, emas.getLastTimestamp());
        assertEquals(1, emas.getCurrent().length);
        assertEquals(x1, emas.getCurrent()[0], tol);

        double x2 = 7;
        emas.processDouble(2, x2);
        assertEquals(2, emas.getLastTimestamp());
        assertEquals(1, emas.getCurrent().length);
        assertEquals(x2, emas.getCurrent()[0], tol);
    }

    /**
     * Make sure that it returns the right size.
     */
    public void testSize() {
        double[] timeScales = {1, 2, 3, 4, 5};

        EmaArray emas = new EmaArray(Ema.Type.LEVEL, Ema.Mode.TIME, timeScales);
        assertEquals(timeScales.length, emas.size());
    }

}
