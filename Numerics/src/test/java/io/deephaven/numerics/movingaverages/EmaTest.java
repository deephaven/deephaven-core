package io.deephaven.numerics.movingaverages;

import junit.framework.TestCase;

import java.io.*;
import java.util.Random;

/**
 * Test Ema.
 */
public class EmaTest extends TestCase {
    private final static long SECOND = 1000;
    private final static long MINUTE = 60 * SECOND;
    private final static long HOUR = 60 * MINUTE;
    public final static long DAY = 24 * HOUR;

    public void testLevelTick() {
        double timeScale = 3;
        double tol = 1e-10;

        double alpha = Math.exp(-1 / timeScale);

        Ema emas = new Ema(Ema.Type.LEVEL, Ema.Mode.TICK, timeScale);

        double x1 = 5;
        emas.processDouble(1, x1);
        assertEquals(1, emas.getLastTimestamp());
        assertEquals(x1, emas.getCurrent(), tol);

        double x2 = 7;
        emas.processDouble(2, x2);
        assertEquals(2, emas.getLastTimestamp());
        assertEquals(x1 * alpha + x2 * (1 - alpha), emas.getCurrent(), tol);
    }

    public void testLevelTime() {
        double timeScale = 3;
        double tol = 1e-10;

        double alpha = Math.exp(-10 / timeScale);

        Ema emas = new Ema(Ema.Type.LEVEL, Ema.Mode.TIME, timeScale);

        double x1 = 5;
        emas.processDouble(10, x1);
        assertEquals(10, emas.getLastTimestamp());
        assertEquals(x1, emas.getCurrent(), tol);

        double x2 = 7;
        emas.processDouble(20, x2);
        assertEquals(20, emas.getLastTimestamp());
        assertEquals(x1 * alpha + x2 * (1 - alpha), emas.getCurrent(), tol);
    }

    public void testDifferenceTick() {
        double timeScale = 3;
        double tol = 1e-10;

        double alpha = Math.exp(-1 / timeScale);

        Ema emas = new Ema(Ema.Type.DIFFERENCE, Ema.Mode.TICK, timeScale);

        double x1 = 5;
        emas.processDouble(1, x1);
        assertEquals(1, emas.getLastTimestamp());
        assertEquals(x1 * (1 - alpha), emas.getCurrent(), tol);

        double x2 = 7;
        emas.processDouble(2, x2);
        assertEquals(2, emas.getLastTimestamp());
        assertEquals(x1 * alpha * (1 - alpha) + x2 * (1 - alpha), emas.getCurrent(), tol);
    }

    /**
     * Make sure that smaller timescales correspond to faster moving averages. The smallest double
     * should correspond to no averaging.
     */
    public void testTimescales() {
        double timeScale = Double.MIN_VALUE;
        double tol = 1e-10;

        Ema emas = new Ema(Ema.Type.LEVEL, Ema.Mode.TICK, timeScale);

        double x1 = 5;
        emas.processDouble(1, x1);
        assertEquals(1, emas.getLastTimestamp());
        assertEquals(x1, emas.getCurrent(), tol);

        double x2 = 7;
        emas.processDouble(2, x2);
        assertEquals(2, emas.getLastTimestamp());
        assertEquals(x2, emas.getCurrent(), tol);
    }

    public void testLastMillis() {
        double timeScale = 3;

        Ema emas = new Ema(Ema.Type.LEVEL, Ema.Mode.TICK, timeScale);

        double x1 = 5;
        emas.processDouble(1, x1);
        assertEquals(1, emas.getLastTimestamp());

        double x2 = 7;
        emas.processDouble(2, x2);
        assertEquals(2, emas.getLastTimestamp());
    }


    public void testSerialize() throws IOException, ClassNotFoundException {
        double timeScale = DAY;
        Ema stat = new Ema(Ema.Type.LEVEL, Ema.Mode.TICK, timeScale);
        Random myRandom = new Random(892734L);
        for (long t = 0; t < DAY * 30; t += HOUR) {
            stat.processDouble(t, myRandom.nextDouble() + 100.0);
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(stat);
        oos.close();

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bis);
        Ema stat2 = (Ema) ois.readObject();

        assertTrue(stat2 != null);
    }
}
