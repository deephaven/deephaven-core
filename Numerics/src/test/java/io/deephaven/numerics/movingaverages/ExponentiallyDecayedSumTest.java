package io.deephaven.numerics.movingaverages;

import junit.framework.TestCase;

/**
 * Test ExponentiallyDecayedSum.
 */
public class ExponentiallyDecayedSumTest extends TestCase {

    public void testEverything() {
        double tol = 1e-10;
        double rate = 200;

        ExponentiallyDecayedSum eds = new ExponentiallyDecayedSum(rate, true);

        assertEquals(eds.getValue(), 0, tol);

        long time1 = 0;
        double value1 = 10;
        eds.processDouble(time1, value1);
        assertEquals(time1, eds.getLastTimestamp());
        assertEquals(value1, eds.getValue(), tol);

        long time2 = 100;
        double value2 = 15;
        eds.processDouble(time2, value2);
        assertEquals(time2, eds.getLastTimestamp());
        assertEquals(value2 + Math.exp(-(time2 - time1) / rate) * value1, eds.getValue(), tol);

        long time3 = 170;
        double value3 = 8;
        eds.processDouble(time3, value3);
        assertEquals(time3, eds.getLastTimestamp());
        assertEquals(value3 + Math.exp(-(time3 - time2) / rate) * value2 + Math.exp(-(time3 - time1) / rate) * value1,
                eds.getValue(), tol);

        eds.reset();

        assertEquals(eds.getValue(), 0, tol);

        eds.processDouble(time1, value1);
        assertEquals(time1, eds.getLastTimestamp());
        assertEquals(value1, eds.getValue(), tol);

        eds.processDouble(time2, value2);
        assertEquals(time2, eds.getLastTimestamp());
        assertEquals(value2 + Math.exp(-(time2 - time1) / rate) * value1, eds.getValue(), tol);
    }
}
