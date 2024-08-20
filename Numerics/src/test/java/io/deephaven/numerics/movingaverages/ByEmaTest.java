//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.numerics.movingaverages;

import io.deephaven.base.testing.RecordingMockObject;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;

import java.time.Instant;

/**
 * Test ByEma.
 */
public class ByEmaTest extends TestCase {
    private final static long SECOND = 1000;
    private final static long MINUTE = 60 * SECOND;
    private final static long HOUR = 60 * MINUTE;
    public final static long DAY = 24 * HOUR;

    private static class MA extends AbstractMa {

        private final RecordingMockObject logger;
        private final ByEma.Key key;
        private int nSamples = 0;

        private MA(RecordingMockObject logger, ByEma.Key key) {
            this.logger = logger;
            this.key = key;
            logger.recordActivity("MA.Constructor(" + key + ")\n");
        }

        @Override
        protected void processDoubleLocal(long timestamp, double data) {
            nSamples++;
            logger.recordActivity(
                    "MA.processDoubleLocal(" + key + "," + timestamp + "," + data + ") = " + nSamples + "\n");
        }

        @Override
        public double getCurrent() {
            logger.recordActivity("getCurrent() = " + nSamples + "\n");
            return nSamples;
        }

        @Override
        public void setCurrent(double value) {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("not implemented");
        }
    }

    private static class BE extends ByEma {
        private final RecordingMockObject logger = new RecordingMockObject();

        protected BE(BadDataBehavior nullBehavior, BadDataBehavior nanBehavior) {
            super(nullBehavior, nanBehavior);
        }

        @Override
        protected AbstractMa createEma(Key key) {
            return new MA(logger, key);
        }
    }

    private static final double NAN = Double.NaN;
    private static final double NULL = QueryConstants.NULL_DOUBLE;

    public void testEverything() {

        ByEma.BadDataBehavior nullBehavior = ByEma.BadDataBehavior.BD_RESET;
        ByEma.BadDataBehavior nanBehavior = ByEma.BadDataBehavior.BD_RESET;

        BE emaActual = new BE(nullBehavior, nanBehavior);
        RecordingMockObject target = new RecordingMockObject();

        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts0 = DateTimeUtils.epochNanosToInstant(DAY * 1000000);
        ByEma.Key k0 = new ByEma.Key("A", "B");
        MA ma0 = new MA(target, k0);
        ma0.processDoubleLocal(DateTimeUtils.epochNanos(ts0), 1);
        ma0.getCurrent();
        emaActual.update(ts0, 1, "A", "B");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts1 = DateTimeUtils.epochNanosToInstant(2 * DAY * 1000000);
        ma0.processDoubleLocal(DateTimeUtils.epochNanos(ts1), 2);
        ma0.getCurrent();
        emaActual.update(ts1, 2, "A", "B");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts2 = DateTimeUtils.epochNanosToInstant(3 * DAY * 1000000);
        ByEma.Key k1 = new ByEma.Key("A", "C");
        MA ma1 = new MA(target, k1);
        ma1.processDoubleLocal(DateTimeUtils.epochNanos(ts2), 3);
        ma1.getCurrent();
        emaActual.update(ts2, 3, "A", "C");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts3 = DateTimeUtils.epochNanosToInstant(4 * DAY * 1000000);
        ma0.processDoubleLocal(DateTimeUtils.epochNanos(ts3), 4);
        ma0.getCurrent();
        emaActual.update(ts3, 4, "A", "B");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts4 = DateTimeUtils.epochNanosToInstant(5 * DAY * 1000000);
        MA ma2 = new MA(target, k0);
        emaActual.update(ts4, NULL, "A", "B");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts5 = DateTimeUtils.epochNanosToInstant(6 * DAY * 1000000);
        ma1.processDoubleLocal(DateTimeUtils.epochNanos(ts5), 6);
        ma1.getCurrent();
        emaActual.update(ts5, 6, "A", "C");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts6 = DateTimeUtils.epochNanosToInstant(5 * DAY * 1000000);
        ma2.processDoubleLocal(DateTimeUtils.epochNanos(ts6), 7);
        ma2.getCurrent();
        emaActual.update(ts6, 7, "A", "B");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts7 = DateTimeUtils.epochNanosToInstant(6 * DAY * 1000000);
        MA ma3 = new MA(target, k0);
        emaActual.update(ts7, NAN, "A", "B");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts8 = DateTimeUtils.epochNanosToInstant(7 * DAY * 1000000);
        ma3.processDoubleLocal(DateTimeUtils.epochNanos(ts8), 8);
        ma3.getCurrent();
        emaActual.update(ts8, 8, "A", "B");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        Instant ts9 = DateTimeUtils.epochNanosToInstant(8 * DAY * 1000000);
        ma1.processDoubleLocal(DateTimeUtils.epochNanos(ts9), 8);
        ma1.getCurrent();
        emaActual.update(ts9, 8, "A", "C");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());

        // test no time version
        Instant ts10 = DateTimeUtils.epochNanosToInstant(9 * DAY * 1000000);
        ma1.processDoubleLocal(Long.MIN_VALUE, 9);
        ma1.getCurrent();
        emaActual.update(9, "A", "C");
        assertEquals(target.getActivityRecordAndReset(), emaActual.logger.getActivityRecordAndReset());
    }
}
