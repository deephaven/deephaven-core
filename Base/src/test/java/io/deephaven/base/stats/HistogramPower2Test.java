/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

import junit.framework.TestCase;

public class HistogramPower2Test extends TestCase {

    static long NOW = 123456789L;

    public void setUp() {
        Stats.clearAll();
    }

    public void testSample() throws Exception {
        Item testItem = Stats.makeItem("HistogramPower2Test", "testData", HistogramPower2.FACTORY, NOW);
        Value testNewHistoState = testItem.getValue();

        assertEquals(testNewHistoState.getTypeTag(), 'N');

        // accumulate a count of 1 in each ^2 bin
        // negative numbers and zero go in bin 0
        long x = 0;
        for (int ii = 0; ii < 64; ii++) {
            x = 0x1L << ii;
            testNewHistoState.sample(x);
        }
        testNewHistoState.sample(0);

        testNewHistoState.alwaysUpdated(true);

        // should have a count of 1 in bin[1]..bin[63]; bin[0]=2

        Stats.update(new ItemUpdateListener() {
            public void handleItemUpdated(Item item, long now, long appNow, int intervalIndex, long intervalMillis,
                    String intervalName) {
                // Value v = item.getValue();
                HistogramPower2 nh;
                nh = (HistogramPower2) item.getValue();
                History history = nh.getHistory();
                StringBuilder sb = new StringBuilder();
                sb.append("STAT")
                        .append(',').append(intervalName)
                        .append(',').append(now / 1000.)
                        .append(',').append(appNow / 1000.)
                        .append(',').append(nh.getTypeTag())
                        .append(',').append(item.getGroupName())
                        .append('.').append(item.getName())
                        .append(',').append(history.getN(intervalIndex, 1))
                        .append(',').append(history.getSum(intervalIndex, 1))
                        .append(',').append(history.getLast(intervalIndex, 1))
                        .append(',').append(history.getMin(intervalIndex, 1))
                        .append(',').append(history.getMax(intervalIndex, 1))
                        .append(',').append(history.getAvg(intervalIndex, 1))
                        .append(',').append(history.getSum2(intervalIndex, 1))
                        .append(',').append(history.getStdev(intervalIndex, 1))
                        .append(',').append(nh.getHistogramString());

                System.out.println(sb);
            }
        }, NOW + 1000, NOW + 1000, 0);

        ((HistogramPower2) testNewHistoState).clear();
    }
}
