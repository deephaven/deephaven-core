/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.stats;

import junit.framework.TestCase;

public class HistogramStateTest extends TestCase {

    static long NOW = 123456789L;

    public void testSample() throws Exception {
        Item testItem =
            Stats.makeItem("HistogramStateTest", "testData", HistogramState.FACTORY, NOW,
                new HistogramState.Spec("HistogramStateTest", "testDate", 0, 100, 10));
        Value testHistogram = testItem.getValue();

        for (long x = -10; x < 110; ++x) {
            testHistogram.sample(x);
        }

        testHistogram.alwaysUpdated(true);

        // This should print 10 invocations every time

        Stats.update(new ItemUpdateListener() {
            public void handleItemUpdated(Item item, long now, long appNow, int intervalIndex,
                long intervalMillis, String intervalName) {
                Value v = item.getValue();
                History history = v.getHistory();
                StringBuilder sb = new StringBuilder();
                sb.append("STAT")
                    .append(',').append(intervalName)
                    .append(',').append(now / 1000.)
                    .append(',').append(appNow / 1000.)
                    .append(',').append(v.getTypeTag())
                    .append(',').append(item.getGroupName())
                    .append('.').append(item.getName())
                    .append(',').append(history.getN(intervalIndex, 1))
                    .append(',').append(history.getSum(intervalIndex, 1))
                    .append(',').append(history.getLast(intervalIndex, 1))
                    .append(',').append(history.getMin(intervalIndex, 1))
                    .append(',').append(history.getMax(intervalIndex, 1))
                    .append(',').append(history.getAvg(intervalIndex, 1))
                    .append(',').append(history.getSum2(intervalIndex, 1))
                    .append(',').append(history.getStdev(intervalIndex, 1));
                System.out.println(sb);
            }
        }, NOW + 1000, NOW + 1000, 0);

        testHistogram.reset();
    }
}
