/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import java.util.Random;

public class IndexPeformanceTest {

    private static final int SIZE = (Integer.MAX_VALUE - 1);

    public static void main(String[] args) {
        Index index = Index.FACTORY.getIndexByRange(0, SIZE);
        long sum = 0;
        long start = System.currentTimeMillis();
        for (Index.Iterator it = index.iterator(); it.hasNext();) {
            sum += it.nextLong();
        }
        System.out.println("Range iteration per item time = "
            + (System.currentTimeMillis() - start) / (SIZE / 1000000) + "ns " + sum);
        Random random = new Random(0);
        Index.RandomBuilder indexBuilder = Index.FACTORY.getRandomBuilder();
        long runningValue = 0;// Math.abs(random.nextLong());
        start = System.currentTimeMillis();
        long lastRangeStart = runningValue;
        for (int i = 0; runningValue < SIZE; i++) {
            int inc = random.nextInt(100);
            if (inc < 50) {
                if (lastRangeStart == -1) {
                    lastRangeStart = runningValue;
                }
                runningValue++;
            } else {
                if (lastRangeStart != -1) {
                    indexBuilder.addRange(lastRangeStart, runningValue);
                    lastRangeStart = -1;
                }
                runningValue += (inc - 5);
                indexBuilder.addKey(runningValue);
            }
        }
        System.out.println("Random construction per item time = "
            + (System.currentTimeMillis() - start) / (SIZE / 1000000) +
            "ns " + sum + " " + runningValue);
        index = indexBuilder.getIndex();
        sum = 0;
        start = System.currentTimeMillis();
        for (Index.Iterator it = index.iterator(); it.hasNext();) {
            sum += it.nextLong();
        }
        System.out.println("Random iteration per item time = "
            + (System.currentTimeMillis() - start) / (SIZE / 1000000) + "ns " + sum);
    }
}
